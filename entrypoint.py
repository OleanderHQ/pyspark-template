from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from app.word_count import STREAM_KEY, build_batch_word_deltas


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"ERROR: {name} is required", file=sys.stderr)
        sys.exit(2)
    return value


def _parse_jdbc_url(database_url: str) -> tuple[str, dict[str, str]]:
    """Convert a libpq-style DATABASE_URL to a JDBC URL and properties dict."""
    parsed = urlparse(database_url)
    jdbc_url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port or 5432}{parsed.path}"
    props = {
        "user": parsed.username or "",
        "password": parsed.password or "",
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified",
    }
    return jdbc_url, props


@dataclass(frozen=True)
class _Config:
    stream_key: str
    jdbc_url: str
    jdbc_props: dict[str, str]
    postgres_table: str
    state_table: str
    iceberg_table: str
    checkpoint_location: str


MESSAGE_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("body", StringType()),
    StructField("word_count", IntegerType()),
    StructField("created_at", StringType()),
    StructField("source", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
])

_BATCH_METRICS_SQL = (
    "SELECT "
    "  COUNT(*) AS message_count, "
    "  COALESCE(SUM(word_count), 0) AS total_word_delta, "
    "  MAX(word_count) AS longest_message_word_count, "
    "  MAX_BY(id, kafka_offset) AS latest_message_id "
    "FROM __batch_messages"
)


def _kafka_options() -> dict[str, str]:
    opts: dict[str, str] = {
        "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        "subscribe": _require_env("PUBLIC_STREAM_KAFKA_TOPIC"),
        "startingOffsets": "latest",
    }

    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
    if security_protocol:
        opts["kafka.security.protocol"] = security_protocol
        opts["kafka.sasl.mechanism"] = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
        api_key = os.getenv("KAFKA_API_KEY", "")
        api_secret = os.getenv("KAFKA_API_SECRET", "")
        opts["kafka.sasl.jaas.config"] = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{api_key}" password="{api_secret}";'
        )

    return opts


_SELECT_STATE_SQL = (
    "SELECT total_word_count, message_count, longest_message_word_count, popular_words "
    "FROM public_stream_state WHERE stream_key = ?"
)

_UPSERT_STATE_SQL = (
    "INSERT INTO public_stream_state ("
    "  stream_key, total_word_count, message_count, latest_message_id,"
    "  average_words_per_message, unique_word_count, longest_message_word_count,"
    "  most_frequent_word, most_frequent_word_count, popular_words, updated_at"
    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())"
    " ON CONFLICT (stream_key) DO UPDATE SET"
    "  total_word_count = EXCLUDED.total_word_count,"
    "  message_count = EXCLUDED.message_count,"
    "  latest_message_id = EXCLUDED.latest_message_id,"
    "  average_words_per_message = EXCLUDED.average_words_per_message,"
    "  unique_word_count = EXCLUDED.unique_word_count,"
    "  longest_message_word_count = EXCLUDED.longest_message_word_count,"
    "  most_frequent_word = EXCLUDED.most_frequent_word,"
    "  most_frequent_word_count = EXCLUDED.most_frequent_word_count,"
    "  popular_words = EXCLUDED.popular_words,"
    "  updated_at = EXCLUDED.updated_at"
)


def _update_stream_state(
    jvm,
    jdbc_url: str,
    props: dict[str, str],
    state_table: str,
    stream_key: str,
    metrics: dict,
    word_deltas: dict[str, int],
) -> None:
    """Read-merge-write the running stream aggregates into the state table."""
    java_props = jvm.java.util.Properties()
    for k, v in props.items():
        java_props.setProperty(k, v)

    conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, java_props)
    try:
        ps = conn.prepareStatement(
            _SELECT_STATE_SQL.replace("public_stream_state", state_table)
        )
        ps.setString(1, stream_key)
        rs = ps.executeQuery()

        if rs.next():
            prev_total = rs.getLong(1)
            prev_count = rs.getLong(2)
            prev_longest = rs.getInt(3)
            pw_json = rs.getString(4)
            prev_words = json.loads(pw_json) if pw_json else {}
            if isinstance(prev_words, list):
                prev_words = {}
        else:
            prev_total = prev_count = prev_longest = 0
            prev_words = {}
        rs.close()
        ps.close()

        new_total = prev_total + metrics["total_word_delta"]
        new_count = prev_count + metrics["message_count"]
        new_longest = max(prev_longest, metrics["longest_message_word_count"])
        new_avg = new_total / new_count if new_count > 0 else 0.0

        merged = dict(prev_words)
        for word, count in word_deltas.items():
            merged[word] = merged.get(word, 0) + count

        sorted_words = sorted(merged.items(), key=lambda x: x[1], reverse=True)
        top_word, top_count = sorted_words[0] if sorted_words else ("", 0)
        popular = {w: c for w, c in sorted_words[:100]}

        ps = conn.prepareStatement(
            _UPSERT_STATE_SQL.replace("public_stream_state", state_table)
        )
        ps.setString(1, stream_key)
        ps.setLong(2, int(new_total))
        ps.setLong(3, int(new_count))
        ps.setString(4, str(metrics["latest_message_id"]))
        ps.setDouble(5, float(new_avg))
        ps.setLong(6, len(merged))
        ps.setInt(7, int(new_longest))
        ps.setString(8, top_word)
        ps.setLong(9, int(top_count))
        ps.setString(10, json.dumps(popular))
        ps.executeUpdate()
        ps.close()
    finally:
        conn.close()


def _make_batch_handler(config: _Config):
    """Return a foreachBatch handler closed over resolved config."""

    def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        batch_df.cache()
        try:
            batch_df.createOrReplaceTempView("__batch_messages")

            word_deltas = build_batch_word_deltas(batch_df)

            metrics = batch_df.sparkSession.sql(_BATCH_METRICS_SQL).first()

            summary = {
                "stream_key": config.stream_key,
                "message_count": metrics["message_count"],
                "total_word_delta": metrics["total_word_delta"],
                "latest_message_id": metrics["latest_message_id"],
                "longest_message_word_count": metrics["longest_message_word_count"],
                "word_counts": word_deltas,
                "batch_id": batch_id,
            }
            print(json.dumps(summary, default=str))

            iceberg_df = batch_df.drop("latitude", "longitude", "city", "country")
            iceberg_df.writeTo(config.iceberg_table).append()
            batch_df.write.jdbc(
                config.jdbc_url,
                table=config.postgres_table,
                mode="append",
                properties=config.jdbc_props,
            )
            _update_stream_state(
                batch_df.sparkSession._jvm,
                config.jdbc_url,
                config.jdbc_props,
                config.state_table,
                config.stream_key,
                summary,
                word_deltas,
            )
        finally:
            batch_df.unpersist()

    return _process_batch


def main() -> None:
    database_url = _require_env("DATABASE_URL")
    jdbc_url, jdbc_props = _parse_jdbc_url(database_url)

    config = _Config(
        stream_key=STREAM_KEY,
        jdbc_url=jdbc_url,
        jdbc_props=jdbc_props,
        postgres_table=os.getenv("POSTGRES_TABLE", "public_stream_messages"),
        state_table=os.getenv("STATE_TABLE", "public_stream_state"),
        iceberg_table=os.getenv(
            "ICEBERG_TABLE", "oleander.default.public_stream_messages"
        ),
        checkpoint_location=os.getenv(
            "PUBLIC_STREAM_CHECKPOINT_LOCATION",
            "/tmp/oleander-public-stream-checkpoint",
        ),
    )

    spark = (
        SparkSession.builder
        .appName("oleander-public-stream-word-count")
        .getOrCreate()
    )

    try:
        raw_stream = (
            spark.readStream.format("kafka")
            .options(**_kafka_options())
            .load()
        )

        parsed = (
            raw_stream
            .selectExpr(
                "CAST(value AS STRING) AS json_value",
                "topic AS kafka_topic",
                "partition AS kafka_partition",
                "offset AS kafka_offset",
            )
            .select(
                from_json(col("json_value"), MESSAGE_SCHEMA).alias("msg"),
                col("kafka_topic"),
                col("kafka_partition"),
                col("kafka_offset"),
            )
            .select(
                col("msg.id").alias("id"),
                col("msg.body").alias("body"),
                col("msg.word_count").alias("word_count"),
                col("msg.created_at").alias("created_at"),
                col("msg.source").alias("source"),
                col("kafka_topic"),
                col("kafka_partition"),
                col("kafka_offset"),
                col("msg.latitude").alias("latitude"),
                col("msg.longitude").alias("longitude"),
                col("msg.city").alias("city"),
                col("msg.country").alias("country"),
            )
        )

        query = (
            parsed.writeStream
            .foreachBatch(_make_batch_handler(config))
            .option("checkpointLocation", config.checkpoint_location)
            .start()
        )

        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
