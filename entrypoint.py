from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass, replace
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg as spark_avg,
    col,
    count,
    current_timestamp,
    from_json,
    sum as spark_sum,
    to_timestamp,
    udf,
    when,
    window,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from app.word_count import STREAM_KEY, build_batch_word_deltas, compute_sentiment

_DEFAULT_PUBLIC_STREAM_CHECKPOINT_LOCATION = (
    "s3a://stream-time-window-579897423473-us-east-2-an/public-stream/checkpoints/messages-v1"
)
_DEFAULT_SENTIMENT_WINDOW_CHECKPOINT_LOCATION = (
    "s3a://stream-time-window-579897423473-us-east-2-an/public-stream/checkpoints/sentiment-v1"
)
_OLEANDER_APP_STATE_DIR_CONF = "spark.oleander.app.state.dir"
_PUBLIC_STREAM_CHECKPOINT_SUFFIX = "public-stream/checkpoints/messages-v1"
_SENTIMENT_WINDOW_CHECKPOINT_SUFFIX = "public-stream/checkpoints/sentiment-v1"


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"ERROR: {name} is required", file=sys.stderr)
        sys.exit(2)
    return value


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError:
        print(f"ERROR: {name} must be an integer", file=sys.stderr)
        sys.exit(2)


def _is_local_tmp_checkpoint(location: str) -> bool:
    loc = location.strip()
    parsed = urlparse(loc)
    if parsed.scheme == "file":
        return parsed.path.startswith("/tmp/")
    return loc == "/tmp" or loc.startswith("/tmp/")


def _fail_if_local_checkpoint_on_cluster(
    spark: SparkSession, location: str, env_var_name: str
) -> None:
    """Structured streaming checkpoints must be on shared storage when running distributed.

    Default ``/tmp/...`` paths are local to each container; stateful operators (e.g. window
    aggregation) write ``*.delta`` files that other executors must read. On EMR Serverless or
    any cluster, use ``s3://``, ``s3a://``, or ``hdfs://`` via env vars such as
    ``PUBLIC_STREAM_CHECKPOINT_LOCATION`` / ``SENTIMENT_WINDOW_CHECKPOINT_LOCATION``.
    """
    if os.getenv("ALLOW_LOCAL_STREAMING_CHECKPOINTS", "1") == "1":
        return
    master = spark.sparkContext.master
    if master.startswith("local"):
        return
    if _is_local_tmp_checkpoint(location):
        print(
            f"ERROR: {env_var_name}={location!r} points at local disk under /tmp. "
            "Executors cannot share this path; windowed streaming state will fail with "
            "missing *.delta files. Set a cluster-visible URI "
            f"(e.g. {env_var_name}=s3://your-bucket/prefix/checkpoints). "
            f"spark.master={master!r}. "
            "Local checkpoints are allowed by default; set "
            "ALLOW_LOCAL_STREAMING_CHECKPOINTS=0 to enforce shared checkpoint storage.",
            file=sys.stderr,
        )
        sys.exit(3)


def _require_shared_stateful_checkpoint(
    spark: SparkSession, location: str, env_var_name: str
) -> None:
    master = spark.sparkContext.master
    if master.startswith("local") or not _is_local_tmp_checkpoint(location):
        return
    print(
        f"ERROR: {env_var_name}={location!r} is local to each executor under "
        f"spark.master={master!r}. Sentiment windows use Spark's state store, so "
        "the checkpoint must be on shared storage such as "
        f"{env_var_name}=s3a://your-bucket/prefix/checkpoints.",
        file=sys.stderr,
    )
    sys.exit(3)


def _checkpoint_location(
    spark: SparkSession,
    env_var_name: str,
    default_location: str,
    suffix: str,
) -> str:
    env_location = os.getenv(env_var_name)
    if env_location:
        return env_location

    state_dir = spark.conf.get(_OLEANDER_APP_STATE_DIR_CONF, "").strip()
    if state_dir:
        return f"{state_dir.rstrip('/')}/{suffix}"

    return default_location


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
    iceberg_compaction_interval_batches: int
    iceberg_compaction_target_file_size_bytes: int
    watermark_threshold_minutes: int
    sentiment_window_minutes: int
    sentiment_window_table: str
    sentiment_window_checkpoint_location: str


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

_sentiment_udf = udf(compute_sentiment, DoubleType())

# Must match the Iceberg table schema (streaming-only columns like event_time /
# kafka_timestamp are kept on the batch DataFrame for metrics / JDBC only).
_ICEBERG_APPEND_COLUMNS = (
    "id",
    "body",
    "word_count",
    "created_at",
    "source",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
)

# Align with oleander `public_stream_messages` (no event_time / kafka_timestamp).
def _postgres_messages_df(enriched_df: DataFrame) -> DataFrame:
    return enriched_df.select(
        col("id"),
        to_timestamp(col("created_at")).alias("created_at"),
        current_timestamp().alias("updated_at"),
        col("body"),
        col("word_count"),
        col("source"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("latitude"),
        col("longitude"),
        col("city"),
        col("country"),
        col("sentiment_score"),
    )


_BATCH_METRICS_SQL = (
    "SELECT "
    "  COUNT(*) AS message_count, "
    "  COALESCE(SUM(word_count), 0) AS total_word_delta, "
    "  MAX(word_count) AS longest_message_word_count, "
    "  MAX_BY(id, kafka_offset) AS latest_message_id, "
    "  MAX(event_time) AS batch_max_event_time, "
    "  MIN(event_time) AS batch_min_event_time, "
    "  ROUND(AVG(UNIX_TIMESTAMP(kafka_timestamp) - UNIX_TIMESTAMP(event_time)), 2)"
    "    AS avg_producer_latency_seconds, "
    "  SUM(CASE WHEN event_time IS NULL THEN 1 ELSE 0 END) AS null_event_time_count "
    "FROM __batch_messages"
)


def _late_messages_sql(threshold_minutes: int) -> str:
    return (
        "SELECT COUNT(*) AS late_count FROM __batch_messages "
        "WHERE event_time < ("
        f"  SELECT MAX(event_time) - INTERVAL {threshold_minutes} MINUTES"
        "   FROM __batch_messages"
        ") AND event_time IS NOT NULL"
    )

_ICEBERG_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


def _split_iceberg_table_id(table: str) -> tuple[str, str]:
    """Return (catalog, namespace.table) for a three-part Iceberg table name."""
    parts = table.split(".")
    if len(parts) != 3 or not all(_ICEBERG_IDENTIFIER_RE.fullmatch(part) for part in parts):
        raise ValueError(
            "ICEBERG_TABLE must be a catalog.namespace.table identifier, "
            f"got {table!r}"
        )
    catalog, namespace, table_name = parts
    return catalog, f"{namespace}.{table_name}"


def _iceberg_rewrite_data_files_sql(
    table: str,
    target_file_size_bytes: int,
) -> str:
    catalog, table_without_catalog = _split_iceberg_table_id(table)
    return (
        f"CALL {catalog}.system.rewrite_data_files("
        f"table => '{table_without_catalog}', "
        "strategy => 'binpack', "
        "options => map("
        f"'target-file-size-bytes', '{target_file_size_bytes}'"
        ")"
        ")"
    )


def _should_compact_batch(batch_id: int, interval_batches: int) -> bool:
    return interval_batches > 0 and (batch_id + 1) % interval_batches == 0


def _compact_iceberg_table(spark: SparkSession, config: _Config, batch_id: int) -> None:
    if not _should_compact_batch(batch_id, config.iceberg_compaction_interval_batches):
        return

    sql = _iceberg_rewrite_data_files_sql(
        config.iceberg_table,
        config.iceberg_compaction_target_file_size_bytes,
    )
    try:
        result = spark.sql(sql).collect()
        print(
            json.dumps(
                {
                    "event": "iceberg_compaction_completed",
                    "table": config.iceberg_table,
                    "batch_id": batch_id,
                    "result": [row.asDict(recursive=True) for row in result],
                },
                default=str,
            )
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "event": "iceberg_compaction_failed",
                    "table": config.iceberg_table,
                    "batch_id": batch_id,
                    "error": str(exc),
                }
            ),
            file=sys.stderr,
        )


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


def _sentiment_windows_df(enriched_df: DataFrame, config: _Config) -> DataFrame:
    return (
        enriched_df
        .filter(col("event_time").isNotNull())
        .withColumn("is_positive", when(col("sentiment_score") > 0.5, 1).otherwise(0))
        .groupBy(window(col("event_time"), f"{config.sentiment_window_minutes} minutes"))
        .agg(
            spark_sum("is_positive").alias("positive_count"),
            (count("*") - spark_sum("is_positive")).alias("negative_count"),
            count("*").alias("total_count"),
            spark_avg("sentiment_score").alias("avg_sentiment"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("positive_count"),
            col("negative_count"),
            col("total_count"),
            col("avg_sentiment"),
        )
    )


def _build_sentiment_window_stream(parsed: DataFrame, config: _Config) -> DataFrame:
    return _sentiment_windows_df(
        parsed.withColumn("sentiment_score", _sentiment_udf(col("body"))),
        config,
    )


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

            late_count = batch_df.sparkSession.sql(
                _late_messages_sql(config.watermark_threshold_minutes)
            ).first()["late_count"]

            summary = {
                "stream_key": config.stream_key,
                "message_count": metrics["message_count"],
                "total_word_delta": metrics["total_word_delta"],
                "latest_message_id": metrics["latest_message_id"],
                "longest_message_word_count": metrics["longest_message_word_count"],
                "word_counts": word_deltas,
                "batch_id": batch_id,
                "batch_max_event_time": metrics["batch_max_event_time"],
                "batch_min_event_time": metrics["batch_min_event_time"],
                "avg_producer_latency_seconds": metrics["avg_producer_latency_seconds"],
                "null_event_time_count": metrics["null_event_time_count"],
                "late_message_count": late_count,
            }
            print(json.dumps(summary, default=str))

            enriched_df = batch_df.withColumn("sentiment_score", _sentiment_udf(col("body")))
            iceberg_df = enriched_df.select(*_ICEBERG_APPEND_COLUMNS)
            iceberg_df.writeTo(config.iceberg_table).append()
            _postgres_messages_df(enriched_df).write.jdbc(
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
            _compact_iceberg_table(batch_df.sparkSession, config, batch_id)
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
        checkpoint_location=_DEFAULT_PUBLIC_STREAM_CHECKPOINT_LOCATION,
        iceberg_compaction_interval_batches=_env_int(
            "ICEBERG_COMPACTION_INTERVAL_BATCHES",
            5,
        ),
        iceberg_compaction_target_file_size_bytes=_env_int(
            "ICEBERG_COMPACTION_TARGET_FILE_SIZE_BYTES",
            134_217_728,
        ),
        watermark_threshold_minutes=_env_int(
            "WATERMARK_THRESHOLD_MINUTES",
            1,
        ),
        sentiment_window_minutes=_env_int(
            "SENTIMENT_WINDOW_MINUTES",
            15,
        ),
        sentiment_window_table=os.getenv(
            "SENTIMENT_WINDOW_TABLE", "public_stream_sentiment_windows"
        ),
        sentiment_window_checkpoint_location=_DEFAULT_SENTIMENT_WINDOW_CHECKPOINT_LOCATION,
    )

    spark = (
        SparkSession.builder
        .appName("oleander-public-stream-word-count")
        .getOrCreate()
    )

    config = replace(
        config,
        checkpoint_location=_checkpoint_location(
            spark,
            "PUBLIC_STREAM_CHECKPOINT_LOCATION",
            _DEFAULT_PUBLIC_STREAM_CHECKPOINT_LOCATION,
            _PUBLIC_STREAM_CHECKPOINT_SUFFIX,
        ),
        sentiment_window_checkpoint_location=_checkpoint_location(
            spark,
            "SENTIMENT_WINDOW_CHECKPOINT_LOCATION",
            _DEFAULT_SENTIMENT_WINDOW_CHECKPOINT_LOCATION,
            _SENTIMENT_WINDOW_CHECKPOINT_SUFFIX,
        ),
    )

    _fail_if_local_checkpoint_on_cluster(
        spark, config.checkpoint_location, "PUBLIC_STREAM_CHECKPOINT_LOCATION"
    )
    _require_shared_stateful_checkpoint(
        spark,
        config.sentiment_window_checkpoint_location,
        "SENTIMENT_WINDOW_CHECKPOINT_LOCATION",
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
                "timestamp AS kafka_timestamp",
            )
            .select(
                from_json(col("json_value"), MESSAGE_SCHEMA).alias("msg"),
                col("kafka_topic"),
                col("kafka_partition"),
                col("kafka_offset"),
                col("kafka_timestamp"),
            )
            .select(
                col("msg.id").alias("id"),
                col("msg.body").alias("body"),
                col("msg.word_count").alias("word_count"),
                col("msg.created_at").alias("created_at"),
                to_timestamp(col("msg.created_at")).alias("event_time"),
                col("msg.source").alias("source"),
                col("kafka_topic"),
                col("kafka_partition"),
                col("kafka_offset"),
                col("kafka_timestamp"),
                col("msg.latitude").alias("latitude"),
                col("msg.longitude").alias("longitude"),
                col("msg.city").alias("city"),
                col("msg.country").alias("country"),
            )
            .withWatermark("event_time", f"{config.watermark_threshold_minutes} minutes")
        )

        message_query = (
            parsed.writeStream
            .foreachBatch(_make_batch_handler(config))
            .option("checkpointLocation", config.checkpoint_location)
            .start()
        )

        sentiment_query = (
            _build_sentiment_window_stream(parsed, config)
            .writeStream
            .outputMode("append")
            .foreachBatch(
                lambda df, _: df.write.jdbc(
                    config.jdbc_url,
                    table=config.sentiment_window_table,
                    mode="append",
                    properties=config.jdbc_props,
                )
            )
            .option("checkpointLocation", config.sentiment_window_checkpoint_location)
            .start()
        )

        spark.streams.awaitAnyTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
