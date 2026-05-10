from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    to_timestamp,
    udf,
)
from pyspark.sql.types import DoubleType

from app.public_stream import (
    DEFAULT_MESSAGES_CHECKPOINT_LOCATION,
    MESSAGES_CHECKPOINT_SUFFIX,
    StreamConfig,
    base_config,
    env_bool,
    env_int,
    fail_if_local_checkpoint_on_cluster,
    parsed_messages_stream,
    watermark_threshold_seconds,
    with_checkpoint,
)
from app.word_count import build_batch_word_deltas, compute_sentiment

_ICEBERG_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_SENTIMENT_UDF = udf(compute_sentiment, DoubleType())

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


@dataclass(frozen=True)
class MessagesConfig(StreamConfig):
    postgres_table: str
    state_table: str
    iceberg_table: str
    postgres_write_partitions: int
    iceberg_compaction_interval_batches: int
    iceberg_compaction_target_file_size_bytes: int
    watermark_threshold_seconds: int
    trigger_interval: str
    compute_postgres_sentiment: bool


def _messages_config() -> MessagesConfig:
    base = base_config(DEFAULT_MESSAGES_CHECKPOINT_LOCATION)
    return MessagesConfig(
        **base.__dict__,
        postgres_table=os.getenv("POSTGRES_TABLE", "public_stream_messages"),
        state_table=os.getenv("STATE_TABLE", "public_stream_state"),
        iceberg_table=os.getenv(
            "ICEBERG_TABLE", "oleander.default.public_stream_messages"
        ),
        postgres_write_partitions=max(1, env_int("POSTGRES_WRITE_PARTITIONS", 1)),
        iceberg_compaction_interval_batches=env_int(
            "ICEBERG_COMPACTION_INTERVAL_BATCHES",
            5,
        ),
        iceberg_compaction_target_file_size_bytes=env_int(
            "ICEBERG_COMPACTION_TARGET_FILE_SIZE_BYTES",
            134_217_728,
        ),
        watermark_threshold_seconds=watermark_threshold_seconds(),
        trigger_interval=os.getenv("MESSAGE_TRIGGER_INTERVAL", "1 second"),
        compute_postgres_sentiment=env_bool("COMPUTE_POSTGRES_SENTIMENT", True),
    )


def _with_messages_checkpoint(
    config: MessagesConfig, spark: SparkSession
) -> MessagesConfig:
    return with_checkpoint(
        config,
        spark,
        "PUBLIC_STREAM_CHECKPOINT_LOCATION",
        DEFAULT_MESSAGES_CHECKPOINT_LOCATION,
        MESSAGES_CHECKPOINT_SUFFIX,
    )


def _postgres_messages_df(
    messages_df: DataFrame, compute_sentiment_score: bool
) -> DataFrame:
    sentiment_score = (
        _SENTIMENT_UDF(col("body"))
        if compute_sentiment_score
        else lit(0.5).cast(DoubleType())
    )
    return messages_df.select(
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
        sentiment_score.alias("sentiment_score"),
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


def _late_messages_sql(threshold_seconds: int) -> str:
    return (
        "SELECT COUNT(*) AS late_count FROM __batch_messages "
        "WHERE event_time < ("
        f"  SELECT MAX(event_time) - INTERVAL {threshold_seconds} SECONDS"
        "   FROM __batch_messages"
        ") AND event_time IS NOT NULL"
    )


def _split_iceberg_table_id(table: str) -> tuple[str, str]:
    parts = table.split(".")
    if len(parts) != 3 or not all(
        _ICEBERG_IDENTIFIER_RE.fullmatch(part) for part in parts
    ):
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


def _compact_iceberg_table(
    spark: SparkSession, config: MessagesConfig, batch_id: int
) -> None:
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
    java_props = jvm.java.util.Properties()
    for key, value in props.items():
        java_props.setProperty(key, value)

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
            previous_words_json = rs.getString(4)
            prev_words = json.loads(previous_words_json) if previous_words_json else {}
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

        merged_words = dict(prev_words)
        for word, count in word_deltas.items():
            merged_words[word] = merged_words.get(word, 0) + count

        sorted_words = sorted(
            merged_words.items(), key=lambda item: item[1], reverse=True
        )
        top_word, top_count = sorted_words[0] if sorted_words else ("", 0)
        popular = dict(sorted_words[:100])

        ps = conn.prepareStatement(
            _UPSERT_STATE_SQL.replace("public_stream_state", state_table)
        )
        ps.setString(1, stream_key)
        ps.setLong(2, int(new_total))
        ps.setLong(3, int(new_count))
        ps.setString(4, str(metrics["latest_message_id"]))
        ps.setDouble(5, float(new_avg))
        ps.setLong(6, len(merged_words))
        ps.setInt(7, int(new_longest))
        ps.setString(8, top_word)
        ps.setLong(9, int(top_count))
        ps.setString(10, json.dumps(popular))
        ps.executeUpdate()
        ps.close()
    finally:
        conn.close()


def _run_analytics_for_batch(
    batch_df: DataFrame, config: MessagesConfig, batch_id: int
) -> None:
    batch_df.cache()
    try:
        batch_df.createOrReplaceTempView("__batch_messages")
        word_deltas = build_batch_word_deltas(batch_df)

        metrics = batch_df.sparkSession.sql(_BATCH_METRICS_SQL).first()
        if metrics["message_count"] == 0:
            return

        late_count = batch_df.sparkSession.sql(
            _late_messages_sql(config.watermark_threshold_seconds)
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


def _process_messages_batch(config: MessagesConfig):
    def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
        postgres_df = _postgres_messages_df(batch_df, config.compute_postgres_sentiment)
        postgres_df.coalesce(config.postgres_write_partitions).write.jdbc(
            config.jdbc_url,
            table=config.postgres_table,
            mode="append",
            properties=config.jdbc_props,
        )
        batch_df.select(*_ICEBERG_APPEND_COLUMNS).writeTo(config.iceberg_table).append()
        _run_analytics_for_batch(batch_df, config, batch_id)

    return _process_batch


def main() -> None:
    config = _messages_config()
    spark = (
        SparkSession.builder
        .appName("oleander-public-stream-messages")
        .getOrCreate()
    )

    try:
        config = _with_messages_checkpoint(config, spark)
        fail_if_local_checkpoint_on_cluster(
            spark,
            config.checkpoint_location,
            "PUBLIC_STREAM_CHECKPOINT_LOCATION",
        )

        writer = (
            parsed_messages_stream(spark)
            .writeStream
            .queryName("public-stream-messages")
            .foreachBatch(_process_messages_batch(config))
            .option("checkpointLocation", config.checkpoint_location)
        )
        if config.trigger_interval:
            writer = writer.trigger(processingTime=config.trigger_interval)

        writer.start()
        spark.streams.awaitAnyTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
