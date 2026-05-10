from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, sum as spark_sum, udf, when, window
from pyspark.sql.types import DoubleType

from app.public_stream import (
    DEFAULT_SENTIMENT_WINDOW_CHECKPOINT_LOCATION,
    SENTIMENT_WINDOW_CHECKPOINT_SUFFIX,
    StreamConfig,
    base_config,
    env_int,
    parsed_messages_stream,
    require_shared_stateful_checkpoint,
    watermark_threshold_seconds,
    with_checkpoint,
)
from app.word_count import compute_sentiment

_SENTIMENT_UDF = udf(compute_sentiment, DoubleType())


@dataclass(frozen=True)
class SentimentWindowConfig(StreamConfig):
    table: str
    watermark_threshold_seconds: int
    window_minutes: int
    trigger_interval: str


def _sentiment_window_config() -> SentimentWindowConfig:
    base = base_config(DEFAULT_SENTIMENT_WINDOW_CHECKPOINT_LOCATION)
    return SentimentWindowConfig(
        **base.__dict__,
        table=os.getenv("SENTIMENT_WINDOW_TABLE", "public_stream_sentiment_windows"),
        watermark_threshold_seconds=watermark_threshold_seconds(),
        window_minutes=env_int("SENTIMENT_WINDOW_MINUTES", 1),
        trigger_interval=os.getenv("SENTIMENT_WINDOW_TRIGGER_INTERVAL", "30 seconds"),
    )


def _with_sentiment_checkpoint(
    config: SentimentWindowConfig, spark: SparkSession
) -> SentimentWindowConfig:
    return with_checkpoint(
        config,
        spark,
        "SENTIMENT_WINDOW_CHECKPOINT_LOCATION",
        DEFAULT_SENTIMENT_WINDOW_CHECKPOINT_LOCATION,
        SENTIMENT_WINDOW_CHECKPOINT_SUFFIX,
    )


def _sentiment_windows_df(parsed: DataFrame, config: SentimentWindowConfig) -> DataFrame:
    enriched = (
        parsed
        .filter(col("event_time").isNotNull())
        .withWatermark("event_time", f"{config.watermark_threshold_seconds} seconds")
        .withColumn("sentiment_score", _SENTIMENT_UDF(col("body")))
        .withColumn("is_positive", when(col("sentiment_score") > 0.5, 1).otherwise(0))
    )
    return (
        enriched
        .groupBy(window(col("event_time"), f"{config.window_minutes} minutes"))
        .agg(
            spark_sum("is_positive").alias("positive_count"),
            (count("*") - spark_sum("is_positive")).alias("negative_count"),
            count("*").alias("total_count"),
            avg("sentiment_score").alias("avg_sentiment"),
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


def _write_sentiment_batch(config: SentimentWindowConfig):
    def _process_batch(batch_df: DataFrame, _batch_id: int) -> None:
        batch_df.write.jdbc(
            config.jdbc_url,
            table=config.table,
            mode="append",
            properties=config.jdbc_props,
        )

    return _process_batch


def main() -> None:
    config = _sentiment_window_config()
    spark = (
        SparkSession.builder
        .appName("oleander-public-stream-sentiment-windows")
        .getOrCreate()
    )

    try:
        config = _with_sentiment_checkpoint(config, spark)
        require_shared_stateful_checkpoint(
            spark,
            config.checkpoint_location,
            "SENTIMENT_WINDOW_CHECKPOINT_LOCATION",
        )

        writer = (
            _sentiment_windows_df(parsed_messages_stream(spark), config)
            .writeStream
            .queryName("public-stream-sentiment-windows")
            .outputMode("append")
            .foreachBatch(_write_sentiment_batch(config))
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
