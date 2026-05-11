from __future__ import annotations

import os
import sys
from dataclasses import dataclass, replace
from typing import TypeVar
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from app.word_count import STREAM_KEY

OLEANDER_APP_STATE_DIR_CONF = "spark.oleander.app.state.dir"

DEFAULT_MESSAGES_CHECKPOINT_LOCATION = (
    "s3a://stream-time-window-579897423473-us-east-2-an/public-stream/checkpoints/messages-v4"
)
DEFAULT_SENTIMENT_WINDOW_CHECKPOINT_LOCATION = (
    "s3a://stream-time-window-579897423473-us-east-2-an/public-stream/checkpoints/sentiment-v5"
)

MESSAGES_CHECKPOINT_SUFFIX = "public-stream/checkpoints/messages-v4"
SENTIMENT_WINDOW_CHECKPOINT_SUFFIX = "public-stream/checkpoints/sentiment-v5"

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


@dataclass(frozen=True)
class StreamConfig:
    stream_key: str
    jdbc_url: str
    jdbc_props: dict[str, str]
    checkpoint_location: str


ConfigT = TypeVar("ConfigT", bound=StreamConfig)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"ERROR: {name} is required", file=sys.stderr)
        sys.exit(2)
    return value


def env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError:
        print(f"ERROR: {name} must be an integer", file=sys.stderr)
        sys.exit(2)


def env_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}


def watermark_threshold_seconds() -> int:
    if os.getenv("WATERMARK_THRESHOLD_SECONDS") is not None:
        return env_int("WATERMARK_THRESHOLD_SECONDS", 1)
    if os.getenv("WATERMARK_THRESHOLD_MINUTES") is not None:
        return env_int("WATERMARK_THRESHOLD_MINUTES", 1) * 60
    return 1


def is_local_tmp_checkpoint(location: str) -> bool:
    loc = location.strip()
    parsed = urlparse(loc)
    if parsed.scheme == "file":
        return parsed.path.startswith("/tmp/")
    return loc == "/tmp" or loc.startswith("/tmp/")


def fail_if_local_checkpoint_on_cluster(
    spark: SparkSession, location: str, env_var_name: str
) -> None:
    """Guard against non-shared Structured Streaming checkpoints on clusters."""
    if os.getenv("ALLOW_LOCAL_STREAMING_CHECKPOINTS", "1") == "1":
        return
    master = spark.sparkContext.master
    if master.startswith("local") or not is_local_tmp_checkpoint(location):
        return
    print(
        f"ERROR: {env_var_name}={location!r} points at local disk under /tmp. "
        "Executors cannot share this path; set a cluster-visible URI "
        f"(e.g. {env_var_name}=s3://your-bucket/prefix/checkpoints). "
        f"spark.master={master!r}.",
        file=sys.stderr,
    )
    sys.exit(3)


def require_shared_stateful_checkpoint(
    spark: SparkSession, location: str, env_var_name: str
) -> None:
    master = spark.sparkContext.master
    if master.startswith("local") or not is_local_tmp_checkpoint(location):
        return
    print(
        f"ERROR: {env_var_name}={location!r} is local to each executor under "
        f"spark.master={master!r}. Sentiment windows use Spark's state store, so "
        "the checkpoint must be on shared storage such as "
        f"{env_var_name}=s3a://your-bucket/prefix/checkpoints.",
        file=sys.stderr,
    )
    sys.exit(3)


def checkpoint_location(
    spark: SparkSession,
    env_var_name: str,
    default_location: str,
    suffix: str,
) -> str:
    env_location = os.getenv(env_var_name)
    if env_location:
        return env_location

    state_dir = spark.conf.get(OLEANDER_APP_STATE_DIR_CONF, "").strip()
    if state_dir:
        return f"{state_dir.rstrip('/')}/{suffix}"

    return default_location


def parse_jdbc_url(database_url: str) -> tuple[str, dict[str, str]]:
    parsed = urlparse(database_url)
    jdbc_url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port or 5432}{parsed.path}"
    props = {
        "user": parsed.username or "",
        "password": parsed.password or "",
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified",
        "reWriteBatchedInserts": os.getenv("POSTGRES_REWRITE_BATCHED_INSERTS", "true"),
        "batchsize": os.getenv("POSTGRES_JDBC_BATCH_SIZE", "5000"),
    }
    return jdbc_url, props


def base_config(default_checkpoint_location: str) -> StreamConfig:
    jdbc_url, jdbc_props = parse_jdbc_url(require_env("DATABASE_URL"))
    return StreamConfig(
        stream_key=STREAM_KEY,
        jdbc_url=jdbc_url,
        jdbc_props=jdbc_props,
        checkpoint_location=default_checkpoint_location,
    )


def with_checkpoint(
    config: ConfigT,
    spark: SparkSession,
    env_var_name: str,
    default_location: str,
    suffix: str,
) -> ConfigT:
    return replace(
        config,
        checkpoint_location=checkpoint_location(
            spark,
            env_var_name,
            default_location,
            suffix,
        ),
    )


def kafka_options() -> dict[str, str]:
    opts: dict[str, str] = {
        "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        "subscribe": require_env("PUBLIC_STREAM_KAFKA_TOPIC"),
        "startingOffsets": "latest",
    }
    max_offsets_per_trigger = os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "1000")
    if max_offsets_per_trigger:
        opts["maxOffsetsPerTrigger"] = max_offsets_per_trigger

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


def parsed_messages_stream(spark: SparkSession) -> DataFrame:
    raw_stream = spark.readStream.format("kafka").options(**kafka_options()).load()
    return (
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
    )
