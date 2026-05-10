# pyspark-template

Template repository for a streaming word-count Spark job that runs on Oleander-managed Spark.

The job reads JSON messages from a Kafka topic, counts words per micro-batch, appends raw messages to Postgres and Iceberg, and upserts rolling aggregates into a Postgres state table.

The repository uses:

- `uv` for local Python dependency management
- two task-specific Spark entrypoints:
  - `entrypoint_messages_v2.py` for Postgres messages, Postgres state/analytics (`STATE_TABLE`), and raw Iceberg appends
  - `entrypoint_sentiment_windows_v2.py` for watermarked sentiment time ranges
- `app/` for Python modules that are packaged as `pyFiles`
- `tests/` for unit tests (run with `pytest`)
- Docker to build the deployment virtual environment artifact

## Manage dependencies with uv

Install the current project and dev dependencies locally:

```bash
uv sync --dev
```

Add a new runtime dependency:

```bash
uv add <package>
```

Add a new dev dependency:

```bash
uv add --dev <package>
```

After changing dependencies, commit both `pyproject.toml` and `uv.lock`.

## Run tests

```bash
uv run pytest tests/
```

## Build artifacts

Build both deployment artifacts:

```bash
make
```

Outputs:

- `out/pyfiles.zip`
- `out/environment.tar.gz`

Build artifacts individually:

```bash
make pyfiles
make environment
```

Rebuild from scratch:

```bash
make rebuild
```

## Deploy with oleander-cli

Configure the CLI first if needed:

```bash
oleander configure
```

Build the artifacts:

```bash
make
```

```bash
# upload the messages/state/analytics/Iceberg entrypoint
oleander spark jobs upload entrypoint_messages_v2.py \
  --py-files out/pyfiles.zip \
  --virtualenv out/environment.tar.gz
```

```bash
# upload the watermarked sentiment entrypoint
oleander spark jobs upload entrypoint_sentiment_windows_v2.py \
  --py-files out/pyfiles.zip \
  --virtualenv out/environment.tar.gz
```

```bash
# run
oleander spark jobs submit entrypoint_messages_v2.py \
  --namespace streaming \
  --name public_stream_messages \
  --mode STREAMING \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.postgresql:postgresql:42.7.4 \
  --wait
```

Adjust `--namespace`, `--name`, and any other submit options for your job.

Run the low-latency message writer and the watermarked sentiment windows as
separate tasks/jobs. The message task should stay isolated so Kafka-to-Postgres
latency is not affected by stateful window processing:

```bash
# Kafka -> Postgres messages, state analytics, and raw Iceberg append
oleander spark jobs submit entrypoint_messages_v2.py \
  --namespace streaming \
  --name public_stream_messages \
  --mode STREAMING \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.postgresql:postgresql:42.7.4
```

```bash
# watermarked sentiment time windows
oleander spark jobs submit entrypoint_sentiment_windows_v2.py \
  --namespace streaming \
  --name public_stream_sentiment_windows \
  --mode STREAMING \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.postgresql:postgresql:42.7.4
```

## Environment variables

The streaming entrypoints read configuration from oleander platform-level environment variables.
Set these in the oleander UI or API before submitting the job.

### Required

| Variable | Description |
| --- | --- |
| `PUBLIC_STREAM_KAFKA_TOPIC` | Kafka topic to subscribe to |
| `DATABASE_URL` | Postgres connection string (e.g. `postgresql://user:pass@host:5432/dbname`) |
| `KAFKA_BOOTSTRAP` | Kafka bootstrap servers (default `localhost:9092` — will not work on oleander) |

### Recommended

| Variable | Description |
| --- | --- |
| `PUBLIC_STREAM_CHECKPOINT_LOCATION` | Durable checkpoint path (e.g. `s3a://bucket/checkpoint`). Overrides `spark.oleander.app.state.dir`; otherwise defaults to `s3a://stream-time-window-579897423473-us-east-2-an/public-stream/checkpoints/messages-v4`. |
| `SENTIMENT_WINDOW_CHECKPOINT_LOCATION` | Durable checkpoint path for the sentiment window stream (e.g. `s3a://bucket/sentiment-window-checkpoint`). Overrides `spark.oleander.app.state.dir`; otherwise defaults to `s3a://stream-time-window-579897423473-us-east-2-an/public-stream/checkpoints/sentiment-v4`. |
| `ALLOW_LOCAL_STREAMING_CHECKPOINTS` | Allow `/tmp` checkpoint paths on non-local Spark masters. Defaults to `1`; set to `0` to require cluster-visible checkpoint storage. |
| `POSTGRES_TABLE` | Target Postgres table name (default `public_stream_messages`) |
| `STATE_TABLE` | Postgres table for rolling stream analytics upserts (default `public_stream_state`). Updated by `entrypoint_messages_v2.py` after each micro-batch. |
| `COMPUTE_POSTGRES_SENTIMENT` | Compute VADER sentiment in the low-latency Postgres message path. Defaults to `true`, matching the original fast stream behavior. Set to `false` only if the Python UDF becomes the bottleneck and neutral `0.5` scores are acceptable in the message table. |
| `MESSAGE_TRIGGER_INTERVAL` | Processing trigger interval for the low-latency Postgres message path. Defaults to `1 second`; set to an empty value to use Spark's default as-fast-as-possible trigger. |
| `KAFKA_MAX_OFFSETS_PER_TRIGGER` | Maximum Kafka offsets per micro-batch. Defaults to `1000` to prevent a huge backlog batch from blocking fresh message visibility for minutes. Set to an empty value to remove the cap. |
| `POSTGRES_WRITE_PARTITIONS` | Number of partitions used for each Postgres message write. Defaults to `1` to avoid launching many tiny JDBC write tasks for small micro-batches. Increase only if large batches need more write throughput. |
| `POSTGRES_JDBC_BATCH_SIZE` | JDBC insert batch size for Postgres message writes. Defaults to `5000`. |
| `POSTGRES_REWRITE_BATCHED_INSERTS` | Enables Postgres JDBC batched insert rewriting. Defaults to `true`. |
| `SENTIMENT_WINDOW_TABLE` | Target Postgres table for sentiment windows (default `public_stream_sentiment_windows`) |
| `ICEBERG_TABLE` | Fully-qualified Iceberg table for raw messages (default `oleander.default.public_stream_messages`) |
| `ICEBERG_COMPACTION_INTERVAL_BATCHES` | Run Iceberg `rewrite_data_files` every N completed micro-batches. Defaults to `5`. |
| `ICEBERG_COMPACTION_TARGET_FILE_SIZE_BYTES` | Target file size for Iceberg compaction. Defaults to `134217728` (128 MiB). |
| `WATERMARK_THRESHOLD_SECONDS` | Event-time watermark for sentiment windows and late-message reporting threshold. Defaults to `1`. Falls back to `WATERMARK_THRESHOLD_MINUTES * 60` if the seconds variable is unset. |
| `SENTIMENT_WINDOW_MINUTES` | Sentiment aggregation window size. Defaults to `1` so append-mode windows are visible quickly. |
| `SENTIMENT_WINDOW_TRIGGER_INTERVAL` | Processing trigger interval for the deferred sentiment window query. Defaults to `30 seconds` so the stateful window query does not constantly compete with the message path. |

If a stream fails with a missing checkpoint state file such as
`file:/tmp/.../state/.../*.delta does not exist`, restart it with fresh checkpoint
locations on shared storage. Do not reuse the broken `/tmp` checkpoint path:

```bash
PUBLIC_STREAM_CHECKPOINT_LOCATION=s3a://stream-time-window-579897423473-us-east-2-an/public-stream/checkpoints/messages-v4
SENTIMENT_WINDOW_CHECKPOINT_LOCATION=s3a://stream-time-window-579897423473-us-east-2-an/public-stream/checkpoints/sentiment-v4
```

On Oleander, the job derives checkpoint locations from
`spark.conf.get("spark.oleander.app.state.dir")` when explicit checkpoint env vars
are not set. For EMR Serverless, the sentiment window stream requires the resolved
checkpoint path to be on shared storage such as S3. The job role must be able to
list, read, write, and delete objects under the checkpoint prefixes.

Postgres message writes use a direct JDBC append for low latency. Keep one live
stream per checkpoint and avoid replaying already-inserted offsets into the same
`POSTGRES_TABLE`, because duplicate primary keys will abort the micro-batch.
For sub-3-second message visibility, run `entrypoint_messages_v2.py` by itself. That
task writes messages to Postgres first, appends the same raw rows to
`ICEBERG_TABLE`, then logs batch metrics and upserts aggregates into `STATE_TABLE`.
Run `entrypoint_sentiment_windows_v2.py` as a separate task/job for the watermarked
sentiment time ranges.
Sentiment windows use Spark append mode, so each window is written after the
window end plus the configured watermark delay.

### Conditional (Kafka authentication)

Only required when the Kafka cluster uses SASL authentication.

| Variable | Description |
| --- | --- |
| `KAFKA_SECURITY_PROTOCOL` | Security protocol (e.g. `SASL_SSL`) |
| `KAFKA_SASL_MECHANISM` | SASL mechanism (e.g. `PLAIN`) |
| `KAFKA_API_KEY` | SASL username |
| `KAFKA_API_SECRET` | SASL password |
