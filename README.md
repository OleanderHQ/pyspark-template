# pyspark-template

Template repository for a streaming word-count Spark job that runs on Oleander-managed Spark.

The job reads JSON messages from a Kafka topic, counts words per micro-batch, and appends raw messages to both a Postgres table and an Oleander Iceberg table.

The repository uses:

- `uv` for local Python dependency management
- `entrypoint.py` as the Spark job entrypoint
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
# upload
oleander spark jobs upload entrypoint.py \
  --py-files out/pyfiles.zip \
  --virtualenv out/environment.tar.gz
```

```bash
# run
oleander spark jobs submit entrypoint.py \
  --namespace streaming \
  --name public_stream_word_count \
  --mode STREAMING \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.postgresql:postgresql:42.7.4 \
  --wait
```

Adjust `--namespace`, `--name`, and any other submit options for your job.

## Environment variables

The streaming entrypoint reads configuration from oleander platform-level environment variables.
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
| `PUBLIC_STREAM_CHECKPOINT_LOCATION` | Durable checkpoint path (e.g. `s3a://bucket/checkpoint`). Overrides `spark.oleander.app.state.dir`; otherwise defaults to `s3a://stream-time-window-579897423473-us-east-2-an/public-stream/checkpoints/messages-v2`. |
| `ANALYTICS_CHECKPOINT_LOCATION` | Durable checkpoint path for deferred Iceberg/state/metrics processing. Overrides `spark.oleander.app.state.dir`; otherwise defaults to `s3a://stream-time-window-579897423473-us-east-2-an/public-stream/checkpoints/analytics-v2`. |
| `SENTIMENT_WINDOW_CHECKPOINT_LOCATION` | Durable checkpoint path for the sentiment window stream (e.g. `s3a://bucket/sentiment-window-checkpoint`). Overrides `spark.oleander.app.state.dir`; otherwise defaults to `s3a://stream-time-window-579897423473-us-east-2-an/public-stream/checkpoints/sentiment-v3`. |
| `ALLOW_LOCAL_STREAMING_CHECKPOINTS` | Allow `/tmp` checkpoint paths on non-local Spark masters. Defaults to `1`; set to `0` to require cluster-visible checkpoint storage. |
| `POSTGRES_TABLE` | Target Postgres table name (default `public_stream_messages`) |
| `COMPUTE_POSTGRES_SENTIMENT` | Compute VADER sentiment in the low-latency Postgres message path. Defaults to `true`, matching the original fast stream behavior. Set to `false` only if the Python UDF becomes the bottleneck and neutral `0.5` scores are acceptable in the message table. |
| `POSTGRES_JDBC_BATCH_SIZE` | JDBC insert batch size for Postgres message writes. Defaults to `5000`. |
| `POSTGRES_REWRITE_BATCHED_INSERTS` | Enables Postgres JDBC batched insert rewriting. Defaults to `true`. |
| `SENTIMENT_WINDOW_TABLE` | Target Postgres table for sentiment windows (default `public_stream_sentiment_windows`) |
| `ICEBERG_TABLE` | Fully-qualified Iceberg table for raw messages (default `oleander.default.public_stream_messages`) |
| `ICEBERG_COMPACTION_INTERVAL_BATCHES` | Run Iceberg `rewrite_data_files` every N completed micro-batches. Defaults to `5`. |
| `ICEBERG_COMPACTION_TARGET_FILE_SIZE_BYTES` | Target file size for Iceberg compaction. Defaults to `134217728` (128 MiB). |
| `WATERMARK_THRESHOLD_SECONDS` | Event-time watermark for sentiment windows and late-message reporting threshold. Defaults to `1`. Falls back to `WATERMARK_THRESHOLD_MINUTES * 60` if the seconds variable is unset. |
| `SENTIMENT_WINDOW_MINUTES` | Sentiment aggregation window size. Defaults to `1` so append-mode windows are visible quickly. |
| `ANALYTICS_TRIGGER_INTERVAL` | Processing trigger interval for deferred Iceberg/state/metrics work. Defaults to `60 seconds`. |
| `SENTIMENT_WINDOW_TRIGGER_INTERVAL` | Processing trigger interval for the deferred sentiment window query. Defaults to `5 seconds`. |

If a stream fails with a missing checkpoint state file such as
`file:/tmp/.../state/.../*.delta does not exist`, restart it with fresh checkpoint
locations on shared storage. Do not reuse the broken `/tmp` checkpoint path:

```bash
PUBLIC_STREAM_CHECKPOINT_LOCATION=s3a://stream-time-window-579897423473-us-east-2-an/public-stream/checkpoints/messages-v2
SENTIMENT_WINDOW_CHECKPOINT_LOCATION=s3a://stream-time-window-579897423473-us-east-2-an/public-stream/checkpoints/sentiment-v3
```

On Oleander, the job derives checkpoint locations from
`spark.conf.get("spark.oleander.app.state.dir")` when explicit checkpoint env vars
are not set. For EMR Serverless, the sentiment window stream requires the resolved
checkpoint path to be on shared storage such as S3. The job role must be able to
list, read, write, and delete objects under the checkpoint prefixes.

Postgres message writes use a direct JDBC append for low latency. Keep one live
stream per checkpoint and avoid replaying already-inserted offsets into the same
`POSTGRES_TABLE`, because duplicate primary keys will abort the micro-batch.
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
