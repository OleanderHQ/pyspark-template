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

Upload the Spark job entrypoint together with the Python module archive and virtual environment:

```bash
oleander spark jobs upload entrypoint.py \
  --py-files out/pyfiles.zip \
  --virtualenv out/environment.tar.gz
```

Submit the uploaded job:

```bash
oleander spark jobs submit entrypoint.py \
  --namespace <namespace> \
  --name <job-name> \
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
| `PUBLIC_STREAM_CHECKPOINT_LOCATION` | Durable checkpoint path (e.g. `s3a://bucket/checkpoint`). Defaults to `/tmp/oleander-public-stream-checkpoint` which does not survive restarts. |
| `POSTGRES_TABLE` | Target Postgres table name (default `public_stream_messages`) |
| `ICEBERG_TABLE` | Fully-qualified Iceberg table for raw messages (default `oleander.default.public_stream_messages`) |

### Conditional (Kafka authentication)

Only required when the Kafka cluster uses SASL authentication.

| Variable | Description |
| --- | --- |
| `KAFKA_SECURITY_PROTOCOL` | Security protocol (e.g. `SASL_SSL`) |
| `KAFKA_SASL_MECHANISM` | SASL mechanism (e.g. `PLAIN`) |
| `KAFKA_API_KEY` | SASL username |
| `KAFKA_API_SECRET` | SASL password |
