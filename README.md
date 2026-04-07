# pyspark-template

Template repository for Python Spark jobs that run on Oleander-managed Spark.

The repository uses:

- `uv` for local Python dependency management
- `entrypoint.py` as the Spark job entrypoint
- `mylib/` for Python modules that are packaged as `pyFiles`
- Docker to build the deployment virtual environment artifact

You can rename `entrypoint.py` and `mylib/` to fit your job layout.

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
