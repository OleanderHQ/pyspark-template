---
name: oleander-spark-submit
description: How to submit, monitor, and configure Spark jobs on oleander using the CLI & TypeScript SDK.
---

# oleander Spark Submit

Use this skill when submitting Spark jobs to oleander, monitoring run state, or building automated workflows that execute Spark jobs.

## CLI submission

Upload a script and submit it from the terminal:

```bash
oleander spark jobs submit my_job.py \
  --namespace my_namespace \
  --name my-job-name \
  --wait
```

With arguments passed to the script:

```bash
oleander spark jobs submit my_job.py \
  --namespace my_namespace \
  --name my-job-name \
  --wait \
  --args "--input-table oleander.default.sf_311 --output-catalog oleander.results"
```

`--wait` blocks until the run reaches a terminal state (`COMPLETE`, `FAIL`, or `ABORT`).

## TypeScript SDK submission

Use `@oleanderhq/sdk` to submit jobs programmatically:

```typescript
import { Oleander, RunNotFoundError } from "@oleanderhq/sdk";

const oleander = new Oleander(); // reads OLEANDER_API_KEY from env

const { runId } = await oleander.submitSparkJob({
  cluster: "oleander",
  namespace: "my_namespace",
  name: "my-job-name",
  entrypoint: "my_job.py",
});
```

Submit and wait for completion with built-in polling:

```typescript
const result = await oleander.submitSparkJobAndWait({
  cluster: "oleander",
  namespace: "my_namespace",
  name: "my-job-name",
  entrypoint: "my_job.py",
});
```

## Polling for run state

When not using `submitSparkJobAndWait`, poll manually with `getRun`:

```typescript
import { Oleander, RunNotFoundError } from "@oleanderhq/sdk";

const oleander = new Oleander();
const { runId } = await oleander.submitSparkJob({ ... });

const started = Date.now();
const timeoutMs = 900_000; // 15 minutes
let notFoundRetries = 10;

while (Date.now() - started < timeoutMs) {
  await wait.for({ seconds: 60 });
  try {
    const run = await oleander.getRun(runId);
    const state = run.state ?? "";
    if (state === "COMPLETE" || state === "FAIL" || state === "ABORT") {
      return { runId, state, run };
    }
  } catch (error) {
    if (error instanceof RunNotFoundError) {
      notFoundRetries--;
      if (notFoundRetries <= 0) throw error;
      continue; // run may not be visible yet; retry
    }
    throw error;
  }
}

throw new Error(`Timeout waiting for run ${runId}`);
```

Always handle `RunNotFoundError` — a freshly submitted run may not be immediately visible.

## Cluster options

|Cluster|Use case|
|---|---|
|`"oleander"`|Default managed Spark cluster|
|`"emr-serverless"`|AWS EMR Serverless|
|`"glue"`|AWS Glue|

Use `"oleander"` unless you have a specific reason to use a different backend.

## Machine types

Machine types follow the pattern `spark.<size>.<class>`:

- Classes: `b` (balanced), `c` (compute), `m` (memory)
- Sizes: `1` through `16`
- Default: `spark.1.b` for both driver and executors

Choose memory-optimized (`m`) when joins or aggregations spill. Choose compute-optimized (`c`) for CPU-bound transformations.

## Executor count

- Default: 2 executors
- Range: 1–20
- Scale up for large data volumes; keep low for small or exploratory jobs

## Script structure conventions

- Accept table names and output destinations as `argparse` arguments, not hardcoded.
- Validate required env vars at startup and fail fast with a clear error message.
- Return a non-zero exit code on failure (`sys.exit(2)`), not just a log message.
- Print a structured JSON summary at the end for observability.
