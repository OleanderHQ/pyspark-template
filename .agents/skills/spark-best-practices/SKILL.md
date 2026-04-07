---
name: spark-best-practices
description: General Apache Spark best practices for scalable, maintainable, and performant DataFrame jobs.
---

# Spark Best Practices

Use this skill for general Apache Spark guidance when optimizing performance, reliability, and maintainability.

## 1) Keep execution distributed

- Avoid `collect()`, `toPandas()`, and large `take()` in core data paths.
- Materialize to driver memory only for very small control outputs (metrics, IDs, summaries).
- Keep heavy transformation and write paths in Spark DataFrame execution.

## 2) Prefer DataFrame APIs to Python loops

- Use Spark SQL/DataFrame functions so Catalyst can optimize execution plans.
- Avoid row-by-row Python logic when equivalent DataFrame expressions exist.
- Keep transformations declarative and composable.

## 3) Reduce shuffle cost

- Project and filter early to reduce data volume before joins/aggregations.
- Repartition intentionally before heavy joins/writes.
- Use `coalesce` when reducing output partitions.
- Watch for skewed keys and apply skew mitigation.

## 4) Use efficient joins

- Broadcast small dimension tables when appropriate.
- Align join key types and null handling before joins.
- Validate expected join cardinality to avoid explosive outputs.

## 5) Cache only reused intermediates

- Cache/persist DataFrames only when reused across multiple downstream actions.
- Unpersist promptly when no longer needed.
- Consider checkpointing for very long lineage plans.

## 6) Write in table-friendly layouts

- Prefer columnar formats (Parquet/Delta/Iceberg) when possible.
- Partition by bounded-cardinality business keys.
- Avoid small file explosion; compact files when needed.

## 7) Be explicit with schema and quality

- Define schemas explicitly where practical.
- Normalize data types across sources before joins/unions.
- Handle null semantics intentionally in filters, joins, and aggregations.

## 8) Observe and verify

- Use `explain()` and execution metrics/logs to inspect physical plans and shuffle boundaries.
- Track row counts and key metrics at major steps.
- Compare runtime and output quality after each optimization pass.
