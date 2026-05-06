# Spark – Classic Compute

Classic compute IoManagers support credential injection via
`spark.conf.set()` using the `spark_configs` parameter.  This follows
the same dict-or-callable pattern as the Polars `storage_options`.

## Partitioning

Both Delta and Parquet IoManagers support `partition_by` via the
`@task` decorator.  When `partition_by` includes `"backfill_key"`,
the column is auto-injected via `F.lit()` before writing, and
auto-filtered on read.  Partitioning uses Spark's native
`partitionBy()`.

```python
io = SparkDeltaIoManager(
    base_path="abfss://lake@acct.dfs.core.windows.net/data",
    spark_configs=_configs,
    mode="overwrite",
)

@task(io_manager=io, partition_by=["backfill_key", "region"])
def extract(): ...
```

## Delta

!!! tip "Merge / Upsert"

    `mode="merge"` is **not** a valid write mode and will raise a `ValueError`.
    To perform merge/upsert operations, return a `DeltaMerge` from your task
    function.
    See [Delta Write Modes & Merge](index.md#delta-write-modes-merge) for
    full examples.

::: databricks_bundle_decorators.io_managers.SparkDeltaIoManager

## Parquet

::: databricks_bundle_decorators.io_managers.SparkParquetIoManager
