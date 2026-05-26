# Spark – Unity Catalog

Unity Catalog IoManagers work on **both** classic and serverless compute.
UC handles authentication and access control, so no credential injection
is needed.

## Partitioning

All UC IoManagers support `partition_by` via the `@task` decorator.
`"backfill_key"` is auto-injected on write and auto-filtered on read.
Managed tables use `partitionBy()` with `saveAsTable()`; volume paths
use `partitionBy()` with `save()`.

```python
io = SparkUCTableIoManager(
    catalog="main",
    schema="staging",
    mode="overwrite",
)

@task(io_manager=io, partition_by="backfill_key")
def extract(): ...
```

## Managed Tables

!!! tip "Merge / Upsert"

    `mode="merge"` is **not** a valid write mode and will raise a `ValueError`.
    To perform merge/upsert operations, return a `DeltaMerge` from your task
    function.
    See [Delta Write Modes & Merge](index.md#delta-write-modes-merge) for
    full examples.

    This applies to all Delta-backed UC IoManagers: `SparkUCTableIoManager`,
    `SparkUCVolumeDeltaIoManager`.

::: databricks_bundle_decorators.io_managers.SparkUCTableIoManager

### External Tables

Set `location` to create external tables backed by storage you control:

```python
io = SparkUCTableIoManager(
    catalog="main",
    schema="bronze",
    location="s3://my-bucket/delta",
)

@task(io_manager=io, output_name="customers")
def extract_customers():
    ...  # table: main.bronze.customers
         # path:  s3://my-bucket/delta/customers
```

The path must be registered as a UC
[external location](https://docs.databricks.com/en/sql/language-manual/sql-ref-external-locations.html).
Reads use `spark.table()` so location is transparent.

## Volume – Delta

::: databricks_bundle_decorators.io_managers.SparkUCVolumeDeltaIoManager

## Volume – Parquet

::: databricks_bundle_decorators.io_managers.SparkUCVolumeParquetIoManager
