# Spark – Serverless Compute

Serverless compute does **not** support `spark.conf.set()`.
The `base_path` **must** be a storage location registered as a
Unity Catalog **external location** — serverless compute can only
access paths governed by UC.  Arbitrary cloud storage URIs that are
not registered as external locations will fail at runtime.

## Partitioning

Same behaviour as classic compute — `partition_by` is specified on the
`@task` decorator and uses Spark's native `partitionBy()`.  
`"backfill_key"` is auto-injected on write and auto-filtered on read.

## Delta

!!! tip "Merge / Upsert"

    `mode="merge"` is **not** a valid write mode and will raise a `ValueError`.
    To perform merge/upsert operations, return a `DeltaMerge` from your task
    function.
    See [Delta Write Modes & Merge](index.md#delta-write-modes-merge) for
    full examples.

::: databricks_bundle_decorators.io_managers.SparkServerlessDeltaIoManager

## Parquet

::: databricks_bundle_decorators.io_managers.SparkServerlessParquetIoManager
