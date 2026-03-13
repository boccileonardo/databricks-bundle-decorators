# Spark – Serverless Compute

Serverless compute does **not** support `spark.conf.set()`.
The `base_path` **must** be a storage location registered as a
Unity Catalog **external location** — serverless compute can only
access paths governed by UC.  Arbitrary cloud storage URIs that are
not registered as external locations will fail at runtime.

## Partitioning

Same behaviour as classic compute — `partition_by` uses Spark's
native `partitionBy()`, and `"logical_date"` is auto-injected on
write and auto-filtered on read.

## Delta

::: databricks_bundle_decorators.io_managers.SparkServerlessDeltaIoManager

## Parquet

::: databricks_bundle_decorators.io_managers.SparkServerlessParquetIoManager
