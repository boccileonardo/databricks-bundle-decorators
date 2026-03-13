# Spark – Unity Catalog

Unity Catalog IoManagers work on **both** classic and serverless compute.
UC handles authentication and access control, so no credential injection
is needed.

## Partitioning

All UC IoManagers support `partition_by`.  `"logical_date"` is
auto-injected on write and auto-filtered on read.  Managed tables use
`partitionBy()` with `saveAsTable()`; volume paths use `partitionBy()`
with `save()`.

```python
io = SparkUCTableIoManager(
    catalog="main",
    schema="staging",
    partition_by="logical_date",
    mode="overwrite",
)
```

## Managed Tables

::: databricks_bundle_decorators.io_managers.SparkUCTableIoManager

## Volume – Delta

::: databricks_bundle_decorators.io_managers.SparkUCVolumeDeltaIoManager

## Volume – Parquet

::: databricks_bundle_decorators.io_managers.SparkUCVolumeParquetIoManager
