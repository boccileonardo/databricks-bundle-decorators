# Spark – Classic Compute

Classic compute IoManagers support credential injection via
`spark.conf.set()` using the `spark_configs` parameter.  This follows
the same dict-or-callable pattern as the Polars `storage_options`.

## Partitioning

Both Delta and Parquet IoManagers support `partition_by`.  When
`partition_by` includes `"logical_date"`, the column is auto-injected
via `F.lit()` before writing, and auto-filtered on read.
Partitioning uses Spark's native `partitionBy()`.

```python
io = SparkDeltaIoManager(
    base_path="abfss://lake@acct.dfs.core.windows.net/data",
    spark_configs=_configs,
    partition_by=["logical_date", "region"],
    mode="overwrite",
)
```

## Delta

::: databricks_bundle_decorators.io_managers.SparkDeltaIoManager

## Parquet

::: databricks_bundle_decorators.io_managers.SparkParquetIoManager
