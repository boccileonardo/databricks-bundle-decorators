# Spark – Classic Compute

Classic compute IoManagers support credential injection via
`spark.conf.set()` using the `spark_configs` parameter.  This follows
the same dict-or-callable pattern as the Polars `storage_options`.

## Delta

::: databricks_bundle_decorators.io_managers.SparkDeltaIoManager

## Parquet

::: databricks_bundle_decorators.io_managers.SparkParquetIoManager
