# Built-in IoManagers

## Polars

### Parquet

::: databricks_bundle_decorators.io_managers.PolarsParquetIoManager

### Delta

::: databricks_bundle_decorators.io_managers.PolarsDeltaIoManager

### JSON (NDJSON)

::: databricks_bundle_decorators.io_managers.PolarsJsonIoManager

### CSV

::: databricks_bundle_decorators.io_managers.PolarsCsvIoManager

## Spark – Classic Compute

Classic compute IoManagers support credential injection via
`spark.conf.set()` using the `spark_configs` parameter.  This follows
the same dict-or-callable pattern as the Polars `storage_options`.

### Delta

::: databricks_bundle_decorators.io_managers.SparkDeltaIoManager

### Parquet

::: databricks_bundle_decorators.io_managers.SparkParquetIoManager

## Spark – Serverless Compute

Serverless compute does **not** support `spark.conf.set()`.
The `base_path` **must** be a storage location registered as a
Unity Catalog **external location** — serverless compute can only
access paths governed by UC.  Arbitrary cloud storage URIs that are
not registered as external locations will fail at runtime.

### Delta

::: databricks_bundle_decorators.io_managers.SparkServerlessDeltaIoManager

### Parquet

::: databricks_bundle_decorators.io_managers.SparkServerlessParquetIoManager

## Spark – Unity Catalog

Unity Catalog IoManagers work on **both** classic and serverless compute.
UC handles authentication and access control, so no credential injection
is needed.

### Managed Tables

::: databricks_bundle_decorators.io_managers.SparkUCTableIoManager

### Volume – Delta

::: databricks_bundle_decorators.io_managers.SparkUCVolumeDeltaIoManager

### Volume – Parquet

::: databricks_bundle_decorators.io_managers.SparkUCVolumeParquetIoManager
