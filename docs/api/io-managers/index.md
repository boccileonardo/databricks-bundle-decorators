# Built-in IoManagers

Ready-to-use `IoManager` implementations for common data formats and
compute types. Pick one, pass it to `@task(io_manager=...)`, and the
framework handles reading and writing data between tasks automatically.

Choose an IoManager based on your compute type and preferred data format.

| Compute | Format | IoManager |
|---|---|---|
| **Polars** | Parquet | `PolarsParquetIoManager` |
| | Delta | `PolarsDeltaIoManager` |
| | JSON (NDJSON) | `PolarsJsonIoManager` |
| | CSV | `PolarsCsvIoManager` |
| **Spark – Classic** | Delta | `SparkDeltaIoManager` |
| | Parquet | `SparkParquetIoManager` |
| **Spark – Serverless** | Delta | `SparkServerlessDeltaIoManager` |
| | Parquet | `SparkServerlessParquetIoManager` |
| **Spark – Unity Catalog** | Managed Tables | `SparkUCTableIoManager` |
| | Volume – Delta | `SparkUCVolumeDeltaIoManager` |
| | Volume – Parquet | `SparkUCVolumeParquetIoManager` |
