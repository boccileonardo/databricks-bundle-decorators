# Built-in IoManagers

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
