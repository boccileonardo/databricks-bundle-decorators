"""Built-in IoManager implementations.

Provides ready-to-use IoManagers for common storage backends.

Polars
------
- `PolarsParquetIoManager` – Parquet files
- `PolarsDeltaIoManager` – Delta tables (requires ``deltalake``)
- `PolarsJsonIoManager` – NDJSON files
- `PolarsCsvIoManager` – CSV files

Spark (classic compute)
-----------------------
- `SparkDeltaIoManager` – Delta tables with ``spark.conf.set()`` credentials
- `SparkParquetIoManager` – Parquet files with ``spark.conf.set()`` credentials

Spark (serverless compute)
--------------------------
- `SparkServerlessDeltaIoManager` – Delta tables (Unity Catalog ext. location auth)
- `SparkServerlessParquetIoManager` – Parquet files (Unity Catalog ext. location auth)

Spark (Unity Catalog)
---------------------
- `SparkUCTableIoManager` – managed Delta tables via three‑level namespace
- `SparkUCVolumeDeltaIoManager` – Delta tables in UC Volumes
- `SparkUCVolumeParquetIoManager` – Parquet files in UC Volumes
"""

from databricks_bundle_decorators.io_managers.polars_csv import (
    PolarsCsvIoManager as PolarsCsvIoManager,
)
from databricks_bundle_decorators.io_managers.polars_delta import (
    PolarsDeltaIoManager as PolarsDeltaIoManager,
)
from databricks_bundle_decorators.io_managers.polars_json import (
    PolarsJsonIoManager as PolarsJsonIoManager,
)
from databricks_bundle_decorators.io_managers.polars_parquet import (
    PolarsParquetIoManager as PolarsParquetIoManager,
)
from databricks_bundle_decorators.io_managers.spark_delta import (
    SparkDeltaIoManager as SparkDeltaIoManager,
    SparkServerlessDeltaIoManager as SparkServerlessDeltaIoManager,
)
from databricks_bundle_decorators.io_managers.spark_parquet import (
    SparkParquetIoManager as SparkParquetIoManager,
    SparkServerlessParquetIoManager as SparkServerlessParquetIoManager,
)
from databricks_bundle_decorators.io_managers.spark_uc import (
    SparkUCTableIoManager as SparkUCTableIoManager,
    SparkUCVolumeDeltaIoManager as SparkUCVolumeDeltaIoManager,
    SparkUCVolumeParquetIoManager as SparkUCVolumeParquetIoManager,
)

__all__ = [
    "PolarsCsvIoManager",
    "PolarsDeltaIoManager",
    "PolarsJsonIoManager",
    "PolarsParquetIoManager",
    "SparkDeltaIoManager",
    "SparkParquetIoManager",
    "SparkServerlessDeltaIoManager",
    "SparkServerlessParquetIoManager",
    "SparkUCTableIoManager",
    "SparkUCVolumeDeltaIoManager",
    "SparkUCVolumeParquetIoManager",
]
