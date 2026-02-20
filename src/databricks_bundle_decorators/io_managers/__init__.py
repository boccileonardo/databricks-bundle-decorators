"""Built-in IoManager implementations.

Provides ready-to-use IoManagers for common storage backends.
"""

from databricks_bundle_decorators.io_managers.polars_parquet import (
    PolarsParquetIoManager as PolarsParquetIoManager,
)

__all__ = [
    "PolarsParquetIoManager",
]
