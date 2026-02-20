"""Cloud-agnostic Polars Parquet IoManager.

Reads and writes Polars DataFrames as Parquet files to any storage backend
supported by Polars (local, ``abfss://``, ``s3://``, ``gs://``, …).

Requires the ``polars`` optional dependency::

    uv add databricks-bundle-decorators[polars]
"""

from __future__ import annotations

from typing import Any

from databricks_bundle_decorators.io_manager import (
    InputContext,
    IoManager,
    OutputContext,
)


class PolarsParquetIoManager(IoManager):
    """Persist Polars DataFrames as Parquet on any cloud or local filesystem.

    Automatically dispatches based on return-value type:

    - `polars.DataFrame` → ``write_parquet`` / ``read_parquet``
    - `polars.LazyFrame` → ``sink_parquet`` / ``scan_parquet``

    On the **read** side, the downstream task's parameter type annotation
    determines the method used.  Annotate the parameter as
    ``pl.DataFrame`` to receive an eager read; otherwise (including
    unannotated parameters) a lazy ``scan_parquet`` is used by default.

    Parameters
    ----------
    base_path : str
        Root URI for Parquet files.  Can be a local path (``/tmp/data``),
        an Azure URI (``abfss://container@account.dfs.core.windows.net/path``),
        an S3 URI (``s3://bucket/prefix``), a GCS URI (``gs://bucket/prefix``),
        or any other URI scheme that Polars supports.
    storage_options : dict[str, str] | None
        Credentials / options forwarded to Polars I/O calls.
        For example::

            {"account_name": "...", "account_key": "..."}   # Azure
            {"aws_access_key_id": "...", "aws_secret_access_key": "..."}  # S3

    Example
    -------
    ::

        from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

        io = PolarsParquetIoManager(
            base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
            storage_options={"account_name": "myaccount", "account_key": "***"},
        )

        @task(io_manager=io)
        def extract() -> pl.LazyFrame:    # sink_parquet on write
            return pl.LazyFrame({"a": [1, 2]})

        @task
        def transform(df: pl.LazyFrame):  # scan_parquet on read
            print(df.collect())
    """

    def __init__(
        self,
        base_path: str,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        self.base_path = base_path.rstrip("/")
        self.storage_options = storage_options

    def _uri(self, key: str) -> str:
        return f"{self.base_path}/{key}.parquet"

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a Polars DataFrame or LazyFrame to Parquet.

        - `polars.DataFrame` → ``write_parquet``
        - `polars.LazyFrame` → ``sink_parquet``
        """
        import polars as pl  # ty: ignore[unresolved-import]  # lazy – polars is optional

        uri = self._uri(context.task_key)

        if isinstance(obj, pl.LazyFrame):
            obj.sink_parquet(uri, storage_options=self.storage_options)
        elif isinstance(obj, pl.DataFrame):
            obj.write_parquet(uri, storage_options=self.storage_options)
        else:
            msg = (
                f"PolarsParquetIoManager.write() expects a polars.DataFrame or "
                f"polars.LazyFrame, got {type(obj).__name__}"
            )
            raise TypeError(msg)

    def read(self, context: InputContext) -> Any:
        """Read Parquet as a LazyFrame or DataFrame.

        If the downstream parameter is annotated as `polars.DataFrame`,
        returns ``read_parquet`` (eager).  Otherwise returns ``scan_parquet``
        (lazy `polars.LazyFrame`) — this is the default for
        unannotated parameters.
        """
        import polars as pl  # ty: ignore[unresolved-import]  # lazy – polars is optional

        uri = self._uri(context.upstream_task_key)

        if context.expected_type is pl.DataFrame:
            return pl.read_parquet(uri, storage_options=self.storage_options)
        return pl.scan_parquet(uri, storage_options=self.storage_options)
