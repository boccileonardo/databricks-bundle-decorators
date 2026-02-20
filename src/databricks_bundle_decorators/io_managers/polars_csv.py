"""Cloud-agnostic Polars CSV IoManager.

Reads and writes Polars DataFrames as CSV files to any storage backend
supported by Polars (local, ``abfss://``, ``s3://``, ``gs://``, …).

Requires the ``polars`` optional dependency::

    uv add databricks-bundle-decorators[polars]
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from databricks_bundle_decorators.io_manager import (
    InputContext,
    IoManager,
    OutputContext,
)


class PolarsCsvIoManager(IoManager):
    """Persist Polars DataFrames as CSV on any cloud or local filesystem.

    Write dispatch:

    - `polars.LazyFrame` → ``sink_csv``
    - `polars.DataFrame` → ``write_csv``

    On the **read** side, the downstream task's parameter type annotation
    determines the method used.  Annotate the parameter as
    ``pl.DataFrame`` to receive an eager ``read_csv``; otherwise
    (including unannotated parameters) a lazy ``scan_csv`` is used
    by default.

    Parameters
    ----------
    base_path : str
        Root URI for CSV files.  Can be a local path (``/tmp/data``),
        an Azure URI (``abfss://container@account.dfs.core.windows.net/path``),
        an S3 URI (``s3://bucket/prefix``), a GCS URI (``gs://bucket/prefix``),
        or any other URI scheme that Polars supports.
    storage_options : dict[str, str] | Callable[[], dict[str, str]] | None
        Credentials / options forwarded to Polars I/O calls.
        Can be a plain dict, a **callable** that returns a dict (resolved
        lazily on each read/write), or ``None``.

        Use a callable to defer credential lookup to runtime::

            from databricks_bundle_decorators import get_dbutils

            def _storage_options() -> dict[str, str]:
                dbutils = get_dbutils()
                key = dbutils.secrets.get(scope="kv", key="storage-key")
                return {"account_name": "myaccount", "account_key": key}

            io = PolarsCsvIoManager(
                base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
                storage_options=_storage_options,
            )

    write_options : dict[str, Any] | None
        Extra keyword arguments forwarded to the Polars write call
        (``write_csv`` / ``sink_csv``).  For example::

            {"separator": ";", "quote_char": '"'}

        Do **not** include ``storage_options`` here — use the
        dedicated parameter instead.
    read_options : dict[str, Any] | None
        Extra keyword arguments forwarded to the Polars read call
        (``read_csv`` / ``scan_csv``).

    Example
    -------
    ::

        from databricks_bundle_decorators.io_managers import PolarsCsvIoManager

        io = PolarsCsvIoManager(
            base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
        )

        @task(io_manager=io)
        def extract() -> pl.LazyFrame:    # sink_csv on write
            return pl.LazyFrame({"a": [1, 2]})

        @task
        def transform(df: pl.LazyFrame):  # scan_csv on read
            print(df.collect())
    """

    def __init__(
        self,
        base_path: str,
        storage_options: dict[str, str] | Callable[[], dict[str, str]] | None = None,
        write_options: dict[str, Any] | None = None,
        read_options: dict[str, Any] | None = None,
    ) -> None:
        self.base_path = base_path.rstrip("/")
        self._storage_options = storage_options
        self._write_options = write_options or {}
        self._read_options = read_options or {}

    @property
    def storage_options(self) -> dict[str, str] | None:
        """Resolve *storage_options*, calling it first if it is a callable."""
        if callable(self._storage_options):
            return self._storage_options()
        return self._storage_options

    def _uri(self, key: str) -> str:
        return f"{self.base_path}/{key}.csv"

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a Polars DataFrame or LazyFrame as CSV.

        - `polars.DataFrame` → ``write_csv``
        - `polars.LazyFrame` → ``sink_csv``
        """
        import polars as pl  # ty: ignore[unresolved-import]  # lazy – polars is optional

        uri = self._uri(context.task_key)

        if isinstance(obj, pl.LazyFrame):
            obj.sink_csv(
                uri, storage_options=self.storage_options, **self._write_options
            )
        elif isinstance(obj, pl.DataFrame):
            obj.write_csv(
                uri, storage_options=self.storage_options, **self._write_options
            )
        else:
            msg = (
                f"PolarsCsvIoManager.write() expects a polars.DataFrame or "
                f"polars.LazyFrame, got {type(obj).__name__}"
            )
            raise TypeError(msg)

    def read(self, context: InputContext) -> Any:
        """Read CSV as a LazyFrame or DataFrame.

        If the downstream parameter is annotated as `polars.DataFrame`,
        returns ``read_csv`` (eager).  Otherwise returns ``scan_csv``
        (lazy `polars.LazyFrame`) — this is the default for
        unannotated parameters.
        """
        import polars as pl  # ty: ignore[unresolved-import]  # lazy – polars is optional

        uri = self._uri(context.upstream_task_key)

        if context.expected_type is pl.DataFrame:
            return pl.read_csv(
                uri, storage_options=self.storage_options, **self._read_options
            )
        return pl.scan_csv(
            uri, storage_options=self.storage_options, **self._read_options
        )
