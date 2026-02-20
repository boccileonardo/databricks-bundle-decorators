"""Cloud-agnostic Polars Delta IoManager.

Reads and writes Polars DataFrames as Delta tables to any storage backend
supported by Polars and `deltalake` (local, ``abfss://``, ``s3://``,
``gs://``, …).

Requires the ``polars`` and ``deltalake`` optional dependencies::

    uv add databricks-bundle-decorators[polars] deltalake
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from databricks_bundle_decorators.io_manager import (
    InputContext,
    IoManager,
    OutputContext,
)


class PolarsDeltaIoManager(IoManager):
    """Persist Polars DataFrames as Delta tables on any cloud or local filesystem.

    Write dispatch:

    - `polars.DataFrame` → ``write_delta``
    - `polars.LazyFrame` → ``sink_delta``
    - `deltalake.table.TableMerger` → ``.execute()``
      (for merge operations with predicate / action chaining)

    On the **read** side, the downstream task's parameter type annotation
    determines the method used.  Annotate the parameter as
    ``pl.DataFrame`` to receive an eager ``read_delta``; otherwise
    (including unannotated parameters) a lazy ``scan_delta`` is used
    by default.

    Parameters
    ----------
    base_path : str
        Root URI for Delta tables.  Each task creates a sub-directory
        named after its task key.  Can be a local path, an Azure URI
        (``abfss://…``), an S3 URI (``s3://…``), a GCS URI (``gs://…``),
        or any other URI scheme supported by ``deltalake``.
    storage_options : dict[str, str] | Callable[[], dict[str, str]] | None
        Credentials / options forwarded to Polars and ``deltalake`` I/O
        calls.  Can be a plain dict, a **callable** that returns a dict
        (resolved lazily on each read/write), or ``None``.

        .. note::

           ``deltalake`` uses its own key naming convention for storage
           options (e.g. ``AZURE_STORAGE_ACCOUNT_NAME`` instead of
           ``account_name``).  Consult the `deltalake documentation
           <https://delta-io.github.io/delta-rs/>`_ for the correct keys.

        Use a callable to defer credential lookup to runtime::

            from databricks_bundle_decorators import get_dbutils

            def _storage_options() -> dict[str, str]:
                dbutils = get_dbutils()
                key = dbutils.secrets.get(scope="kv", key="storage-key")
                return {"AZURE_STORAGE_ACCOUNT_NAME": "myaccount",
                        "AZURE_STORAGE_ACCOUNT_KEY": key}

            io = PolarsDeltaIoManager(
                base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
                storage_options=_storage_options,
            )

    write_options : dict[str, Any] | None
        Extra keyword arguments forwarded to the Polars write call
        (``write_delta`` / ``sink_delta``).  For example::

            {"delta_write_options": {"partition_by": ["region"]}}

        Do **not** include ``storage_options`` or ``mode`` here —
        they are managed by the IoManager.
    mode : str
        Delta write mode.  One of ``"overwrite"``, ``"append"``,
        ``"error"``, or ``"ignore"``.  Defaults to ``"overwrite"``
        for idempotent task outputs.

        For **merge** operations, ignore this parameter and return a
        fully-configured `deltalake.table.TableMerger` from your task
        instead.  The IoManager will call ``.execute()`` on it
        automatically.
    read_options : dict[str, Any] | None
        Extra keyword arguments forwarded to the Polars read call
        (``read_delta`` / ``scan_delta``).

    Example
    -------
    ::

        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(
            base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
        )

        @task(io_manager=io)
        def extract() -> pl.DataFrame:
            return pl.DataFrame({"a": [1, 2]})

        @task
        def transform(df: pl.LazyFrame):  # scan_delta on read
            print(df.collect())
    """

    def __init__(
        self,
        base_path: str,
        storage_options: dict[str, str] | Callable[[], dict[str, str]] | None = None,
        write_options: dict[str, Any] | None = None,
        read_options: dict[str, Any] | None = None,
        mode: str = "error",
    ) -> None:
        self.base_path = base_path.rstrip("/")
        self._storage_options = storage_options
        self._write_options = write_options or {}
        self._read_options = read_options or {}
        self._mode = mode

    @property
    def storage_options(self) -> dict[str, str] | None:
        """Resolve *storage_options*, calling it first if it is a callable."""
        if callable(self._storage_options):
            return self._storage_options()
        return self._storage_options

    def _uri(self, key: str) -> str:
        return f"{self.base_path}/{key}"

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a Polars DataFrame, LazyFrame, or TableMerger.

        - `polars.DataFrame` → ``write_delta``
        - `polars.LazyFrame` → ``sink_delta``
        - `deltalake.table.TableMerger` → ``.execute()``
        """
        # Handle merge builders first (no import guard needed — duck-type
        # check avoids requiring deltalake at import time).
        _merger_cls: type | None = None
        try:
            from deltalake.table import TableMerger  # type: ignore[import-untyped]

            _merger_cls = TableMerger
        except ImportError:
            pass

        if _merger_cls is not None and isinstance(obj, _merger_cls):
            obj.execute()
            return

        import polars as pl  # ty: ignore[unresolved-import]  # lazy – polars is optional

        uri = self._uri(context.task_key)

        if isinstance(obj, pl.LazyFrame):
            obj.sink_delta(
                uri,
                mode=self._mode,
                storage_options=self.storage_options,
                **self._write_options,
            )
        elif isinstance(obj, pl.DataFrame):
            obj.write_delta(
                uri,
                mode=self._mode,
                storage_options=self.storage_options,
                **self._write_options,
            )
        else:
            msg = (
                f"PolarsDeltaIoManager.write() expects a polars.DataFrame, "
                f"polars.LazyFrame, or deltalake TableMerger, "
                f"got {type(obj).__name__}"
            )
            raise TypeError(msg)

    def read(self, context: InputContext) -> Any:
        """Read a Delta table as a LazyFrame or DataFrame.

        If the downstream parameter is annotated as `polars.DataFrame`,
        returns ``read_delta`` (eager).  Otherwise returns ``scan_delta``
        (lazy `polars.LazyFrame`) — this is the default for
        unannotated parameters.
        """
        import polars as pl  # ty: ignore[unresolved-import]  # lazy – polars is optional

        uri = self._uri(context.upstream_task_key)

        if context.expected_type is pl.DataFrame:
            return pl.read_delta(
                uri, storage_options=self.storage_options, **self._read_options
            )
        return pl.scan_delta(
            uri, storage_options=self.storage_options, **self._read_options
        )
