"""Cloud-agnostic Polars JSON (NDJSON) IoManager.

Reads and writes Polars DataFrames as newline-delimited JSON (NDJSON) files
to any storage backend supported by Polars (local, ``abfss://``, ``s3://``,
``gs://``, …).

NDJSON is used instead of standard JSON because it supports streaming
reads/writes and cloud storage via ``storage_options``.

Requires the ``polars`` optional dependency::

    uv add databricks-bundle-decorators[polars]
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from databricks_bundle_decorators.io_manager import (
    InputContext,
    IoManager,
    OutputContext,
    _format_logical_date,
    _needs_logical_date_col,
    _normalize_partition_by,
)


class PolarsJsonIoManager(IoManager):
    """Persist Polars DataFrames as NDJSON on any cloud or local filesystem.

    Uses newline-delimited JSON (NDJSON) format, the standard for
    streaming data pipelines.  This is the only JSON variant in Polars
    that supports cloud storage (``storage_options``) and lazy I/O.

    Write dispatch:

    - `polars.LazyFrame` → ``sink_ndjson``
    - `polars.DataFrame` → ``.lazy()`` then ``sink_ndjson``
      (routed through the lazy path for cloud storage support)

    On the **read** side, the downstream task's parameter type annotation
    determines the method used.  Annotate the parameter as
    ``pl.DataFrame`` to receive an eager ``read_ndjson``; otherwise
    (including unannotated parameters) a lazy ``scan_ndjson`` is used
    by default.

    Parameters
    ----------
    base_path : str
        Root URI for NDJSON files.  Can be a local path (``/tmp/data``),
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

            io = PolarsJsonIoManager(
                base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
                storage_options=_storage_options,
            )

    write_options : dict[str, Any] | None
        Extra keyword arguments forwarded to the Polars write call
        (``sink_ndjson``).

        Do **not** include ``storage_options`` here — use the
        dedicated parameter instead.
    read_options : dict[str, Any] | None
        Extra keyword arguments forwarded to the Polars read call
        (``read_ndjson`` / ``scan_ndjson``).

    Example
    -------
    ::

        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(
            base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
        )

        @task(io_manager=io)
        def extract() -> pl.LazyFrame:    # sink_ndjson on write
            return pl.LazyFrame({"a": [1, 2]})

        @task
        def transform(df: pl.LazyFrame):  # scan_ndjson on read
            print(df.collect())
    """

    def __init__(
        self,
        base_path: str,
        storage_options: dict[str, str] | Callable[[], dict[str, str]] | None = None,
        write_options: dict[str, Any] | None = None,
        read_options: dict[str, Any] | None = None,
        partition_by: str | list[str] | None = None,
    ) -> None:
        self.base_path = base_path.rstrip("/")
        self._storage_options = storage_options
        self._write_options = write_options or {}
        self._read_options = read_options or {}
        self._partition_by = _normalize_partition_by(partition_by)

    @property
    def storage_options(self) -> dict[str, str] | None:
        """Resolve *storage_options*, calling it first if it is a callable."""
        if callable(self._storage_options):
            return cast(Callable[[], dict[str, str]], self._storage_options)()
        return self._storage_options

    def _uri(self, key: str) -> str:
        return f"{self.base_path}/{key}"

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a Polars DataFrame or LazyFrame as NDJSON.

        - `polars.LazyFrame` → ``sink_ndjson``
        - `polars.DataFrame` → ``.lazy()`` then ``sink_ndjson``

        When ``partition_by`` is set, writes to Hive-style partitioned
        directories using ``pl.PartitionByKey``.
        """
        import polars as pl  # ty: ignore[unresolved-import]  # lazy – polars is optional

        base_uri = self._uri(context.task_key)

        # Inject logical_date column if it's a partition column
        if _needs_logical_date_col(self._partition_by):
            ld_str = _format_logical_date(context.logical_date)
            obj = obj.with_columns(pl.lit(ld_str).alias("logical_date"))

        if self._partition_by:
            if isinstance(obj, pl.LazyFrame):
                obj.sink_ndjson(
                    pl.PartitionByKey(base_uri, by=self._partition_by),
                    mkdir=True,
                    storage_options=self.storage_options,
                    **self._write_options,
                )
            elif isinstance(obj, pl.DataFrame):
                obj.lazy().sink_ndjson(
                    pl.PartitionByKey(base_uri, by=self._partition_by),
                    mkdir=True,
                    storage_options=self.storage_options,
                    **self._write_options,
                )
            else:
                msg = (
                    f"PolarsJsonIoManager.write() expects a polars.DataFrame or "
                    f"polars.LazyFrame, got {type(obj).__name__}"
                )
                raise TypeError(msg)
        else:
            uri = f"{base_uri}.ndjson"
            if isinstance(obj, pl.LazyFrame):
                obj.sink_ndjson(
                    uri, storage_options=self.storage_options, **self._write_options
                )
            elif isinstance(obj, pl.DataFrame):
                obj.lazy().sink_ndjson(
                    uri, storage_options=self.storage_options, **self._write_options
                )
            else:
                msg = (
                    f"PolarsJsonIoManager.write() expects a polars.DataFrame or "
                    f"polars.LazyFrame, got {type(obj).__name__}"
                )
                raise TypeError(msg)

    def read(self, context: InputContext) -> Any:
        """Read NDJSON as a LazyFrame or DataFrame.

        If the downstream parameter is annotated as `polars.DataFrame`,
        returns ``read_ndjson`` (eager).  Otherwise returns ``scan_ndjson``
        (lazy `polars.LazyFrame`) — this is the default for
        unannotated parameters.

        When ``partition_by`` is set, reads from the Hive-partitioned
        directory.  By default only the current ``logical_date``
        partition is returned; use `all_partitions()` on the
        upstream dependency or ``@task(all_partitions=True)`` on
        the consuming task to read all partitions.
        """
        import polars as pl  # ty: ignore[unresolved-import]  # lazy – polars is optional

        base_uri = self._uri(context.upstream_task_key)

        if self._partition_by:
            glob_uri = f"{base_uri}/**/*.ndjson"
            if context.expected_type is pl.DataFrame:
                result = pl.read_ndjson(
                    glob_uri,
                    hive_partitioning=True,
                    storage_options=self.storage_options,
                    **self._read_options,
                )
            else:
                result = pl.scan_ndjson(
                    glob_uri,
                    hive_partitioning=True,
                    storage_options=self.storage_options,
                    **self._read_options,
                )
            if (
                _needs_logical_date_col(self._partition_by)
                and not context.all_partitions
            ):
                ld_str = _format_logical_date(context.logical_date)
                result = result.filter(pl.col("logical_date") == ld_str)
            return result

        uri = f"{base_uri}.ndjson"
        if context.expected_type is pl.DataFrame:
            return pl.read_ndjson(
                uri, storage_options=self.storage_options, **self._read_options
            )
        return pl.scan_ndjson(
            uri, storage_options=self.storage_options, **self._read_options
        )
