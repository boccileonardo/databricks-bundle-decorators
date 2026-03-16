"""Cloud-agnostic Polars Parquet IoManager.

Reads and writes Polars DataFrames as Parquet files to any storage backend
supported by Polars (local, ``abfss://``, ``s3://``, ``gs://``, …).

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
    storage_options : dict[str, str] | Callable[[], dict[str, str]] | None
        Credentials / options forwarded to Polars I/O calls.
        Can be a plain dict, a **callable** that returns a dict (resolved
        lazily on each read/write), or ``None``.

        Use a callable to defer credential lookup to runtime — this is
        essential when credentials come from ``get_dbutils`` which is only
        available on a Databricks cluster, not during local bundle deploy::

            from databricks_bundle_decorators import get_dbutils

            def _storage_options() -> dict[str, str]:
                dbutils = get_dbutils()
                key = dbutils.secrets.get(scope="kv", key="storage-key")
                return {"account_name": "myaccount", "account_key": key}

            io = PolarsParquetIoManager(
                base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
                storage_options=_storage_options,
            )

        A plain dict also works when credentials are known statically::

            {"account_name": "...", "account_key": "..."}   # Azure
            {"aws_access_key_id": "...", "aws_secret_access_key": "..."}  # S3

    write_options : dict[str, Any] | None
        Extra keyword arguments forwarded to the Polars write call
        (``write_parquet`` / ``sink_parquet``).  For example::

            {"compression": "zstd", "row_group_size": 100_000}

        Do **not** include ``storage_options`` here — use the
        dedicated parameter instead.
    read_options : dict[str, Any] | None
        Extra keyword arguments forwarded to the Polars read call
        (``read_parquet`` / ``scan_parquet``).

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
            return cast(Callable[[], dict[str, str]], self._storage_options)()
        return self._storage_options

    def _uri(self, key: str) -> str:
        return f"{self.base_path}/{key}"

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a Polars DataFrame or LazyFrame to Parquet.

        - `polars.DataFrame` → ``write_parquet`` (native ``partition_by``)
        - `polars.LazyFrame` → ``sink_parquet`` (``pl.PartitionByKey``)

        When ``partition_by`` is set on the ``@task`` decorator, writes
        to Hive-style partitioned directories.
        """
        import polars as pl  # ty: ignore[unresolved-import]  # lazy – polars is optional

        base_uri = self._uri(context.task_key)
        partition_by = context.partition_by

        # Inject logical_date column if it's a partition column
        if _needs_logical_date_col(partition_by):
            ld_str = _format_logical_date(context.logical_date)
            obj = obj.with_columns(pl.lit(ld_str).alias("logical_date"))

        if partition_by:
            if isinstance(obj, pl.LazyFrame):
                obj.sink_parquet(
                    pl.PartitionByKey(base_uri, by=partition_by),
                    mkdir=True,
                    storage_options=self.storage_options,
                    **self._write_options,
                )
            elif isinstance(obj, pl.DataFrame):
                obj.write_parquet(
                    base_uri,
                    partition_by=partition_by,
                    mkdir=True,
                    storage_options=self.storage_options,
                    **self._write_options,
                )
            else:
                msg = (
                    f"PolarsParquetIoManager.write() expects a polars.DataFrame or "
                    f"polars.LazyFrame, got {type(obj).__name__}"
                )
                raise TypeError(msg)
        else:
            uri = f"{base_uri}.parquet"
            if isinstance(obj, pl.LazyFrame):
                obj.sink_parquet(
                    uri, storage_options=self.storage_options, **self._write_options
                )
            elif isinstance(obj, pl.DataFrame):
                obj.write_parquet(
                    uri, storage_options=self.storage_options, **self._write_options
                )
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

        When ``partition_by`` is set on the producing ``@task``, reads
        from the Hive-partitioned directory.  By default only the
        current ``logical_date`` partition is returned; use
        `all_partitions()` on the upstream dependency or
        ``@task(all_partitions=True)`` on the consuming task to read
        all partitions.
        """
        import polars as pl  # ty: ignore[unresolved-import]  # lazy – polars is optional

        base_uri = self._uri(context.upstream_task_key)
        partition_by = context.partition_by

        if partition_by:
            glob_uri = f"{base_uri}/**/*.parquet"
            if context.expected_type is pl.DataFrame:
                result = pl.read_parquet(
                    glob_uri,
                    hive_partitioning=True,
                    storage_options=self.storage_options,
                    **self._read_options,
                )
            else:
                result = pl.scan_parquet(
                    glob_uri,
                    hive_partitioning=True,
                    storage_options=self.storage_options,
                    **self._read_options,
                )
            if _needs_logical_date_col(partition_by) and not context.all_partitions:
                result = result.filter(
                    pl.col("logical_date") == _format_logical_date(context.logical_date)
                )
            return result

        uri = f"{base_uri}.parquet"
        if context.expected_type is pl.DataFrame:
            return pl.read_parquet(
                uri, storage_options=self.storage_options, **self._read_options
            )
        return pl.scan_parquet(
            uri, storage_options=self.storage_options, **self._read_options
        )
