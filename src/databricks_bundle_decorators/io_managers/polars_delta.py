"""Cloud-agnostic Polars Delta IoManager.

Reads and writes Polars DataFrames as Delta tables to any storage backend
supported by Polars and `deltalake` (local, ``abfss://``, ``s3://``,
``gs://``, …).

Requires the ``polars`` and ``deltalake`` optional dependencies::

    uv add databricks-bundle-decorators[polars] deltalake
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, cast

from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from databricks_bundle_decorators.io_manager import (
    InputContext,
    IoManager,
    OutputContext,
    RetryConfig,
    _build_replace_where,
    _needs_backfill_key_col,
    _polars_apply_partition_filter,
    _polars_extract_partition_values,
    _resolve_backfill_key,
    _should_inject_backfill_key,
    _validate_delta_mode,
)
from databricks_bundle_decorators.merge import DeltaMerge

_logger = logging.getLogger(__name__)


def _resolve_merge_partition_values(
    merge: DeltaMerge, partition_by: list[str] | None
) -> dict[str, list[str]]:
    """Return the partition-value dict to publish for a DeltaMerge write.

    - No ``partition_by`` on the task → empty dict (no downstream filtering).
    - ``merge.partition_values`` set by the caller → trust and return it
      after normalising to string values. Skips a full plan execution on
      the source LazyFrame.
    - Otherwise → fall back to scanning the source, matching the
      pre-fix behaviour.

    Callers passing ``partition_values`` are responsible for the values
    matching those actually present in the source; a mismatch causes
    downstream auto-filtering to drop rows.
    """
    if not partition_by:
        return {}
    if merge.partition_values is not None:
        # Normalise to list[str] to match _polars_extract_partition_values.
        return {
            col: [str(v) for v in merge.partition_values.get(col, [])]
            for col in partition_by
        }
    return _polars_extract_partition_values(merge.source, partition_by)


class PolarsDeltaIoManager(IoManager):
    """Persist Polars DataFrames as Delta tables on any cloud or local filesystem.

    Write dispatch:

    - `polars.DataFrame` → ``write_delta``
    - `polars.LazyFrame` → ``sink_delta``
    - `DeltaMerge` → merge/upsert operation

    On the **read** side, the downstream task's parameter type annotation
    determines the method used.  Annotate the parameter as
    ``pl.DataFrame`` to receive an eager ``read_delta``; otherwise
    (including unannotated parameters) a lazy ``scan_delta`` is used
    by default.

    Parameters
    ----------
    base_path : str | Callable[[], str]
        Root URI for Delta tables.  Each task creates a sub-directory
        named after its task key.  Can be a local path, an Azure URI
        (``abfss://…``), an S3 URI (``s3://…``), a GCS URI (``gs://…``),
        or any other URI scheme supported by ``deltalake``.

        Can also be a **callable** that returns a string, resolved lazily
        at runtime.  Use this for multi-environment deployments where the
        path depends on job parameters::

            from databricks_bundle_decorators import params

            io = PolarsDeltaIoManager(
                base_path=lambda: f"abfss://lake@{params['env']}account.dfs.core.windows.net/data",
            )
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
                return {
                    "AZURE_STORAGE_ACCOUNT_NAME": "myaccount",
                    "AZURE_STORAGE_ACCOUNT_KEY": key,
                }


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
        ``"error"``, or ``"ignore"``.  Defaults to ``"error"``.

        For **merge** operations, return a `DeltaMerge` from your task
        instead.
    read_options : dict[str, Any] | None
        Extra keyword arguments forwarded to the Polars read call
        (``read_delta`` / ``scan_delta``).
    retry : `RetryConfig` | None
        Optional retry configuration for write operations.  When set,
        failed writes are retried with exponential backoff (powered by
        `tenacity`).  Useful for handling transient Delta commit
        conflicts during concurrent backfill runs on unpartitioned
        tables.  Defaults to ``None`` (no retries).

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
        base_path: str | Callable[[], str],
        storage_options: dict[str, str] | Callable[[], dict[str, str]] | None = None,
        write_options: dict[str, Any] | None = None,
        read_options: dict[str, Any] | None = None,
        mode: str = "error",
        *,
        auto_filter: bool = True,
        retry: RetryConfig | None = None,
    ) -> None:
        _validate_delta_mode(mode, type(self).__name__)
        self._base_path = base_path
        self._storage_options = storage_options
        self._write_options = write_options or {}
        self._read_options = read_options or {}
        self._mode = mode
        self.auto_filter = auto_filter
        self.retry = retry

    @property
    def base_path(self) -> str:
        """Resolve *base_path*, calling it first if it is a callable."""
        if isinstance(self._base_path, str):
            return self._base_path.rstrip("/")
        return self._base_path().rstrip("/")

    @property
    def storage_options(self) -> dict[str, str] | None:
        """Resolve *storage_options*, calling it first if it is a callable."""
        if callable(self._storage_options):
            return cast("Callable[[], dict[str, str]]", self._storage_options)()
        return self._storage_options

    def _uri(self, key: str) -> str:
        return f"{self.base_path}/{key}"

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a Polars DataFrame, LazyFrame, or DeltaMerge.

        - `polars.DataFrame` → ``write_delta``
        - `polars.LazyFrame` → ``sink_delta``
        - `DeltaMerge` → merge/upsert operation

        When ``partition_by`` is set on the ``@task`` decorator, writes
        with ``delta_write_options={"partition_by": ...}``.
        """
        # Handle DeltaMerge definitions — build a fresh merger and execute.
        if isinstance(obj, DeltaMerge):
            uri = self._uri(context.asset_name)
            _logger.info(
                "Merging into %s (predicate=%r, actions=%s)",
                uri,
                obj.predicate,
                obj._describe_actions(),
            )
            merger = obj._build_merger(uri, storage_options=self.storage_options)
            if merger is None:
                # Target table doesn't exist yet — write source data directly.
                obj._initial_write(
                    uri,
                    storage_options=self.storage_options,
                    partition_by=context.partition_by,
                    write_options=dict(self._write_options),
                )
            else:
                merger.execute()
            self._last_partition_values = _resolve_merge_partition_values(
                obj, context.partition_by
            )
            return

        import polars as pl  # noqa: PLC0415

        uri = self._uri(context.asset_name)
        partition_by = context.partition_by

        # Inject backfill_key column if it's a partition column
        has_bk_col = isinstance(
            obj, (pl.DataFrame, pl.LazyFrame)
        ) and "backfill_key" in (
            obj.collect_schema().names()
            if isinstance(obj, pl.LazyFrame)
            else obj.columns
        )
        if _should_inject_backfill_key(partition_by, has_backfill_key_col=has_bk_col):
            bk = _resolve_backfill_key(context.backfill_key)
            obj = obj.with_columns(pl.lit(bk).alias("backfill_key"))

        # Merge partition_by into write_options for Delta
        write_opts = dict(self._write_options)
        if partition_by:
            delta_opts = write_opts.setdefault("delta_write_options", {})
            delta_opts.setdefault("partition_by", partition_by)

        # Extract partition values from data before writing
        if partition_by:
            self._last_partition_values = _polars_extract_partition_values(
                obj, partition_by
            )

        # Scope overwrite to affected partitions only
        if self._mode == "overwrite" and partition_by and self._last_partition_values:
            delta_opts = write_opts.setdefault("delta_write_options", {})
            delta_opts.setdefault(
                "predicate", _build_replace_where(self._last_partition_values)
            )

        _logger.info(
            "Writing to %s (mode=%s, partition_by=%s)", uri, self._mode, partition_by
        )

        if isinstance(obj, pl.LazyFrame):
            obj.sink_delta(
                uri,
                mode=self._mode,
                storage_options=self.storage_options,
                **write_opts,
            )
        elif isinstance(obj, pl.DataFrame):
            obj.write_delta(
                uri,
                mode=self._mode,
                storage_options=self.storage_options,
                **write_opts,
            )
        else:
            msg = (
                f"PolarsDeltaIoManager.write() expects a polars.DataFrame, "
                f"polars.LazyFrame, or DeltaMerge, "
                f"got {type(obj).__name__}"
            )
            raise TypeError(msg)

    def write_with_retry(self, context: OutputContext, obj: Any) -> None:
        """Write with retry logic.

        `DeltaMerge` and DataFrame/LazyFrame writes are all retried
        when `RetryConfig` is configured.
        """
        # DeltaMerge — retry-safe path: rebuild merger on each attempt.
        if isinstance(obj, DeltaMerge):
            if self.retry is None:
                self.write(context, obj)
                return

            uri = self._uri(context.asset_name)

            def _execute_merge() -> None:
                merger = obj._build_merger(uri, storage_options=self.storage_options)
                if merger is None:
                    obj._initial_write(
                        uri,
                        storage_options=self.storage_options,
                        partition_by=context.partition_by,
                        write_options=dict(self._write_options),
                    )
                else:
                    merger.execute()

            wait_kwargs: dict[str, Any] = {
                "multiplier": self.retry.delay,
                "exp_base": self.retry.backoff_factor,
            }
            if self.retry.max_delay is not None:
                wait_kwargs["max"] = self.retry.max_delay

            retryer = retry(
                stop=stop_after_attempt(self.retry.max_attempts),
                wait=wait_exponential(**wait_kwargs),
                reraise=True,
                before_sleep=before_sleep_log(_logger, logging.WARNING),
            )
            retryer(_execute_merge)()
            self._last_partition_values = _resolve_merge_partition_values(
                obj, context.partition_by
            )
            return

        super().write_with_retry(context, obj)

    def read(self, context: InputContext) -> Any:
        """Read a Delta table as a LazyFrame or DataFrame.

        If the downstream parameter is annotated as `polars.DataFrame`,
        returns ``read_delta`` (eager).  Otherwise returns ``scan_delta``
        (lazy `polars.LazyFrame`) — this is the default for
        unannotated parameters.

        When ``partition_by`` includes ``"backfill_key"``, reads are
        filtered to the current partition unless the upstream
        dependency uses `all_partitions()` or the consuming
        task uses ``@task(all_partitions=True)``.
        """
        import polars as pl  # noqa: PLC0415

        uri = self._uri(context.upstream_asset_name)
        _logger.info(
            "Reading from %s (partition_filter=%s)", uri, context.partition_filter
        )

        if context.expected_type is pl.DataFrame:
            result = pl.read_delta(
                uri, storage_options=self.storage_options, **self._read_options
            )
        else:
            result = pl.scan_delta(
                uri, storage_options=self.storage_options, **self._read_options
            )

        if context.partition_filter and not context.all_partitions:
            result = _polars_apply_partition_filter(result, context.partition_filter)
        elif (
            self.auto_filter
            and _needs_backfill_key_col(context.partition_by)
            and not context.all_partitions
        ):
            result = result.filter(
                pl.col("backfill_key") == _resolve_backfill_key(context.backfill_key)
            )

        return result
