"""IoManager abstraction for inter-task data persistence.

Follows the Dagster IoManager pattern: large data (DataFrames, datasets)
is written to *permanent storage* (Delta tables, Unity Catalog volumes,
cloud object stores) rather than being squeezed through Databricks task
values.

Users implement concrete IoManagers and attach them to tasks via the
``io_manager`` parameter of the ``@task`` decorator.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _normalize_partition_by(
    partition_by: str | list[str] | None,
) -> list[str] | None:
    """Normalize ``partition_by`` to a list or None."""
    if partition_by is None:
        return None
    if isinstance(partition_by, str):
        return [partition_by]
    return partition_by


def _needs_logical_date_col(partition_by: list[str] | None) -> bool:
    """Return True if ``"logical_date"`` is in *partition_by*."""
    return partition_by is not None and "logical_date" in partition_by


def _format_logical_date(context_logical_date: datetime | None) -> str:
    """Format the logical_date for use as a partition column value."""
    if context_logical_date is not None:
        return context_logical_date.strftime("%Y-%m-%d")
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")


def _polars_extract_partition_values(
    scan: Any, partition_by: list[str]
) -> dict[str, list[str]]:
    """Extract distinct partition values from a Polars scan (LazyFrame)."""
    import polars as pl  # ty: ignore[unresolved-import]

    df = scan.select(partition_by).unique().collect()
    return {col: sorted(df[col].cast(pl.Utf8).to_list()) for col in partition_by}


def _polars_apply_partition_filter(
    result: Any, partition_filter: dict[str, list[str]]
) -> Any:
    """Filter a Polars DataFrame/LazyFrame to matching partition values."""
    import polars as pl  # ty: ignore[unresolved-import]

    for col, values in partition_filter.items():
        if len(values) == 1:
            result = result.filter(pl.col(col) == values[0])
        else:
            result = result.filter(pl.col(col).is_in(values))
    return result


def _spark_extract_partition_values(
    df: Any, partition_by: list[str]
) -> dict[str, list[str]]:
    """Extract distinct partition values from a PySpark DataFrame."""
    rows = df.select(*partition_by).distinct().collect()
    return {col: sorted({str(row[col]) for row in rows}) for col in partition_by}


def _spark_apply_partition_filter(
    result: Any, partition_filter: dict[str, list[str]]
) -> Any:
    """Filter a PySpark DataFrame to matching partition values."""
    from pyspark.sql import functions as F  # type: ignore[import-untyped]

    for col, values in partition_filter.items():
        if len(values) == 1:
            result = result.filter(F.col(col) == values[0])
        else:
            result = result.filter(F.col(col).isin(values))
    return result


@dataclass
class OutputContext:
    """Context provided to `IoManager.write` when persisting a task's return value."""

    job_name: str
    task_key: str
    run_id: str
    logical_date: datetime | None = None
    partition_by: list[str] | None = None


@dataclass
class InputContext:
    """Context provided to `IoManager.read` when retrieving upstream output.

    Attributes
    ----------
    expected_type : type | None
        The type annotation of the downstream task's parameter, if available.
        IoManagers can use this to return the appropriate type (e.g.
        ``polars.LazyFrame`` vs ``polars.DataFrame``).
    logical_date : datetime | None
        The logical date of the current run.  IoManagers use this to
        scope reads to the correct partition.
    all_partitions : bool
        When True, the IoManager should read **all** partitions instead
        of filtering to the current ``logical_date``.  Set when the
        upstream dependency is wrapped with `all_partitions()` or
        when the consuming task uses ``@task(all_partitions=True)``.
    partition_filter : dict[str, list[str]] | None
        Mapping of partition column names to their written values,
        pushed by the producing task via task values.  When set, the
        IoManager uses these values to filter the read result.  Populated
        automatically when ``auto_filter=True`` on the producing
        IoManager.
    """

    job_name: str
    task_key: str
    upstream_task_key: str
    run_id: str
    expected_type: type | None = field(default=None, repr=False)
    logical_date: datetime | None = None
    all_partitions: bool = False
    partition_by: list[str] | None = None
    partition_filter: dict[str, list[str]] | None = None


class IoManager(ABC):
    """Base class for managing data transfer between tasks.

    Each ``@task`` can optionally declare an ``IoManager`` that controls how
    its return value is persisted and how downstream tasks read that data.

    Lifecycle
    ---------
    IoManager instances are created at **import time** during both deploy
    and runtime phases.  ``__init__`` must therefore be safe to run locally
    without a Databricks runtime — do **not** import modules like
    ``pyspark.dbutils`` or establish cluster-only connections there.

    Instead, override `setup` for any initialisation that requires a
    Databricks runtime environment.  The framework calls ``setup()``
    exactly once per instance, at **runtime only**, before the first
    `read` or `write` invocation.

    Example
    -------
    ::

        import polars as pl
        from databricks_bundle_decorators import IoManager, OutputContext, InputContext

        class DeltaIoManager(IoManager):
            def __init__(self, catalog: str, schema: str):
                self.catalog = catalog
                self.schema = schema

            def setup(self) -> None:
                # Safe here — only called at runtime on Databricks.
                from pyspark.dbutils import DBUtils          # noqa: F401
                self.dbutils = DBUtils(...)

            def write(self, context: OutputContext, obj: Any) -> None:
                table = f"{self.catalog}.{self.schema}.{context.task_key}"
                obj.write_delta(table, mode="overwrite")

            def read(self, context: InputContext) -> Any:
                table = f"{self.catalog}.{self.schema}.{context.upstream_task_key}"
                return pl.read_delta(table)
    """

    _is_setup: bool = False
    """Internal flag to ensure `setup` is called at most once."""

    auto_filter: bool = True
    """When True, partition values are pushed via task values on write
    and used to auto-filter reads.  Set to False to disable."""

    def setup(self) -> None:
        """Initialise runtime-only resources.

        Override this method to perform initialisation that requires a
        Databricks cluster environment (Spark sessions, DBUtils, secret
        scopes, etc.).  The framework guarantees this is called **once**
        before the first `read` or `write`, and **only at
        runtime** — never during ``databricks bundle deploy``.

        The default implementation does nothing.
        """

    def _ensure_setup(self) -> None:
        """Call `setup` if it has not been called yet."""
        if not self._is_setup:
            self.setup()
            self._is_setup = True

    def _extract_partition_values(self, context: OutputContext) -> dict[str, list[str]]:
        """Extract distinct partition column values from written data.

        Called by the runtime after `write` when ``auto_filter=True``
        and ``partition_by`` is set.  The returned dict is pushed as a
        task value so downstream tasks can filter reads automatically.

        Subclasses **must** implement this method to support
        ``auto_filter=True``.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement "
            f"_extract_partition_values(). "
            f"Set auto_filter=False to disable automatic partition "
            f"filtering."
        )

    @abstractmethod
    def write(self, context: OutputContext, obj: Any) -> None:
        """Persist the return value of a task."""
        ...

    @abstractmethod
    def read(self, context: InputContext) -> Any:
        """Read the output of an upstream task for use downstream."""
        ...
