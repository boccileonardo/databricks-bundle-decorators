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
from typing import Any


@dataclass
class OutputContext:
    """Context provided to `IoManager.write` when persisting a task's return value."""

    job_name: str
    task_key: str
    run_id: str


@dataclass
class InputContext:
    """Context provided to `IoManager.read` when retrieving upstream output.

    Attributes
    ----------
    expected_type : type | None
        The type annotation of the downstream task's parameter, if available.
        IoManagers can use this to return the appropriate type (e.g.
        ``polars.LazyFrame`` vs ``polars.DataFrame``).
    """

    job_name: str
    task_key: str
    upstream_task_key: str
    run_id: str
    expected_type: type | None = field(default=None, repr=False)


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

    @abstractmethod
    def write(self, context: OutputContext, obj: Any) -> None:
        """Persist the return value of a task."""
        ...

    @abstractmethod
    def read(self, context: InputContext) -> Any:
        """Read the output of an upstream task for use downstream."""
        ...
