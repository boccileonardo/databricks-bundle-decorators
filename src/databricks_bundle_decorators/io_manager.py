"""IoManager abstraction for inter-task data persistence.

Follows the Dagster IoManager pattern: large data (DataFrames, datasets)
is written to *permanent storage* (Delta tables, Unity Catalog volumes,
cloud object stores) rather than being squeezed through Databricks task
values.

Users implement concrete IoManagers and attach them to tasks via the
``io_manager`` parameter of the ``@task`` decorator.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class OutputContext:
    """Context provided to :meth:`IoManager.store` when persisting a task's return value."""

    job_name: str
    task_key: str
    run_id: str


@dataclass
class InputContext:
    """Context provided to :meth:`IoManager.load` when retrieving upstream output."""

    job_name: str
    task_key: str
    upstream_task_key: str
    run_id: str


class IoManager(ABC):
    """Base class for managing data transfer between tasks.

    Each ``@task`` can optionally declare an ``IoManager`` that controls how
    its return value is stored and how downstream tasks load that data.

    Example
    -------
    ::

        import polars as pl
        from databricks_bundle_decorators import IoManager, OutputContext, InputContext

        class AdlsParquetIoManager(IoManager):
            def __init__(self, storage_account: str, container: str, base_path: str = "data"):
                self.root = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{base_path}"

            def store(self, context: OutputContext, obj: Any) -> None:
                obj.write_parquet(f"{self.root}/{context.task_key}.parquet")

            def load(self, context: InputContext) -> Any:
                return pl.read_parquet(f"{self.root}/{context.upstream_task_key}.parquet")
    """

    @abstractmethod
    def store(self, context: OutputContext, obj: Any) -> None:
        """Persist the return value of a task."""
        ...

    @abstractmethod
    def load(self, context: InputContext) -> Any:
        """Load the output of an upstream task for use downstream."""
        ...
