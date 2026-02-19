"""IoManager abstraction for inter-task data persistence.

Follows the Dagster IoManager pattern: large data (DataFrames, datasets)
is written to *permanent storage* (Delta tables, Unity Catalog volumes,
cloud object stores) rather than being squeezed through Databricks task
values.

Users implement concrete IoManagers and attach them to tasks via the
``io_manager`` parameter of the ``@task`` decorator.
"""

from __future__ import annotations

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

        class DeltaIoManager(IoManager):
            def __init__(self, catalog: str, schema: str):
                self.catalog = catalog
                self.schema = schema

            def store(self, context: OutputContext, obj: Any) -> None:
                table = f"{self.catalog}.{self.schema}.{context.task_key}"
                obj.write_delta(table, mode="overwrite")

            def load(self, context: InputContext) -> Any:
                table = f"{self.catalog}.{self.schema}.{context.upstream_task_key}"
                return pl.read_delta(table)
    """

    @abstractmethod
    def store(self, context: OutputContext, obj: Any) -> None:
        """Persist the return value of a task."""
        ...

    @abstractmethod
    def load(self, context: InputContext) -> Any:
        """Load the output of an upstream task for use downstream."""
        ...
