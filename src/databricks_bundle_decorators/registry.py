"""Global registries for tasks, clusters, and jobs.

Decorators populate these registries at import time. The codegen module
reads them to produce databricks.bundles.jobs resources at deploy time,
and the runtime module reads them to dispatch task execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from databricks_bundle_decorators.io_manager import IoManager
    from databricks_bundle_decorators.backfill import BackfillDef


@dataclass
class TaskMeta:
    """Metadata recorded by the @task decorator."""

    fn: Callable
    task_key: str
    io_manager: IoManager | None = None
    partition_by: list[str] | None = None
    sdk_config: dict[str, Any] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)


@dataclass
class ClusterMeta:
    """Metadata recorded by job_cluster()."""

    name: str
    spec: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskValueRef:
    """Reference to a specific task-value from an upstream task.

    Created via the `task_value` helper and passed to
    ``@for_each_task(inputs=...)`` to specify which upstream task-value
    provides the iteration list.
    """

    task_key: str
    """The task key of the upstream task."""

    key: str
    """The task-value key name (the ``key`` argument to
    `set_task_value`)."""


@dataclass
class ForEachMeta:
    """Metadata for a for-each task wrapper.

    Recorded by ``@for_each_task`` inside a ``@job`` body.  The outer
    task iterates over *inputs* and executes the inner task once per
    element.
    """

    inputs_task_key: str | None = None
    """Upstream task whose task-value provides the iteration list.
    ``None`` when a static list is used."""

    inputs_value_key: str | None = None
    """The task-value key name on the upstream task (e.g. ``"countries"``).
    ``None`` when a static list is used."""

    static_inputs: list[Any] | None = None
    """A static JSON-serialisable list used when no upstream task supplies
    the inputs dynamically."""

    concurrency: int | None = None
    """Maximum parallel iterations (maps to ``ForEachTask.concurrency``)."""


@dataclass
class JobMeta:
    """Metadata recorded by the @job decorator."""

    fn: Callable
    name: str
    params: dict[str, str] = field(default_factory=dict)
    cluster: ClusterMeta | None = None
    libraries: list[Any] | None = None
    # task_key -> list of upstream task_keys
    dag: dict[str, list[str]] = field(default_factory=dict)
    # task_key -> {param_name: upstream_task_key}
    dag_edges: dict[str, dict[str, str]] = field(default_factory=dict)
    # task_key -> set of param names that read all partitions
    all_partitions_edges: dict[str, set[str]] = field(default_factory=dict)
    sdk_config: dict[str, Any] = field(default_factory=dict)
    # task_key -> ForEachMeta (for tasks that are for_each wrappers)
    for_each_tasks: dict[str, ForEachMeta] = field(default_factory=dict)
    backfill: BackfillDef | None = None
    """Backfill definition for key enumeration.  Does not affect
    runtime behaviour — ``backfill_key`` is always available."""


# ---------------------------------------------------------------------------
# Global registries – populated by decorators, consumed by codegen & runtime
# ---------------------------------------------------------------------------

# Tasks are keyed by qualified name ("job_name.task_name") when defined
# inside a @job body, or by short name when defined standalone.
_TASK_REGISTRY: dict[str, TaskMeta] = {}
_CLUSTER_REGISTRY: dict[str, ClusterMeta] = {}
_JOB_REGISTRY: dict[str, JobMeta] = {}


class DuplicateResourceError(Exception):
    """Raised when a resource with the same key is registered twice."""


def _register_unique(
    registry: dict[str, Any],
    key: str,
    value: Any,
    kind: str,
) -> None:
    """Insert *value* into *registry* under *key*, raising on duplicates."""
    if key in registry:
        raise DuplicateResourceError(
            f"Duplicate {kind} '{key}'. Each {kind} must have a unique name."
        )
    registry[key] = value


def reset_registries() -> None:
    """Reset all registries. Useful for testing."""
    _TASK_REGISTRY.clear()
    _CLUSTER_REGISTRY.clear()
    _JOB_REGISTRY.clear()
