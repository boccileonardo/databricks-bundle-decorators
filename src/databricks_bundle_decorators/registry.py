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


@dataclass
class TaskMeta:
    """Metadata recorded by the @task decorator."""

    fn: Callable
    task_key: str
    io_manager: IoManager | None = None
    sdk_config: dict[str, Any] = field(default_factory=dict)


@dataclass
class ClusterMeta:
    """Metadata recorded by job_cluster()."""

    name: str
    spec: dict[str, Any] = field(default_factory=dict)


@dataclass
class JobMeta:
    """Metadata recorded by the @job decorator."""

    fn: Callable
    name: str
    params: dict[str, str] = field(default_factory=dict)
    cluster: ClusterMeta | None = None
    # task_key -> list of upstream task_keys
    dag: dict[str, list[str]] = field(default_factory=dict)
    # task_key -> {param_name: upstream_task_key}
    dag_edges: dict[str, dict[str, str]] = field(default_factory=dict)
    sdk_config: dict[str, Any] = field(default_factory=dict)


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
