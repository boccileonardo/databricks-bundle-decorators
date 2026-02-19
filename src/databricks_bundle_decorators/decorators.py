"""Decorators for defining Databricks tasks, clusters, and jobs.

These decorators register metadata into global registries at import time.
At deploy time the codegen module reads the registries to produce
``databricks.bundles.jobs`` resources.  At runtime the same registries
are used to dispatch task execution.

DAG construction uses the **TaskFlow** pattern: inside a ``@job`` body,
``@task``-decorated functions are called normally.  Each call returns a
lightweight :class:`TaskProxy` that records the dependency edge when
passed as an argument to another task call.  No AST parsing is needed.
"""

from __future__ import annotations

import functools
import inspect
import types
from typing import Any, Callable, Unpack, overload

from databricks_bundle_decorators.io_manager import IoManager
from databricks_bundle_decorators.registry import (
    ClusterMeta,
    DuplicateResourceError,
    JobMeta,
    TaskMeta,
    _CLUSTER_REGISTRY,
    _JOB_REGISTRY,
    _TASK_REGISTRY,
    _register_unique,
)
from databricks_bundle_decorators.sdk_types import ClusterConfig, JobConfig, TaskConfig


# ---------------------------------------------------------------------------
# Job context – tracks which @job body is currently being executed
# ---------------------------------------------------------------------------

_current_job_name: str | None = None
"""Set while a ``@job`` body is executing so ``@task`` calls can record
themselves into the DAG automatically."""


# ---------------------------------------------------------------------------
# TaskProxy – returned by @task calls inside a @job body
# ---------------------------------------------------------------------------


class TaskProxy:
    """Lightweight proxy returned by ``@task`` calls inside a ``@job`` body.

    It carries the *task_key* so that when this proxy is passed as an
    argument to another task, the downstream task can record the
    dependency edge.
    """

    __slots__ = ("task_key",)

    def __init__(self, task_key: str) -> None:
        self.task_key = task_key

    def __repr__(self) -> str:
        return f"TaskProxy({self.task_key!r})"


# ---------------------------------------------------------------------------
# @task
# ---------------------------------------------------------------------------

_TaskDecorator = Callable[[types.FunctionType], Callable[..., Any]]


@overload
def task(
    *,
    io_manager: IoManager | None = ...,
    **kwargs: Unpack[TaskConfig],
) -> _TaskDecorator: ...


@overload
def task(fn: types.FunctionType, /) -> Callable[..., Any]: ...


def task(
    fn: types.FunctionType | None = None,
    *,
    io_manager: IoManager | None = None,
    **kwargs: Unpack[TaskConfig],
):
    """Register a function as a Databricks task.

    When used **inside** a ``@job`` body, the decorated function is
    registered under a qualified key (``job_name.task_name``) and
    calling it returns a :class:`TaskProxy` that wires up the DAG.

    When used **outside** a ``@job`` body (e.g. at module level), the
    function is registered under its short name for use in tests or
    standalone execution.

    Parameters
    ----------
    io_manager:
        An :class:`~databricks_bundle_decorators.io_manager.IoManager` instance that controls
        how the task's return value is persisted and loaded by downstream
        tasks.  When ``None``, no automatic data transfer takes place (use
        :func:`~databricks_bundle_decorators.task_values.set_task_value` for small scalars).
    **kwargs:
        Any additional SDK-native ``Task`` fields (e.g. ``max_retries``,
        ``timeout_seconds``, ``retry_on_timeout``).  These are forwarded
        directly to the ``databricks.bundles.jobs.Task`` constructor at
        deploy time.  See :class:`~databricks_bundle_decorators.sdk_types.TaskConfig`
        for the full list of supported fields.
    """

    def decorator(fn: types.FunctionType) -> Callable[..., Any]:
        task_key = fn.__name__
        meta = TaskMeta(
            fn=fn, task_key=task_key, io_manager=io_manager, sdk_config=dict(kwargs)
        )

        if _current_job_name is not None:
            # Inside a @job body – register under qualified key and
            # store in the job-local tracker so the wrapper can build
            # the DAG.
            qualified_key = f"{_current_job_name}.{task_key}"
            _register_unique(_TASK_REGISTRY, qualified_key, meta, "task")
            # Also stash in a job-scoped dict so @job can iterate.
            _current_job_tasks[task_key] = meta
        else:
            # Module-level definition (standalone / test usage).
            _TASK_REGISTRY[task_key] = meta

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if _current_job_name is not None:
                # We're being *called* inside a @job body – return a
                # TaskProxy and record DAG edges from any proxy args.
                upstream_deps: list[str] = []
                edge_map: dict[str, str] = {}

                param_names = list(inspect.signature(fn).parameters.keys())

                for idx, arg in enumerate(args):
                    if isinstance(arg, TaskProxy):
                        upstream_deps.append(arg.task_key)
                        p_name = (
                            param_names[idx] if idx < len(param_names) else f"arg{idx}"
                        )
                        edge_map[p_name] = arg.task_key

                for kw_name, kw_val in kwargs.items():
                    if isinstance(kw_val, TaskProxy):
                        upstream_deps.append(kw_val.task_key)
                        edge_map[kw_name] = kw_val.task_key

                _current_job_dag[task_key] = upstream_deps
                _current_job_edges[task_key] = edge_map

                return TaskProxy(task_key)
            else:
                # Normal execution (runtime / tests).
                return fn(*args, **kwargs)

        wrapper._task_meta = meta  # type: ignore[attr-defined]
        return wrapper

    if fn is not None:
        return decorator(fn)
    return decorator


# ---------------------------------------------------------------------------
# job_cluster
# ---------------------------------------------------------------------------


def job_cluster(
    name: str,
    **kwargs: Unpack[ClusterConfig],
) -> str:
    """Register a reusable job-cluster configuration.

    Cluster spec fields (``spark_version``, ``node_type_id``,
    ``num_workers``, etc.) are passed as keyword arguments and forwarded
    directly to the ``databricks.bundles.jobs.ClusterSpec`` constructor
    at deploy time.  The cluster is ephemeral: created when the job
    starts and torn down when it finishes.

    Returns the cluster *name* so it can be passed straight to
    ``@job(cluster=…)``.

    Parameters
    ----------
    name:
        Logical name used to reference this cluster from
        ``@job(cluster=…)``.
    **kwargs:
        Any SDK-native ``ClusterSpec`` fields (e.g. ``spark_version``,
        ``node_type_id``, ``num_workers``).  See
        :class:`~databricks_bundle_decorators.sdk_types.ClusterConfig` for the
        full list of supported fields.
    """
    meta = ClusterMeta(name=name, spec=dict(kwargs))
    _register_unique(_CLUSTER_REGISTRY, name, meta, "job_cluster")
    return name


# ---------------------------------------------------------------------------
# Job-level DAG tracking (populated during @job body execution)
# ---------------------------------------------------------------------------

_current_job_tasks: dict[str, TaskMeta] = {}
_current_job_dag: dict[str, list[str]] = {}
_current_job_edges: dict[str, dict[str, str]] = {}


# ---------------------------------------------------------------------------
# @job
# ---------------------------------------------------------------------------

_JobDecorator = Callable[[types.FunctionType], Callable[..., Any]]


@overload
def job(
    *,
    params: dict[str, str] | None = ...,
    cluster: str | None = ...,
    **kwargs: Unpack[JobConfig],
) -> _JobDecorator: ...


@overload
def job(fn: types.FunctionType, /) -> Callable[..., Any]: ...


def job(
    fn: types.FunctionType | None = None,
    *,
    params: dict[str, str] | None = None,
    cluster: str | None = None,
    **kwargs: Unpack[JobConfig],
):
    """Register a function as a Databricks job.

    The function body is **executed once at decoration time** (not at
    Databricks runtime).  Inside the body, ``@task``-decorated functions
    are defined and called.  Each call returns a :class:`TaskProxy`;
    passing a proxy to another task call records the dependency edge.

    Parameters
    ----------
    params:
        Default values for job-level parameters.  Accessible inside task
        functions via ``from databricks_bundle_decorators import params``.
    cluster:
        Name of a ``@job_cluster``-decorated configuration to use as the
        shared job cluster for all tasks.
    **kwargs:
        Any SDK-native ``Job`` fields (e.g. ``tags``, ``schedule``,
        ``max_concurrent_runs``, ``timeout_seconds``,
        ``email_notifications``).  These are forwarded directly to the
        ``databricks.bundles.jobs.Job`` constructor at deploy time.
        See :class:`~databricks_bundle_decorators.sdk_types.JobConfig` for the
        full list of supported fields.
    """

    def decorator(fn: types.FunctionType) -> Callable[..., Any]:
        global _current_job_name
        job_name = fn.__name__

        # --- check job uniqueness -----------------------------------------
        if job_name in _JOB_REGISTRY:
            raise DuplicateResourceError(
                f"Duplicate job '{job_name}'. Each job must have a unique name."
            )

        # --- execute the body to collect tasks and build the DAG ----------
        _current_job_tasks.clear()
        _current_job_dag.clear()
        _current_job_edges.clear()
        _current_job_name = job_name

        try:
            fn()
        finally:
            _current_job_name = None

        dag = dict(_current_job_dag)
        dag_edges = dict(_current_job_edges)

        # Ensure tasks that were defined but never called (no outgoing
        # edges recorded yet) still appear in the DAG with empty deps.
        for tk in _current_job_tasks:
            dag.setdefault(tk, [])

        meta = JobMeta(
            fn=fn,
            name=job_name,
            params=params or {},
            cluster=cluster,
            dag=dag,
            dag_edges=dag_edges,
            sdk_config=dict(kwargs),
        )
        _JOB_REGISTRY[job_name] = meta

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        wrapper._job_meta = meta  # type: ignore[attr-defined]
        return wrapper

    if fn is not None:
        return decorator(fn)
    return decorator
