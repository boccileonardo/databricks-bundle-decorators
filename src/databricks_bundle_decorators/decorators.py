"""Decorators for defining Databricks tasks, clusters, and jobs.

These decorators register metadata into global registries at import time.
At deploy time the codegen module reads the registries to produce
``databricks.bundles.jobs`` resources.  At runtime the same registries
are used to dispatch task execution.

DAG construction uses the **TaskFlow** pattern: inside a ``@job`` body,
``@task``-decorated functions are called normally.  Each call returns a
lightweight `TaskProxy` that records the dependency edge when
passed as an argument to another task call.  No AST parsing is needed.
"""

import functools
import inspect
import json
import types
import warnings
from collections.abc import Callable
from typing import Any, Unpack, overload

from databricks_bundle_decorators.backfill import BACKFILL_KEY_PARAM, BackfillDef
from databricks_bundle_decorators.io_manager import IoManager, _normalize_partition_by
from databricks_bundle_decorators.registry import (
    _CLUSTER_REGISTRY,
    _JOB_REGISTRY,
    _TASK_REGISTRY,
    ClusterMeta,
    DuplicateResourceError,
    ForEachMeta,
    JobMeta,
    TaskMeta,
    TaskValueRef,
    _register_unique,
)
from databricks_bundle_decorators.sdk_types import ClusterConfig, JobConfig, TaskConfig

# ---------------------------------------------------------------------------
# Reserved parameter namespace
# ---------------------------------------------------------------------------

#: Parameter names that are reserved for internal runtime wiring.
_RESERVED_PARAM_NAMES: frozenset[str] = frozenset(
    {
        "__job_name__",
        "__task_key__",
        "__run_id__",
        "__for_each_input__",
    }
)

#: Parameter name prefixes that are reserved for internal runtime wiring.
_RESERVED_PARAM_PREFIXES: tuple[str, ...] = ("__upstream__", "__all_partitions__")


def _validate_user_params(params: dict[str, str], context: str) -> None:
    """Raise ``ValueError`` if any *params* key collides with reserved names."""
    for name in params:
        if name in _RESERVED_PARAM_NAMES or any(
            name.startswith(p) for p in _RESERVED_PARAM_PREFIXES
        ):
            raise ValueError(
                f"{context}: parameter name {name!r} is reserved for internal "
                f"runtime use. Reserved names: {sorted(_RESERVED_PARAM_NAMES)}; "
                f"reserved prefix: {list(_RESERVED_PARAM_PREFIXES)}."
            )


# ---------------------------------------------------------------------------
# Job context - tracks which @job body is currently being executed
# ---------------------------------------------------------------------------

_current_job_name: str | None = None
"""Set while a ``@job`` body is executing so ``@task`` calls can record
themselves into the DAG automatically."""


# ---------------------------------------------------------------------------
# TaskProxy - returned by @task calls inside a @job body
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


class _AllPartitionsProxy:
    """Marker wrapper around a `TaskProxy` for all-partitions reads.

    Created by `all_partitions()`.  When passed as a task argument the
    framework records that the corresponding edge should instruct the
    IoManager to read **all** partitions instead of the current one.
    """

    __slots__ = ("task_key",)

    def __init__(self, task_key: str) -> None:
        self.task_key = task_key

    def __repr__(self) -> str:
        return f"_AllPartitionsProxy({self.task_key!r})"


def all_partitions(proxy: TaskProxy) -> _AllPartitionsProxy:
    """Wrap a `TaskProxy` so the downstream task receives all partitions.

    Use inside a ``@job`` body to indicate that the downstream task
    should read the **entire** dataset from the upstream task, across
    all partitions, rather than filtering to the current ``backfill_key``.

    Parameters
    ----------
    proxy:
        A `TaskProxy` returned by calling a ``@task``-decorated
        function inside a ``@job`` body.

    Returns
    -------
    `_AllPartitionsProxy`
        A wrapped proxy that records the all-partitions flag on the
        dependency edge.

    Example
    -------
    ::

        @job(backfill=DailyBackfill(start_date="2024-01-01"))
        def my_pipeline():
            @task(io_manager=io)
            def extract(): ...

            @task
            def aggregate(data): ...

            data = extract()
            aggregate(all_partitions(data))
    """
    if not isinstance(proxy, TaskProxy):
        raise TypeError(
            f"all_partitions() expects a TaskProxy returned by calling "
            f"a @task-decorated function inside a @job body, "
            f"got {type(proxy).__name__!r}."
        )
    return _AllPartitionsProxy(proxy.task_key)


# ---------------------------------------------------------------------------
# @task
# ---------------------------------------------------------------------------

_TaskDecorator = Callable[[types.FunctionType], Callable[..., Any]]


@overload
def task(
    *,
    io_manager: IoManager | None = ...,
    depends_on: TaskProxy | list[TaskProxy] | None = ...,
    all_partitions: bool = ...,
    partition_by: str | list[str] | None = ...,
    **kwargs: Unpack[TaskConfig],
) -> _TaskDecorator: ...


@overload
def task(fn: types.FunctionType, /) -> Callable[..., Any]: ...


def task(
    fn: types.FunctionType | None = None,
    *,
    io_manager: IoManager | None = None,
    depends_on: TaskProxy | list[TaskProxy] | None = None,
    all_partitions: bool = False,
    partition_by: str | list[str] | None = None,
    **kwargs: Unpack[TaskConfig],
):
    """Register a function as a Databricks task.

    When used **inside** a ``@job`` body, the decorated function is
    registered under a qualified key (``job_name.task_name``) and
    calling it returns a `TaskProxy` that wires up the DAG.

    When used **outside** a ``@job`` body (e.g. at module level), the
    function is registered under its short name for use in tests or
    standalone execution.  Duplicate names at module level raise
    `DuplicateResourceError`.

    Parameters
    ----------
    io_manager:
        An `IoManager` instance that controls
        how the task's return value is persisted and loaded by downstream
        tasks.  When ``None``, no automatic data transfer takes place (use
        `set_task_value` for small scalars).
    depends_on:
        One or more `TaskProxy` objects returned by calling other
        ``@task``-decorated functions inside a ``@job`` body.  Creates
        **control-flow-only** dependencies: the current task will run
        after the specified upstream tasks complete, but no data is
        transferred via `IoManager`.  Use this when a task must wait
        for another to finish without consuming its output.  For
        data dependencies, pass `TaskProxy` objects as regular function
        arguments instead.
    all_partitions:
        When ``True``, **all** upstream data dependencies read the
        entire dataset (all partitions) instead of filtering to the
        current ``backfill_key``.  For fine-grained control, use the
        `all_partitions` function to wrap individual `TaskProxy`
        arguments instead.
    **kwargs:
        Any additional SDK-native ``Task`` fields (e.g. ``max_retries``,
        ``timeout_seconds``, ``retry_on_timeout``).  These are forwarded
        directly to the ``databricks.bundles.jobs.Task`` constructor at
        deploy time.  See `TaskConfig`
        for the full list of supported fields.
    Notes
    -----
    Dependency edges are detected only for `TaskProxy` objects passed as
    **direct** positional or keyword arguments.  Proxies nested inside
    lists, dicts, or other container types are **not** inspected and will
    not register dependency edges."""

    def decorator(fn: types.FunctionType) -> Callable[..., Any]:
        task_key = fn.__name__

        # Normalize and validate depends_on
        depends_on_keys: list[str] = []
        if depends_on is not None:
            deps = depends_on if isinstance(depends_on, list) else [depends_on]
            for dep in deps:
                if not isinstance(dep, TaskProxy):
                    raise TypeError(
                        f"@task(depends_on=...) expects TaskProxy objects "
                        f"returned by calling @task-decorated functions "
                        f"inside a @job body, got {type(dep).__name__!r}."
                    )
                depends_on_keys.append(dep.task_key)

        meta = TaskMeta(
            fn=fn,
            task_key=task_key,
            io_manager=io_manager,
            partition_by=_normalize_partition_by(partition_by),
            sdk_config={**kwargs},
            depends_on=depends_on_keys,
        )

        if _current_job_name is not None:
            # Inside a @job body - register under qualified key and
            # store in the job-local tracker so the wrapper can build
            # the DAG.
            qualified_key = f"{_current_job_name}.{task_key}"
            _register_unique(_TASK_REGISTRY, qualified_key, meta, "task")
            # Also stash in a job-scoped dict so @job can iterate.
            _current_job_tasks[task_key] = meta
        else:
            # Module-level definition (standalone / test usage).
            _register_unique(_TASK_REGISTRY, task_key, meta, "task")

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if _current_job_name is not None:
                # We're being *called* inside a @job body - return a
                # TaskProxy and record DAG edges from any proxy args.
                if task_key in _current_job_dag:
                    raise DuplicateResourceError(
                        f"Task '{task_key}' is called more than once in job "
                        f"'{_current_job_name}'. Each @task may only be invoked "
                        "once per @job body. Use a unique function name for "
                        "each logical step."
                    )

                upstream_deps: list[str] = list(meta.depends_on)
                edge_map: dict[str, str] = {}
                ap_params: set[str] = set()

                param_names = list(inspect.signature(fn).parameters.keys())

                for idx, arg in enumerate(args):
                    if isinstance(arg, (_AllPartitionsProxy, TaskProxy)):
                        upstream_deps.append(arg.task_key)
                        p_name = (
                            param_names[idx] if idx < len(param_names) else f"arg{idx}"
                        )
                        edge_map[p_name] = arg.task_key
                        if isinstance(arg, _AllPartitionsProxy) or all_partitions:
                            ap_params.add(p_name)
                    elif arg is not None:
                        p_name = (
                            param_names[idx] if idx < len(param_names) else f"arg{idx}"
                        )
                        warnings.warn(
                            f"Task '{task_key}' in job '{_current_job_name}' "
                            f"received a non-TaskProxy argument "
                            f"({type(arg).__name__!r}) for parameter "
                            f"'{p_name}'. Inside a @job body, task calls "
                            f"only build the DAG — arguments that are not "
                            f"TaskProxy values returned by other @task "
                            f"calls are silently discarded at runtime. "
                            f"Move data-producing code inside a @task "
                            f"function.",
                            UserWarning,
                            stacklevel=2,
                        )

                for kw_name, kw_val in kwargs.items():
                    if isinstance(kw_val, (_AllPartitionsProxy, TaskProxy)):
                        upstream_deps.append(kw_val.task_key)
                        edge_map[kw_name] = kw_val.task_key
                        if isinstance(kw_val, _AllPartitionsProxy) or all_partitions:
                            ap_params.add(kw_name)
                    elif kw_val is not None:
                        warnings.warn(
                            f"Task '{task_key}' in job '{_current_job_name}' "
                            f"received a non-TaskProxy argument "
                            f"({type(kw_val).__name__!r}) for parameter "
                            f"'{kw_name}'. Inside a @job body, task calls "
                            f"only build the DAG — arguments that are not "
                            f"TaskProxy values returned by other @task "
                            f"calls are silently discarded at runtime. "
                            f"Move data-producing code inside a @task "
                            f"function.",
                            UserWarning,
                            stacklevel=2,
                        )

                # Deduplicate while preserving order
                upstream_deps = list(dict.fromkeys(upstream_deps))
                _current_job_dag[task_key] = upstream_deps
                _current_job_edges[task_key] = edge_map
                if ap_params:
                    _current_job_all_partitions[task_key] = ap_params

                return TaskProxy(task_key)
            # Normal execution (runtime / tests).
            return fn(*args, **kwargs)

        wrapper._task_meta = meta  # ty: ignore[unresolved-attribute]
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
) -> ClusterMeta:
    """Register a reusable job-cluster configuration.

    Cluster spec fields (``spark_version``, ``node_type_id``,
    ``num_workers``, etc.) are passed as keyword arguments and forwarded
    directly to the ``databricks.bundles.jobs.ClusterSpec`` constructor
    at deploy time.  The cluster is ephemeral: created when the job
    starts and torn down when it finishes.

    Returns a `ClusterMeta` object that should be passed directly
    to ``@job(cluster=…)``.

    Parameters
    ----------
    name:
        Logical name for this cluster configuration.
    **kwargs:
        Any SDK-native ``ClusterSpec`` fields (e.g. ``spark_version``,
        ``node_type_id``, ``num_workers``).  See
        `ClusterConfig` for the
        full list of supported fields.
    """
    meta = ClusterMeta(name=name, spec={**kwargs})
    _register_unique(_CLUSTER_REGISTRY, name, meta, "job_cluster")
    return meta


# ---------------------------------------------------------------------------
# Job-level DAG tracking (populated during @job body execution)
# ---------------------------------------------------------------------------

_current_job_tasks: dict[str, TaskMeta] = {}
_current_job_dag: dict[str, list[str]] = {}
_current_job_edges: dict[str, dict[str, str]] = {}
_current_job_for_each: dict[str, ForEachMeta] = {}
_current_job_all_partitions: dict[str, set[str]] = {}


# ---------------------------------------------------------------------------
# @job
# ---------------------------------------------------------------------------

_JobDecorator = Callable[[types.FunctionType], Callable[..., Any]]


@overload
def job(
    *,
    params: dict[str, str] | None = ...,
    cluster: ClusterMeta | None = ...,
    libraries: list | None = ...,
    backfill: BackfillDef | None = ...,
    **kwargs: Unpack[JobConfig],
) -> _JobDecorator: ...


@overload
def job(fn: types.FunctionType, /) -> Callable[..., Any]: ...


def job(
    fn: types.FunctionType | None = None,
    *,
    params: dict[str, str] | None = None,
    cluster: ClusterMeta | None = None,
    libraries: list | None = None,
    backfill: BackfillDef | None = None,
    **kwargs: Unpack[JobConfig],
):
    """Register a function as a Databricks job.

    The function body is **executed once at deploy time** (when
    ``databricks bundle deploy`` imports your module — not at
    Databricks runtime).  Inside the body, ``@task``-decorated functions
    are defined and called.  Each call returns a `TaskProxy`;
    passing a proxy to another task call records the dependency edge.

    Parameters
    ----------
    params:
        Default values for job-level parameters.  Accessible inside task
        functions via ``from databricks_bundle_decorators import params``.
    cluster:
        A `ClusterMeta` returned by `job_cluster()` to use
        as the shared job cluster for all tasks.
    backfill:
        A `BackfillDef` that declares the universe of valid
        ``backfill_key`` values for this job.  The ``dbxdec backfill``
        CLI command uses this to enumerate keys when submitting bulk
        runs.  Has no effect on runtime behaviour.
    libraries:
        Library dependencies to attach to each task.  When ``None``
        (the default), the framework uses ``[Library(whl="dist/*.whl")]``
        which is appropriate for standard wheel-based deployments.
        Set to ``[]`` when the package is pre-installed in a custom
        Docker image.  You may also pass explicit ``Library`` objects
        for PyPI or Maven dependencies.
    **kwargs:
        Any SDK-native ``Job`` fields (e.g. ``tags``, ``schedule``,
        ``max_concurrent_runs``, ``timeout_seconds``,
        ``email_notifications``).  These are forwarded directly to the
        ``databricks.bundles.jobs.Job`` constructor at deploy time.
        See `JobConfig` for the
        full list of supported fields.
    """

    def decorator(fn: types.FunctionType) -> Callable[..., Any]:
        global _current_job_name  # noqa: PLW0603
        job_name = fn.__name__

        # --- check job uniqueness -----------------------------------------
        if job_name in _JOB_REGISTRY:
            raise DuplicateResourceError(
                f"Duplicate job '{job_name}'. Each job must have a unique name."
            )

        # --- validate param names -----------------------------------------
        if params:
            _validate_user_params(params, f"@job('{job_name}')")

        # --- validate cluster type -----------------------------------------
        if cluster is not None and not isinstance(cluster, ClusterMeta):
            raise TypeError(
                f"@job(cluster=...) expects a ClusterMeta returned by "
                f"job_cluster(), got {type(cluster).__name__!r}. "
                f"Pass the job_cluster() return value directly instead "
                f"of a string."
            )

        # --- validate and wire backfill -----------------------------------
        if backfill is not None and not isinstance(backfill, BackfillDef):
            raise TypeError(
                f"@job(backfill=...) expects a BackfillDef instance "
                f"(e.g. DailyBackfill, StaticBackfill), "
                f"got {type(backfill).__name__!r}."
            )

        # Auto-inject the backfill_key parameter when backfill is set
        effective_params: dict[str, str] = dict(params) if params else {}
        if backfill is not None:
            effective_params.setdefault(BACKFILL_KEY_PARAM, "")

        # --- execute the body to collect tasks and build the DAG ----------
        _current_job_tasks.clear()
        _current_job_dag.clear()
        _current_job_edges.clear()
        _current_job_for_each.clear()
        _current_job_all_partitions.clear()
        _current_job_name = job_name

        try:
            fn()
        finally:
            _current_job_name = None

        dag = dict(_current_job_dag)
        dag_edges = dict(_current_job_edges)
        for_each_tasks = dict(_current_job_for_each)
        all_partitions_edges = dict(_current_job_all_partitions)

        # Ensure tasks that were defined but never called (no outgoing
        # edges recorded yet) still appear in the DAG.  If the task has
        # depends_on control-flow deps, include them even when uncalled.
        for tk, t_meta in _current_job_tasks.items():
            dag.setdefault(tk, list(t_meta.depends_on))

        meta = JobMeta(
            fn=fn,
            name=job_name,
            params=effective_params,
            cluster=cluster,
            libraries=libraries,
            dag=dag,
            dag_edges=dag_edges,
            all_partitions_edges=all_partitions_edges,
            sdk_config={**kwargs},
            for_each_tasks=for_each_tasks,
            backfill=backfill,
        )
        _JOB_REGISTRY[job_name] = meta

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        wrapper._job_meta = meta  # ty: ignore[unresolved-attribute]
        return wrapper

    if fn is not None:
        return decorator(fn)
    return decorator


# ---------------------------------------------------------------------------
# @for_each_task
# ---------------------------------------------------------------------------


_TaskRef = Callable[..., Any] | TaskProxy
"""A reference to an upstream task: either the decorated function itself or
a `TaskProxy` returned by calling it inside a ``@job`` body."""


def task_value(task_ref: _TaskRef, key: str) -> TaskValueRef:
    """Create a reference to a specific task-value from an upstream task.

    Use this with ``@for_each_task(inputs=...)`` to specify which
    upstream task-value provides the iteration list.

    Parameters
    ----------
    task_ref:
        A ``@task``-decorated function or a `TaskProxy` returned by
        calling one inside a ``@job`` body.
    key:
        The task-value key name — the ``key`` argument passed to
        `set_task_value` in the upstream task.

    Returns
    -------
    `TaskValueRef`
        An object that can be passed to ``@for_each_task(inputs=...)``.

    Examples
    --------
    ::

        @job
        def my_pipeline():
            @task
            def discover():
                set_task_value("countries", ["US", "UK", "DE"])

            @for_each_task(inputs=task_value(discover, "countries"))
            def process(inputs: str):
                print(f"Processing {inputs}")
    """
    resolved_key = _resolve_task_ref(task_ref, "task_value()")
    return TaskValueRef(task_key=resolved_key, key=key)


def for_each_task(
    *,
    inputs: TaskValueRef | list[Any],
    concurrency: int | None = None,
    io_manager: IoManager | None = None,
    depends_on: _TaskRef | list[_TaskRef] | None = None,
    **kwargs: Unpack[TaskConfig],
) -> _TaskDecorator:
    """Register a function as a Databricks **for-each** task.

    A for-each task iterates over a list of inputs and executes the
    decorated function once per element.  The iteration list is
    specified via the ``inputs`` decorator argument — either a
    `TaskValueRef` created by `task_value` (referencing a specific
    upstream task-value) or a static Python list.

    The decorated function **must** have a parameter named ``inputs``.
    At runtime the framework injects the current element from the
    iteration list into that parameter.

    Inside a ``@job`` body the function must be **called** to add it
    to the DAG — just like ``@task``.  Call arguments wire `IoManager`
    data dependencies.

    Parameters
    ----------
    inputs:
        The iteration source.  Use ``task_value(upstream_task, "key")``
        to iterate over a task-value published by an upstream task via
        `set_task_value`.  Pass a plain Python list (must be
        JSON-serialisable) for static iteration.
    concurrency:
        Maximum number of parallel iterations.  Maps to the
        ``ForEachTask.concurrency`` field in the Databricks SDK.
    io_manager:
        An `IoManager` instance for persisting the task's return value,
        identical in behaviour to ``@task(io_manager=...)``.
    depends_on:
        Control-flow-only dependencies, identical to
        ``@task(depends_on=...)``.  Accepts ``@task``-decorated
        functions or `TaskProxy` objects.
    **kwargs:
        SDK-native ``Task`` fields forwarded to the **inner** task
        (e.g. ``max_retries``, ``timeout_seconds``).  See `TaskConfig`.

    Examples
    --------
    Dynamic inputs from an upstream task with an IoManager data dependency::

        @job
        def my_pipeline():
            @task
            def get_files():
                set_task_value("files", ["a.csv", "b.csv", "c.csv"])

            @task(io_manager=staging_io)
            def load_data():
                return pl.read_parquet("s3://bucket/data.parquet")

            data = load_data()

            @for_each_task(inputs=task_value(get_files, "files"), concurrency=5)
            def process(inputs: str, data):
                subset = data.filter(pl.col("file") == inputs)
                print(f"Processing {inputs}: {len(subset)} rows")

            process(data=data)

    Static inputs::

        @job
        def static_pipeline():
            @for_each_task(inputs=["us-east-1", "eu-west-1"])
            def ingest(inputs: str):
                print(f"Ingesting {inputs}")

            ingest()
    """

    if _current_job_name is None:
        raise RuntimeError("@for_each_task can only be used inside a @job body.")

    # --- resolve inputs ---------------------------------------------------
    inputs_task_key: str | None = None
    inputs_value_key: str | None = None
    static_inputs: list[Any] | None = None
    inputs_dep_key: str | None = None

    if isinstance(inputs, list):
        try:
            json.dumps(inputs)
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"@for_each_task: static inputs must be "
                f"JSON-serialisable, got error: {exc}"
            ) from exc
        static_inputs = inputs
    elif isinstance(inputs, TaskValueRef):
        inputs_task_key = inputs.task_key
        inputs_value_key = inputs.key
        inputs_dep_key = inputs.task_key
    else:
        raise TypeError(
            f"@for_each_task(inputs=...) expects a TaskValueRef from "
            f"task_value() or a static list, got {type(inputs).__name__!r}. "
            f"Use task_value(upstream_task, 'key_name') to reference "
            f"a task-value from an upstream task."
        )

    # --- resolve depends_on -----------------------------------------------
    depends_on_keys: list[str] = []
    if depends_on is not None:
        deps = depends_on if isinstance(depends_on, list) else [depends_on]
        depends_on_keys.extend(
            _resolve_task_ref(dep, "@for_each_task(depends_on=...)") for dep in deps
        )

    # Merge inputs dep into depends_on list
    all_dep_keys = list(depends_on_keys)
    if inputs_dep_key is not None:
        all_dep_keys.append(inputs_dep_key)
    # Deduplicate while preserving order
    all_dep_keys = list(dict.fromkeys(all_dep_keys))

    def decorator(fn: types.FunctionType) -> Callable[..., Any]:
        task_key = fn.__name__

        # Validate that the function has an 'inputs' parameter
        sig = inspect.signature(fn)
        if "inputs" not in sig.parameters:
            raise ValueError(
                f"@for_each_task: function '{task_key}' must have a "
                f"parameter named 'inputs' to receive each element "
                f"from the iteration list. "
                f"Parameters: {list(sig.parameters.keys())}."
            )

        meta = TaskMeta(
            fn=fn,
            task_key=task_key,
            io_manager=io_manager,
            sdk_config={**kwargs},
            depends_on=all_dep_keys,
        )

        assert _current_job_name is not None  # guaranteed by outer check

        qualified_key = f"{_current_job_name}.{task_key}"
        _register_unique(_TASK_REGISTRY, qualified_key, meta, "task")
        _current_job_tasks[task_key] = meta

        # Record ForEachMeta immediately — no call required.
        _current_job_for_each[task_key] = ForEachMeta(
            inputs_task_key=inputs_task_key,
            inputs_value_key=inputs_value_key,
            static_inputs=static_inputs,
            concurrency=concurrency,
        )

        @functools.wraps(fn)
        def wrapper(*args, **call_kwargs):
            if _current_job_name is None:
                # Normal execution (runtime / tests) — call directly.
                return fn(*args, **call_kwargs)

            # Inside a @job body — wire data-dependency edges.
            if task_key in _current_job_dag:
                raise DuplicateResourceError(
                    f"Task '{task_key}' is called more than once in job "
                    f"'{_current_job_name}'. Each @task / @for_each_task "
                    "may only be invoked once per @job body."
                )

            # Map positional args to parameter names (skip 'inputs')
            param_names = [p for p in sig.parameters if p != "inputs"]
            all_call_kwargs: dict[str, Any] = {}
            for idx, arg in enumerate(args):
                p_name = param_names[idx] if idx < len(param_names) else f"arg{idx}"
                all_call_kwargs[p_name] = arg
            all_call_kwargs.update(call_kwargs)

            upstream_deps: list[str] = list(all_dep_keys)
            edge_map: dict[str, str] = {}

            # Process call args as data deps (same as @task)
            for kw_name, kw_val in all_call_kwargs.items():
                if isinstance(kw_val, TaskProxy):
                    upstream_deps.append(kw_val.task_key)
                    edge_map[kw_name] = kw_val.task_key
                elif kw_val is not None:
                    warnings.warn(
                        f"for_each_task '{task_key}' in job "
                        f"'{_current_job_name}' received a non-TaskProxy "
                        f"argument ({type(kw_val).__name__!r}) for "
                        f"parameter '{kw_name}'. Inside a @job body, "
                        f"task calls only build the DAG.",
                        UserWarning,
                        stacklevel=2,
                    )

            upstream_deps = list(dict.fromkeys(upstream_deps))
            _current_job_dag[task_key] = upstream_deps
            _current_job_edges[task_key] = edge_map

            return TaskProxy(task_key)

        wrapper._task_meta = meta  # ty: ignore[unresolved-attribute]
        return wrapper

    return decorator


def _resolve_task_ref(ref: Any, context: str) -> str:
    """Extract a task key from a function reference or `TaskProxy`.

    Parameters
    ----------
    ref:
        Either a ``@task``-decorated function (has ``_task_meta``) or a
        `TaskProxy`.
    context:
        Human-readable label for error messages.
    """
    if isinstance(ref, TaskProxy):
        return ref.task_key
    if callable(ref) and hasattr(ref, "_task_meta"):
        return ref._task_meta.task_key
    raise TypeError(
        f"{context} expects a @task-decorated function or a TaskProxy "
        f"returned by calling one, got {type(ref).__name__!r}."
    )
