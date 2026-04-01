"""Convert registries into ``databricks.bundles.jobs`` resource objects.

Called at deploy time by the resource loader.  Reads the global
registries populated by ``@task``, ``@job_cluster``, ``@job``, and
``@for_each_task`` decorators and produces ``Job`` dataclass instances
that the Databricks CLI serialises into the bundle configuration.
"""

from __future__ import annotations

import json
from typing import Any

from databricks_bundle_decorators.backfill import BACKFILL_TAG, _serialize_backfill_tag
from databricks_bundle_decorators.registry import (
    _JOB_REGISTRY,
    _TASK_REGISTRY,
)


def generate_resources(
    package_name: str = "databricks_bundle_decorators",
    default_libraries: list[object] | None = None,
) -> dict:
    """Build ``{resource_key: Job}`` from the global registries.

    Parameters
    ----------
    package_name:
        The Python package name used in ``PythonWheelTask``.  Must match
        the ``[project.name]`` in *pyproject.toml*.
    default_libraries:
        Libraries attached to every task whose ``@job`` does *not* set
        ``libraries`` explicitly.  When ``None`` (the default), uses
        ``[Library(whl="dist/*.whl")]``.  Pass an empty list to suppress
        the default wheel library.
    """
    from databricks.bundles.jobs import (
        ClusterSpec,
        ForEachTask,
        Job,
        JobCluster,
        JobParameterDefinition,
        Library,
        PythonWheelTask,
        Task,
        TaskDependency,
    )

    _default_libraries: list[object] | None = (
        [Library(whl="dist/*.whl")] if default_libraries is None else default_libraries
    )

    jobs: dict[str, Job] = {}

    for job_name, job_meta in _JOB_REGISTRY.items():
        tasks: list[Task] = []

        for task_key, upstream_keys in job_meta.dag.items():
            depends_on = [TaskDependency(task_key=uk) for uk in upstream_keys]

            # ----- named_parameters sent to the wheel entry-point ----------
            named_params: dict[str, str] = {
                "__job_name__": job_name,
                "__task_key__": task_key,
                "__run_id__": "{{job.run_id}}",
            }

            # Upstream edge info so the runtime can invoke IoManager.read()
            edges = job_meta.dag_edges.get(task_key, {})
            for param_name, upstream_task in edges.items():
                named_params[f"__upstream__{param_name}"] = upstream_task

            # All-partitions flags for upstream reads
            ap_params = job_meta.all_partitions_edges.get(task_key, set())
            for param_name in ap_params:
                named_params[f"__all_partitions__{param_name}"] = "true"

            # Forward every job-level parameter to the task CLI
            for param_name in job_meta.params:
                named_params[param_name] = (
                    "{{" + f'job.parameters["{param_name}"]' + "}}"
                )

            # ----- per-task SDK config (max_retries, timeout, etc.) -----
            qualified_key = f"{job_name}.{task_key}"
            task_meta = _TASK_REGISTRY[qualified_key]
            task_sdk_config = task_meta.sdk_config

            # Use job-level libraries if explicitly set, else default wheel
            if job_meta.libraries is not None:
                task_libraries = (
                    job_meta.libraries if len(job_meta.libraries) > 0 else None
                )
            else:
                task_libraries = _default_libraries

            # ----- check if this is a for-each task -----------------------
            fe_meta = job_meta.for_each_tasks.get(task_key)

            if fe_meta is not None:
                # ---- for-each task: outer wrapper with inner task --------
                # Add __for_each_input__ so runtime knows to inject it
                named_params["__for_each_input__"] = "{{input}}"

                # Build the inner Task (the one that runs per iteration)
                inner_task_kwargs: dict[str, Any] = {
                    "task_key": f"{task_key}_inner",
                    "job_cluster_key": (
                        job_meta.cluster.name if job_meta.cluster else None
                    ),
                    "python_wheel_task": PythonWheelTask(
                        package_name=package_name,
                        entry_point="dbxdec-run",
                        named_parameters=named_params,  # type: ignore[arg-type]
                    ),
                    **task_sdk_config,
                }
                if task_libraries is not None:
                    inner_task_kwargs["libraries"] = task_libraries

                inner_task = Task(**inner_task_kwargs)

                # Determine the inputs expression
                if fe_meta.inputs_task_key is not None:
                    value_key = fe_meta.inputs_value_key
                    inputs_expr = (
                        "{{"
                        + f"tasks.{fe_meta.inputs_task_key}.values.{value_key}"
                        + "}}"
                    )
                else:
                    inputs_expr = json.dumps(fe_meta.static_inputs)

                for_each = ForEachTask(
                    inputs=inputs_expr,
                    task=inner_task,
                    concurrency=fe_meta.concurrency,
                )

                outer_task_obj = Task(
                    task_key=task_key,
                    depends_on=depends_on,
                    for_each_task=for_each,
                )
                tasks.append(outer_task_obj)
            else:
                # ---- regular task ----------------------------------------
                task_kwargs: dict[str, Any] = {
                    "task_key": task_key,
                    "depends_on": depends_on,
                    "job_cluster_key": (
                        job_meta.cluster.name if job_meta.cluster else None
                    ),
                    "python_wheel_task": PythonWheelTask(
                        package_name=package_name,
                        entry_point="dbxdec-run",
                        named_parameters=named_params,  # type: ignore[arg-type]  # SDK Variable wrappers
                    ),
                    **task_sdk_config,
                }
                if task_libraries is not None:
                    task_kwargs["libraries"] = task_libraries

                task_obj = Task(**task_kwargs)  # dynamic kwargs
                tasks.append(task_obj)

        # ----- job clusters -----------------------------------------------
        job_clusters: list[JobCluster] = []
        if job_meta.cluster is not None:
            job_clusters.append(
                JobCluster(
                    job_cluster_key=job_meta.cluster.name,
                    new_cluster=ClusterSpec.from_dict(job_meta.cluster.spec),  # type: ignore[arg-type]  # typed as ClusterSpecDict
                )
            )

        # ----- parameters -------------------------------------------------
        parameters = [
            JobParameterDefinition(name=k, default=v)
            for k, v in job_meta.params.items()
        ]

        # ----- backfill tag -----------------------------------------------
        if job_meta.backfill is not None:
            existing_tags: dict[str, str] = job_meta.sdk_config.get("tags", {})
            job_meta.sdk_config["tags"] = {
                **existing_tags,
                BACKFILL_TAG: _serialize_backfill_tag(job_meta.backfill),
            }

        job_obj = Job(
            name=job_name,
            tasks=tasks,  # type: ignore[arg-type]  # SDK Variable wrappers
            parameters=parameters,
            job_clusters=job_clusters,  # type: ignore[arg-type]  # SDK Variable wrappers
            **job_meta.sdk_config,
        )
        jobs[job_name] = job_obj

    return jobs
