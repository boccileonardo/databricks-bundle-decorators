"""Convert registries into ``databricks.bundles.jobs`` resource objects.

Called at deploy time by the resource loader.  Reads the global
registries populated by ``@task``, ``@job_cluster``, and ``@job`` decorators
and produces ``Job`` dataclass instances that the Databricks CLI
serialises into the bundle configuration.
"""

from databricks_bundle_decorators.registry import (
    _CLUSTER_REGISTRY,
    _JOB_REGISTRY,
    _TASK_REGISTRY,
)


def generate_resources(package_name: str = "databricks_bundle_decorators") -> dict:
    """Build ``{resource_key: Job}`` from the global registries.

    Parameters
    ----------
    package_name:
        The Python package name used in ``PythonWheelTask``.  Must match
        the ``[project.name]`` in *pyproject.toml*.
    """
    from databricks.bundles.jobs import (
        ClusterSpec,
        Job,
        JobCluster,
        JobParameterDefinition,
        Library,
        PythonWheelTask,
        Task,
        TaskDependency,
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

            # Forward every job-level parameter to the task CLI
            for param_name in job_meta.params:
                named_params[param_name] = (
                    "{{" + f'job.parameters["{param_name}"]' + "}}"
                )

            # ----- per-task SDK config (max_retries, timeout, etc.) -----
            qualified_key = f"{job_name}.{task_key}"
            task_meta = _TASK_REGISTRY.get(qualified_key)
            task_sdk_config = task_meta.sdk_config if task_meta else {}

            task_obj = Task(
                task_key=task_key,
                depends_on=depends_on,
                job_cluster_key=job_meta.cluster,
                python_wheel_task=PythonWheelTask(
                    package_name=package_name,
                    entry_point="dbxdec-run",
                    named_parameters=named_params,  # type: ignore[arg-type]  # SDK Variable wrappers
                ),
                libraries=[Library(whl="dist/*.whl")],
                **task_sdk_config,
            )
            tasks.append(task_obj)

        # ----- job clusters -----------------------------------------------
        job_clusters: list[JobCluster] = []
        if job_meta.cluster and job_meta.cluster in _CLUSTER_REGISTRY:
            cluster_meta = _CLUSTER_REGISTRY[job_meta.cluster]
            job_clusters.append(
                JobCluster(
                    job_cluster_key=cluster_meta.name,
                    new_cluster=ClusterSpec.from_dict(cluster_meta.spec),  # type: ignore[arg-type]  # typed as ClusterSpecDict
                )
            )

        # ----- parameters -------------------------------------------------
        parameters = [
            JobParameterDefinition(name=k, default=v)
            for k, v in job_meta.params.items()
        ]

        job_obj = Job(
            name=job_name,
            tasks=tasks,  # type: ignore[arg-type]  # SDK Variable wrappers
            parameters=parameters,
            job_clusters=job_clusters,  # type: ignore[arg-type]  # SDK Variable wrappers
            **job_meta.sdk_config,
        )
        jobs[job_name] = job_obj

    return jobs
