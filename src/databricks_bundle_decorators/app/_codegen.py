"""Generate Databricks App resource definitions for the bundle.

Reads the job registry and produces the ``app`` resource block that
wires each job as an app resource with ``CAN_VIEW`` permission.
Environment variables are emitted so the app can discover job IDs
at runtime via ``DBXDEC_JOB_*``.
"""

from __future__ import annotations

from typing import Any

from databricks_bundle_decorators.registry import _JOB_REGISTRY


def generate_app_resource(
    app_name: str,
    source_code_path: str = "./app",
    *,
    permission: str = "CAN_VIEW",
) -> dict[str, Any]:
    """Build a bundle-compatible app resource definition.

    The returned dictionary can be merged into the bundle's
    ``resources.apps`` section.  It declares each registered job as
    an app resource and emits ``DBXDEC_JOB_<name>`` environment
    variables via ``valueFrom`` so the app can discover job IDs at
    runtime.

    Parameters
    ----------
    app_name:
        The Databricks App name (must be lowercase, alphanumeric,
        and hyphens only).
    source_code_path:
        Path to the app source code directory, relative to the
        bundle root.
    permission:
        Permission level to grant the app's service principal on
        each job.  Defaults to ``CAN_VIEW``.

    Returns
    -------
    dict[str, Any]
        A dictionary with a single key (the app resource key) whose
        value is the full app resource definition.

    Example
    -------
    ::

        from databricks_bundle_decorators.app import generate_app_resource

        app_resources = generate_app_resource("my-pipeline-observability")
        # Merge into your bundle config alongside generate_resources()
    """
    resources: list[dict[str, Any]] = []
    env: list[dict[str, Any]] = []

    for job_name in sorted(_JOB_REGISTRY.keys()):
        # Resource name for the app resource binding
        resource_name = f"dbxdec-job-{job_name}".replace("_", "-")

        resources.append(
            {
                "name": resource_name,
                "description": f"Job: {job_name}",
                "job": {
                    "id": f"${{resources.jobs.{job_name}.id}}",
                    "permission": permission,
                },
            }
        )

        # Environment variable so the app discovers the job ID
        env_var_name = f"DBXDEC_JOB_{job_name.upper()}"
        env.append(
            {
                "name": env_var_name,
                "valueFrom": resource_name,
            }
        )

    # Sanitize app_name for use as a resource key
    resource_key = app_name.replace("-", "_")

    return {
        resource_key: {
            "name": app_name,
            "description": "Pipeline observability dashboard",
            "source_code_path": source_code_path,
            "config": {
                "command": ["python", "app.py"],
                "env": env,
            },
            "resources": resources,
        }
    }
