"""Databricks App for pipeline observability.

This module implements a Dash-based observability dashboard designed
to run as a native Databricks App.  Unlike the local ``dashboard``
module (which shells out to the Databricks CLI), this uses the
Databricks Python SDK and discovers jobs via environment variables
injected by the bundle app resource declarations.
"""

from __future__ import annotations

from databricks_bundle_decorators.app._app import run_app as run_app
from databricks_bundle_decorators.app._codegen import (
    generate_app_config_yaml as generate_app_config_yaml,
)
from databricks_bundle_decorators.app._codegen import (
    generate_app_resource as generate_app_resource,
)
from databricks_bundle_decorators.app._codegen import (
    generate_registry_json as generate_registry_json,
)
from databricks_bundle_decorators.app._fetch import (
    fetch_job_runs as fetch_job_runs,
)
from databricks_bundle_decorators.app._fetch import (
    resolve_job_ids_from_env as resolve_job_ids_from_env,
)
from databricks_bundle_decorators.app._fetch import (
    resolve_workspace_url as resolve_workspace_url,
)

__all__ = [
    "fetch_job_runs",
    "generate_app_config_yaml",
    "generate_app_resource",
    "generate_registry_json",
    "resolve_job_ids_from_env",
    "resolve_workspace_url",
    "run_app",
]
