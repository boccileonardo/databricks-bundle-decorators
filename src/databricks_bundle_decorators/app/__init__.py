"""Observability dashboard for pipeline jobs.

Implements a Dash-based dashboard that can be deployed as a native
Databricks App.  Job IDs are resolved at runtime via the Databricks
SDK (``WorkspaceClient().apps.get()``), using the app's resource
bindings.
"""

from __future__ import annotations

# App entry point
from databricks_bundle_decorators.app._app import run_app as run_app

# Codegen
from databricks_bundle_decorators.app._codegen import (
    generate_app_config_yaml as generate_app_config_yaml,
)
from databricks_bundle_decorators.app._codegen import (
    generate_app_resource as generate_app_resource,
)
from databricks_bundle_decorators.app._codegen import (
    generate_registry_json as generate_registry_json,
)
from databricks_bundle_decorators.app._codegen import (
    sync_registry_json as sync_registry_json,
)

# Pure computation
from databricks_bundle_decorators.app._compute import (
    build_job_overview as build_job_overview,
)
from databricks_bundle_decorators.app._compute import (
    compute_backfill_coverage as compute_backfill_coverage,
)

# Data classes
from databricks_bundle_decorators.app._data import (
    BackfillCoverage as BackfillCoverage,
)
from databricks_bundle_decorators.app._data import (
    JobOverview as JobOverview,
)
from databricks_bundle_decorators.app._data import (
    RunInfo as RunInfo,
)

# Data fetching
from databricks_bundle_decorators.app._fetch import (
    fetch_job_runs as fetch_job_runs,
)
from databricks_bundle_decorators.app._fetch import (
    resolve_workspace_url as resolve_workspace_url,
)

__all__ = [
    "BackfillCoverage",
    "JobOverview",
    "RunInfo",
    "build_job_overview",
    "compute_backfill_coverage",
    "fetch_job_runs",
    "generate_app_config_yaml",
    "generate_app_resource",
    "generate_registry_json",
    "resolve_workspace_url",
    "run_app",
    "sync_registry_json",
]
