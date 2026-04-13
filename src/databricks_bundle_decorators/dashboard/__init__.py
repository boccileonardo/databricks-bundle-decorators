"""Observability dashboard for framework-managed pipeline jobs."""

from __future__ import annotations

# App entry point
from databricks_bundle_decorators.dashboard._app import (
    APP_TEMPLATE as APP_TEMPLATE,
)
from databricks_bundle_decorators.dashboard._app import (
    run_app as run_app,
)

# Pure computation
from databricks_bundle_decorators.dashboard._compute import (
    build_job_overview as build_job_overview,
)
from databricks_bundle_decorators.dashboard._compute import (
    compute_backfill_coverage as compute_backfill_coverage,
)

# Data classes
from databricks_bundle_decorators.dashboard._data import (
    BackfillCoverage as BackfillCoverage,
)
from databricks_bundle_decorators.dashboard._data import (
    JobOverview as JobOverview,
)
from databricks_bundle_decorators.dashboard._data import (
    RunInfo as RunInfo,
)

# CLI data fetching
from databricks_bundle_decorators.dashboard._fetch import (
    fetch_job_runs as fetch_job_runs,
)
from databricks_bundle_decorators.dashboard._fetch import (
    resolve_bundle_targets as resolve_bundle_targets,
)
from databricks_bundle_decorators.dashboard._fetch import (
    resolve_job_ids as resolve_job_ids,
)
from databricks_bundle_decorators.dashboard._fetch import (
    resolve_workspace_url as resolve_workspace_url,
)

__all__ = [
    "APP_TEMPLATE",
    "BackfillCoverage",
    "JobOverview",
    "RunInfo",
    "build_job_overview",
    "compute_backfill_coverage",
    "fetch_job_runs",
    "resolve_bundle_targets",
    "resolve_job_ids",
    "resolve_workspace_url",
    "run_app",
]
