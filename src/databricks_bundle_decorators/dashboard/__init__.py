"""Observability dashboard for framework-managed pipeline jobs.

This subpackage is the Dash-based replacement for the old monolith.
All public symbols are re-exported here so that existing imports
from ``databricks_bundle_decorators.dashboard`` keep working.
"""

from __future__ import annotations

# Data classes
from databricks_bundle_decorators.dashboard._data import (
    BackfillCoverage as BackfillCoverage,
    JobOverview as JobOverview,
    RunInfo as RunInfo,
    TaskRunInfo as TaskRunInfo,
)

# CLI data fetching
from databricks_bundle_decorators.dashboard._fetch import (
    fetch_job_runs as fetch_job_runs,
    fetch_task_runs as fetch_task_runs,
    resolve_job_ids as resolve_job_ids,
)

# Pure computation
from databricks_bundle_decorators.dashboard._compute import (
    _backfill_kind as _backfill_kind,
    _effective_state as _effective_state,
    _filter_past_keys as _filter_past_keys,
    _is_terminal_failure as _is_terminal_failure,
    build_job_overview as build_job_overview,
    compute_backfill_coverage as compute_backfill_coverage,
)

# Plotly figure builders
from databricks_bundle_decorators.dashboard._figures import (
    _build_daily_calendar as _build_daily_calendar,
    _build_hourly_calendar as _build_hourly_calendar,
    _build_monthly_calendar as _build_monthly_calendar,
    _build_partition_grid as _build_partition_grid,
    _build_task_dag_figure as _build_task_dag_figure,
    _build_weekly_calendar as _build_weekly_calendar,
)

# Polars helpers
from databricks_bundle_decorators.dashboard._polars_helpers import (
    _coverages_to_records as _coverages_to_records,
    _overviews_to_records as _overviews_to_records,
    _runs_to_records as _runs_to_records,
    _tasks_to_records as _tasks_to_records,
)

# Page helpers
from databricks_bundle_decorators.dashboard._pages import (
    _backfill_date_bounds as _backfill_date_bounds,
    _fmt_duration as _fmt_duration,
    _hourly_date_bounds as _hourly_date_bounds,
)

# App entry point
from databricks_bundle_decorators.dashboard._app import (
    APP_TEMPLATE as APP_TEMPLATE,
    run_app as run_app,
)

__all__ = [
    # Data classes
    "BackfillCoverage",
    "JobOverview",
    "RunInfo",
    "TaskRunInfo",
    # Fetch
    "fetch_job_runs",
    "fetch_task_runs",
    "resolve_job_ids",
    # Compute
    "_backfill_kind",
    "_effective_state",
    "_filter_past_keys",
    "_is_terminal_failure",
    "build_job_overview",
    "compute_backfill_coverage",
    # Figures
    "_build_daily_calendar",
    "_build_hourly_calendar",
    "_build_monthly_calendar",
    "_build_partition_grid",
    "_build_task_dag_figure",
    "_build_weekly_calendar",
    # Polars helpers
    "_coverages_to_records",
    "_overviews_to_records",
    "_runs_to_records",
    "_tasks_to_records",
    # Page helpers
    "_backfill_date_bounds",
    "_fmt_duration",
    "_hourly_date_bounds",
    # App
    "APP_TEMPLATE",
    "run_app",
]
