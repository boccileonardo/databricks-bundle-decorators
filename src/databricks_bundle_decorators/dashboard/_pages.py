"""Dash page layout builders.

Each ``_page_*`` function returns a component tree — no side effects.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import dash_ag_grid as dag
import dash_bootstrap_components as dbc
from dash import dcc, html

from databricks_bundle_decorators.dashboard._compute import (
    _effective_state,
    _is_terminal_failure,
)
from databricks_bundle_decorators.dashboard._data import (
    BackfillCoverage,
    JobOverview,
    RunInfo,
)
from databricks_bundle_decorators.dashboard._fetch import fetch_task_runs
from databricks_bundle_decorators.dashboard._figures import (
    _build_daily_calendar,
    _build_hourly_calendar,
    _build_monthly_calendar,
    _build_partition_grid,
    _build_task_dag_figure,
    _build_weekly_calendar,
)
from databricks_bundle_decorators.dashboard._polars_helpers import (
    _coverages_to_records,
    _overviews_to_records,
    _runs_to_records,
    _tasks_to_records,
)

# ---------------------------------------------------------------------------
# Status badge colours
# ---------------------------------------------------------------------------

_STATE_BADGE_COLORS: dict[str, str] = {
    "SUCCESS": "success",
    "FAILED": "danger",
    "RUNNING": "primary",
    "PENDING": "secondary",
    "CANCELED": "warning",
    "TIMED_OUT": "warning",
    "INTERNAL_ERROR": "danger",
    "SKIPPED": "secondary",
    "UNKNOWN": "light",
}


def _state_badge(state: str) -> Any:
    """Return a Bootstrap badge component for a run/task state."""
    return dbc.Badge(
        state,
        color=_STATE_BADGE_COLORS.get(state, "light"),
        className="me-1",
    )


# ---------------------------------------------------------------------------
# KPI card helper
# ---------------------------------------------------------------------------


def _fmt_duration(seconds: int | float) -> str:
    """Format a duration as ``Hh MMm SSs``, ``Mm SSs``, or ``Ns``."""
    total = int(seconds)
    if total < 60:
        return f"{total}s"
    m, s = divmod(total, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m {s:02d}s"


def _kpi_card(title: str, value: str | int, color: str = "primary") -> Any:
    """Return a Bootstrap card showing a single KPI metric."""
    return dbc.Card(
        dbc.CardBody(
            [
                html.H6(title, className="card-subtitle mb-1 text-muted"),
                html.H3(str(value), className=f"card-title text-{color} mb-0"),
            ],
            className="text-center py-3",
        ),
        className="shadow-sm rounded-3",
    )


# ---------------------------------------------------------------------------
# AG Grid column style helpers
# ---------------------------------------------------------------------------

_STATUS_CELL_STYLE = {
    "styleConditions": [
        {
            "condition": "params.value == 'SUCCESS'",
            "style": {"color": "#2fb380", "fontWeight": "bold"},
        },
        {
            "condition": "params.value == 'FAILED'",
            "style": {"color": "#cf3257", "fontWeight": "bold"},
        },
        {
            "condition": "params.value == 'RUNNING'",
            "style": {"color": "#3459e6", "fontWeight": "bold"},
        },
        {
            "condition": "params.value == 'INTERNAL_ERROR'",
            "style": {"color": "#cf3257", "fontWeight": "bold"},
        },
    ],
}

_DEFAULT_GRID_STYLE: dict[str, Any] = {
    "height": "100%",
    "width": "100%",
}

_DEFAULT_GRID_CLASSNAME = "ag-theme-quartz"


def _default_col_def() -> dict[str, Any]:
    return {
        "resizable": True,
        "sortable": True,
        "filter": True,
        "flex": 1,
    }


# ---------------------------------------------------------------------------
# Backfill date-range helpers
# ---------------------------------------------------------------------------

#: (max_span_days, default_window_delta) thresholds per backfill kind.
#: When the date span exceeds max_span_days the initial date picker
#: range is set to [max_date - window, max_date].
_DATE_THRESHOLDS: dict[str, tuple[int, timedelta]] = {
    "daily": (180, timedelta(days=90)),
    "weekly": (730, timedelta(weeks=52)),
    "monthly": (1460, timedelta(days=730)),  # ~24 months
    "hourly": (14, timedelta(days=7)),
}

#: Kinds that support date-range filtering via a DatePickerRange.
_DATE_FILTERABLE_KINDS = frozenset(_DATE_THRESHOLDS)


def _backfill_date_bounds(
    kind: str,
    expected_keys: list[str],
) -> tuple[date | None, date | None, date | None, date | None]:
    """Derive (min_date, max_date, initial_start, initial_end) from keys.

    Works for ``daily``, ``weekly``, ``monthly``, and ``hourly`` kinds.
    Returns four ``None`` values when no valid dates can be parsed.
    """
    import re as _re

    dates: list[date] = []
    if kind == "daily":
        for key in expected_keys:
            try:
                dates.append(date.fromisoformat(key))
            except ValueError:
                continue
    elif kind == "weekly":
        _week_re = _re.compile(r"^(\d{4})-W(\d{2})$")
        for key in expected_keys:
            m = _week_re.match(key)
            if m:
                dates.append(date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1))
    elif kind == "monthly":
        for key in expected_keys:
            try:
                d = date.fromisoformat(key)
                dates.append(date(d.year, d.month, 1))
            except ValueError:
                continue
    elif kind == "hourly":
        for key in expected_keys:
            try:
                dates.append(datetime.strptime(key, "%Y-%m-%dT%H").date())
            except ValueError:
                continue

    if not dates:
        return None, None, None, None

    unique = sorted(set(dates))
    min_d = unique[0]
    max_d = unique[-1]
    today = date.today()
    # Anchor the visible window to today (clamped to the data range)
    init_end = min(today, max_d) if today >= min_d else max_d
    span_days = (max_d - min_d).days
    max_span, window_delta = _DATE_THRESHOLDS.get(kind, (180, timedelta(days=90)))
    if span_days > max_span:
        init_start = max(min_d, init_end - window_delta)
    else:
        init_start = min_d
    return min_d, max_d, init_start, init_end


# Keep old name available for backwards compat / existing tests
def _hourly_date_bounds(
    expected_keys: list[str],
) -> tuple[date | None, date | None, date | None, date | None]:
    """Derive date bounds from hourly keys.  Delegates to `_backfill_date_bounds`."""
    return _backfill_date_bounds("hourly", expected_keys)


# ---------------------------------------------------------------------------
# Backfill figure builder dispatch
# ---------------------------------------------------------------------------

#: Maps backfill kind → (label, figure_builder_fn). The builder accepts
#: a `BackfillCoverage` plus optional ``start_date`` / ``end_date``.
_FIGURE_BUILDERS: dict[str, tuple[str, Any]] = {
    "daily": (
        "calendar",
        lambda c, **kw: _build_daily_calendar(
            set(c.expected_keys), set(c.completed_keys), c.completed_key_runs, **kw
        ),
    ),
    "weekly": (
        "week calendar",
        lambda c, **kw: _build_weekly_calendar(
            set(c.expected_keys), set(c.completed_keys), c.completed_key_runs, **kw
        ),
    ),
    "monthly": (
        "month calendar",
        lambda c, **kw: _build_monthly_calendar(
            set(c.expected_keys), set(c.completed_keys), c.completed_key_runs, **kw
        ),
    ),
    "hourly": (
        "hour calendar",
        lambda c, **kw: _build_hourly_calendar(
            set(c.expected_keys), set(c.completed_keys), c.completed_key_runs, **kw
        ),
    ),
}


# ---------------------------------------------------------------------------
# Page: Overview
# ---------------------------------------------------------------------------


def _page_overview(
    overviews: list[JobOverview],
    coverages: dict[str, BackfillCoverage],
) -> Any:
    """Build the Overview page — KPI cards + searchable/paginated job table."""
    total_jobs = len(overviews)
    deployed = sum(1 for o in overviews if o.job_id)
    total_runs = sum(o.total_runs for o in overviews)
    total_failures = sum(o.failures for o in overviews)
    all_durations = [
        o.avg_duration_seconds for o in overviews if o.avg_duration_seconds is not None
    ]
    avg_dur_s = round(sum(all_durations) / len(all_durations)) if all_durations else 0
    avg_dur = _fmt_duration(avg_dur_s)
    success_rate = (
        round(
            sum(o.successes for o in overviews) / total_runs * 100,
            1,
        )
        if total_runs
        else 0
    )

    kpi_row = dbc.Row(
        [
            dbc.Col(_kpi_card("Registered Jobs", total_jobs), md=2),
            dbc.Col(_kpi_card("Deployed", deployed), md=2),
            dbc.Col(_kpi_card("Total Runs", total_runs), md=2),
            dbc.Col(_kpi_card("Success Rate", f"{success_rate}%", "success"), md=2),
            dbc.Col(_kpi_card("Failures", total_failures, "danger"), md=2),
            dbc.Col(_kpi_card("Avg Duration", avg_dur), md=2),
        ],
        className="mb-4 g-3",
    )

    # Job overview table with AG Grid — searchable and paginated
    records = _overviews_to_records(overviews)
    column_defs = [
        {"field": "Job", "cellRenderer": "agGroupCellRenderer"},
        {"field": "Deployed", "maxWidth": 100},
        {"field": "Runs", "maxWidth": 80},
        {"field": "Pass", "maxWidth": 80},
        {"field": "Fail", "maxWidth": 80},
        {"field": "Rate", "maxWidth": 80},
        {"field": "Last Run"},
        {"field": "Status", "cellStyle": _STATUS_CELL_STYLE, "maxWidth": 140},
        {"field": "Avg Duration (s)", "maxWidth": 140},
        {"field": "Backfill", "maxWidth": 80},
    ]

    job_grid = dag.AgGrid(
        id="overview-jobs-grid",
        rowData=records,
        columnDefs=column_defs,
        defaultColDef=_default_col_def(),
        dashGridOptions={
            "pagination": True,
            "paginationPageSize": 20,
            "paginationPageSizeSelector": [10, 20, 50, 100],
            "animateRows": True,
        },
        className=_DEFAULT_GRID_CLASSNAME,
        style={"height": "600px"},
    )

    # Backfill summary (if any)
    backfill_section: list[Any] = []
    if coverages:
        cov_records = _coverages_to_records(coverages)
        cov_cols = [{"field": k} for k in cov_records[0]]
        backfill_section = [
            html.H5("Backfill Coverage", className="mt-4 mb-3"),
            dag.AgGrid(
                rowData=cov_records,
                columnDefs=cov_cols,
                defaultColDef=_default_col_def(),
                className=_DEFAULT_GRID_CLASSNAME,
                style={"height": "300px"},
            ),
        ]

    return html.Div(
        [
            html.H4("Overview", className="mb-3"),
            kpi_row,
            html.H5("Jobs", className="mt-4 mb-3"),
            job_grid,
            *backfill_section,
        ]
    )


# ---------------------------------------------------------------------------
# Page: Jobs
# ---------------------------------------------------------------------------


def _page_jobs(overviews: list[JobOverview]) -> Any:
    """Build the Jobs page — AG Grid table with clickable job names."""
    records = _overviews_to_records(overviews)
    if not records:
        return html.Div(
            html.P("No jobs registered.", className="text-muted"),
        )

    column_defs = [
        {
            "field": "Job",
            "cellRenderer": "markdown",
            "cellRendererParams": {},
        },
        {"field": "Deployed", "maxWidth": 100},
        {"field": "Runs", "maxWidth": 80},
        {"field": "Pass", "maxWidth": 80},
        {"field": "Fail", "maxWidth": 80},
        {"field": "Rate", "maxWidth": 80},
        {"field": "Last Run"},
        {"field": "Status", "cellStyle": _STATUS_CELL_STYLE, "maxWidth": 140},
        {"field": "Avg Duration (s)", "maxWidth": 140},
        {"field": "Backfill", "maxWidth": 80},
    ]

    # Add markdown links for job names
    for r in records:
        r["Job"] = f"[{r['Job']}](/jobs/{r['Job']})"

    return html.Div(
        [
            html.H4("Jobs", className="mb-3"),
            html.P(
                f"{len(records)} registered jobs across the bundle.",
                className="text-muted",
            ),
            dag.AgGrid(
                id="jobs-grid",
                rowData=records,
                columnDefs=column_defs,
                defaultColDef=_default_col_def(),
                dashGridOptions={
                    "pagination": True,
                    "paginationPageSize": 25,
                    "paginationPageSizeSelector": [10, 25, 50, 100],
                    "animateRows": True,
                },
                className=_DEFAULT_GRID_CLASSNAME,
                style={"height": "700px"},
            ),
        ]
    )


# ---------------------------------------------------------------------------
# Page: Runs (all runs across all jobs)
# ---------------------------------------------------------------------------


def _page_runs(
    all_runs: dict[str, list[RunInfo]],
    job_names: list[str],
) -> Any:
    """Build the Runs page — all runs with clickable Run ID links."""
    all_records: list[dict[str, Any]] = []
    for name in job_names:
        for r in all_runs.get(name, []):
            start = "\u2014"
            if r.start_time_ms:
                dt = datetime.fromtimestamp(r.start_time_ms / 1000, tz=timezone.utc)
                start = dt.strftime("%Y-%m-%d %H:%M")
            all_records.append(
                {
                    "Run ID": f"[{r.run_id}](/runs/{r.run_id})",
                    "Job": f"[{name}](/jobs/{name})",
                    "Status": _effective_state(r.result_state, r.life_cycle_state),
                    "Start": start,
                    "Duration (s)": (
                        str(r.duration_seconds) if r.duration_seconds else "\u2014"
                    ),
                    "Backfill Key": r.backfill_key or "",
                }
            )

    if not all_records:
        return html.Div(
            [
                html.H4("Runs", className="mb-3"),
                dbc.Alert("No runs found.", color="info"),
            ]
        )

    column_defs = [
        {"field": "Run ID", "cellRenderer": "markdown", "maxWidth": 140},
        {"field": "Job", "cellRenderer": "markdown"},
        {"field": "Status", "cellStyle": _STATUS_CELL_STYLE, "maxWidth": 140},
        {"field": "Start"},
        {"field": "Duration (s)", "maxWidth": 120},
        {"field": "Backfill Key"},
    ]

    return html.Div(
        [
            html.H4("Runs", className="mb-3"),
            html.P(
                f"{len(all_records)} runs across {len(job_names)} jobs.",
                className="text-muted",
            ),
            dag.AgGrid(
                id="runs-grid",
                rowData=all_records,
                columnDefs=column_defs,
                defaultColDef=_default_col_def(),
                dashGridOptions={
                    "pagination": True,
                    "paginationPageSize": 50,
                    "paginationPageSizeSelector": [25, 50, 100],
                    "animateRows": True,
                },
                className=_DEFAULT_GRID_CLASSNAME,
                style={"height": "700px"},
            ),
        ]
    )


# ---------------------------------------------------------------------------
# Page: Run Detail (task DAG + task table for a specific run)
# ---------------------------------------------------------------------------


def _page_run_detail(
    run_id: int,
    all_runs: dict[str, list[RunInfo]],
    profile: str | None,
) -> Any:
    """Build the Run Detail page — task DAG and task breakdown for a run."""
    # Find which job this run belongs to
    job_name: str | None = None
    run_info: RunInfo | None = None
    for name, runs in all_runs.items():
        for r in runs:
            if r.run_id == run_id:
                job_name = name
                run_info = r
                break
        if job_name:
            break

    if run_info is None:
        return dbc.Alert(f"Run {run_id} not found.", color="warning")

    state = _effective_state(run_info.result_state, run_info.life_cycle_state)

    # Header
    header = html.Div(
        [
            html.H4(
                [
                    f"Run {run_id}",
                    html.Span(" "),
                    _state_badge(state),
                ],
                className="mb-1",
            ),
            html.P(
                [
                    html.Span("Job: ", className="text-muted"),
                    html.A(
                        job_name,
                        href=f"/jobs/{job_name}",
                        className="text-decoration-none",
                    ),
                    html.Span(
                        f"  \u00b7  Duration: {run_info.duration_seconds or '\u2014'}s",
                        className="text-muted ms-2",
                    ),
                ],
            ),
        ],
        className="mb-4",
    )

    # Error alert
    error_section: list[Any] = []
    if run_info.state_message and _is_terminal_failure(
        run_info.result_state, run_info.life_cycle_state
    ):
        error_section.append(
            dbc.Alert(
                f"{state}: {run_info.state_message}",
                color="danger",
                className="mb-3",
            )
        )

    # Fetch tasks
    task_runs = fetch_task_runs(run_id, profile=profile)

    dag_section: list[Any] = []
    task_table_section: list[Any] = []

    if task_runs:
        # Task DAG
        dag_fig = _build_task_dag_figure(task_runs)
        if dag_fig is not None:
            dag_section = [
                html.H5("Task DAG", className="mt-3 mb-3"),
                dcc.Graph(figure=dag_fig),
            ]

        # Task table
        task_records = _tasks_to_records(task_runs)
        if task_records:
            task_cols = [
                {"field": "Task"},
                {"field": "Status", "cellStyle": _STATUS_CELL_STYLE, "maxWidth": 140},
                {"field": "Duration (s)", "maxWidth": 120},
                {"field": "Error"},
            ]
            task_table_section = [
                html.H5("Task Breakdown", className="mt-3 mb-2"),
                dag.AgGrid(
                    rowData=task_records,
                    columnDefs=task_cols,
                    defaultColDef=_default_col_def(),
                    className=_DEFAULT_GRID_CLASSNAME,
                    style={"height": "400px"},
                ),
            ]
    else:
        dag_section = [
            html.P("No task data available for this run.", className="text-muted mt-3")
        ]

    return html.Div(
        [
            dbc.Button(
                "\u2190 Back to Runs",
                href="/runs",
                color="link",
                className="mb-2 ps-0",
            ),
            header,
            *error_section,
            *dag_section,
            *task_table_section,
        ]
    )


# ---------------------------------------------------------------------------
# Page: Job Detail
# ---------------------------------------------------------------------------


def _page_job_detail(
    job_name: str,
    overviews: list[JobOverview],
    runs: list[RunInfo],
    coverages: dict[str, BackfillCoverage],
    profile: str | None,
) -> Any:
    """Build the Job Detail page — run history, task DAG, backfill coverage."""
    overview = next((o for o in overviews if o.job_name == job_name), None)
    if overview is None:
        return dbc.Alert(f"Job '{job_name}' not found.", color="warning")

    # Header
    state = overview.last_run_state or "UNKNOWN"
    header = html.Div(
        [
            html.H4(
                [
                    job_name,
                    html.Span(" "),
                    _state_badge(state),
                ],
                className="mb-1",
            ),
            html.P(
                [
                    html.Span(
                        f"{overview.total_runs} runs  \u00b7  "
                        f"{overview.successes} passed  \u00b7  "
                        f"{overview.failures} failed",
                        className="text-muted",
                    ),
                ],
            ),
        ],
        className="mb-4",
    )

    # Run history table with clickable Run IDs
    run_records = _runs_to_records(runs)
    for r in run_records:
        r["Run ID"] = f"[{r['Run ID']}](/runs/{r['Run ID']})"

    run_cols = [
        {"field": "Run ID", "cellRenderer": "markdown", "maxWidth": 140},
        {"field": "Status", "cellStyle": _STATUS_CELL_STYLE, "maxWidth": 140},
        {"field": "Start"},
        {"field": "Duration (s)", "maxWidth": 120},
        {"field": "Backfill Key"},
    ]

    run_table: Any = (
        dag.AgGrid(
            rowData=run_records,
            columnDefs=run_cols,
            defaultColDef=_default_col_def(),
            dashGridOptions={
                "pagination": True,
                "paginationPageSize": 25,
                "paginationPageSizeSelector": [10, 25, 50],
            },
            className=_DEFAULT_GRID_CLASSNAME,
            style={"height": "500px"},
        )
        if run_records
        else html.P("No runs found.", className="text-muted")
    )

    # Error messages from recent failures
    error_alerts: list[Any] = []
    errored = [
        r
        for r in runs
        if _is_terminal_failure(r.result_state, r.life_cycle_state) and r.state_message
    ]
    for r in errored[:3]:
        error_alerts.append(
            dbc.Alert(
                f"Run {r.run_id} "
                f"({_effective_state(r.result_state, r.life_cycle_state)}): "
                f"{r.state_message}",
                color="danger",
                className="mb-2",
            )
        )

    # Task DAG from most recent run
    dag_section: list[Any] = []
    if runs and overview.job_id:
        latest_run = runs[0]
        task_runs = fetch_task_runs(latest_run.run_id, profile=profile)
        if task_runs:
            dag_fig = _build_task_dag_figure(task_runs)
            if dag_fig is not None:
                dag_section.append(
                    html.H5("Task DAG (latest run)", className="mt-4 mb-3")
                )
                dag_section.append(dcc.Graph(figure=dag_fig))

            task_records = _tasks_to_records(task_runs)
            if task_records:
                task_cols = [
                    {"field": "Task"},
                    {
                        "field": "Status",
                        "cellStyle": _STATUS_CELL_STYLE,
                        "maxWidth": 140,
                    },
                    {"field": "Duration (s)", "maxWidth": 120},
                    {"field": "Error"},
                ]
                dag_section.append(html.H5("Task Breakdown", className="mt-3 mb-2"))
                dag_section.append(
                    dag.AgGrid(
                        rowData=task_records,
                        columnDefs=task_cols,
                        defaultColDef=_default_col_def(),
                        className=_DEFAULT_GRID_CLASSNAME,
                        style={"height": "400px"},
                    )
                )

    # Backfill coverage for this job
    backfill_section: list[Any] = []
    cov = coverages.get(job_name)
    if cov:
        backfill_section.append(html.H5("Backfill Coverage", className="mt-4 mb-3"))
        backfill_section.append(
            html.P(
                f"{cov.coverage_pct}% coverage "
                f"({len(cov.completed_keys)}/{len(cov.expected_keys)} keys)",
                className="text-muted",
            )
        )
        if cov.kind in _DATE_FILTERABLE_KINDS:
            min_d, max_d, init_start, init_end = _backfill_date_bounds(
                cov.kind, cov.expected_keys
            )
            builder = _FIGURE_BUILDERS.get(cov.kind)
            if (
                min_d is not None
                and max_d is not None
                and init_start is not None
                and init_end is not None
                and builder is not None
            ):
                backfill_section.append(dcc.Store(id="bf-job-name", data=job_name))
                backfill_section.append(dcc.Store(id="bf-kind", data=cov.kind))
                backfill_section.append(
                    dbc.Row(
                        dbc.Col(
                            dcc.DatePickerRange(
                                id="bf-date-range",
                                start_date=init_start.isoformat(),
                                end_date=init_end.isoformat(),
                                min_date_allowed=min_d.isoformat(),
                                max_date_allowed=max_d.isoformat(),
                                className="mb-3",
                            ),
                            width="auto",
                        ),
                    )
                )
                _, build_fn = builder
                fig = build_fn(cov, start_date=init_start, end_date=init_end)
                if fig is not None:
                    backfill_section.append(dcc.Graph(id="bf-graph", figure=fig))
        elif builder := _FIGURE_BUILDERS.get(cov.kind):
            _, build_fn = builder
            fig = build_fn(cov)
            if fig is not None:
                backfill_section.append(dcc.Graph(figure=fig))
        else:
            fig = _build_partition_grid(
                cov.expected_keys,
                set(cov.completed_keys),
                cov.completed_key_runs,
            )
            if fig is not None:
                backfill_section.append(dcc.Graph(figure=fig))
        if cov.missing_keys:
            backfill_section.append(
                html.Details(
                    [
                        html.Summary(
                            f"{len(cov.missing_keys)} keys not launched",
                            className="text-warning mb-2",
                        ),
                        html.Pre(
                            "\n".join(cov.missing_keys),
                            className="bg-light p-3 rounded",
                        ),
                    ]
                )
            )

    return html.Div(
        [
            dbc.Button(
                "\u2190 Back to Jobs",
                href="/jobs",
                color="link",
                className="mb-2 ps-0",
            ),
            header,
            *error_alerts,
            html.H5("Run History", className="mb-3"),
            run_table,
            *dag_section,
            *backfill_section,
        ]
    )


# ---------------------------------------------------------------------------
# Page: Backfills
# ---------------------------------------------------------------------------


def _page_backfills(coverages: dict[str, BackfillCoverage]) -> Any:
    """Build the Backfills page — AG Grid summary with clickable job links.

    Individual heatmaps are shown on each job's detail page
    (``/jobs/<name>``), keeping this page fast even with hundreds
    of backfill jobs.
    """
    if not coverages:
        return html.Div(
            [
                html.H4("Backfills", className="mb-3"),
                dbc.Alert(
                    "No jobs with backfill definitions found.",
                    color="info",
                ),
            ]
        )

    # Build records with clickable Job column
    cov_records = _coverages_to_records(coverages)
    for r in cov_records:
        r["Job"] = f"[{r['Job']}](/jobs/{r['Job']})"

    cov_cols = [
        {"field": "Job", "cellRenderer": "markdown"},
        {"field": "Type", "maxWidth": 120},
        {"field": "Expected", "maxWidth": 100},
        {"field": "Completed", "maxWidth": 110},
        {"field": "Missing", "maxWidth": 100},
        {"field": "Coverage", "maxWidth": 100},
    ]

    return html.Div(
        [
            html.H4("Backfill Coverage", className="mb-3"),
            html.P(
                "Expected keys from BackfillDef vs successful runs "
                "with matching backfill_key parameter. "
                "Click a job name for the coverage heatmap. "
                "For exact key-level catchup, use: dbxdec catchup",
                className="text-muted",
            ),
            dag.AgGrid(
                id="backfills-grid",
                rowData=cov_records,
                columnDefs=cov_cols,
                defaultColDef=_default_col_def(),
                dashGridOptions={
                    "pagination": True,
                    "paginationPageSize": 25,
                    "paginationPageSizeSelector": [10, 25, 50, 100],
                    "animateRows": True,
                },
                className=_DEFAULT_GRID_CLASSNAME,
                style={"height": "600px"},
            ),
        ]
    )
