"""Dash page layout builders.

Each ``_page_*`` function returns a component tree — no side effects.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import whenever
from dash import dcc, html

from databricks_bundle_decorators.dashboard._compute import (
    _WEEK_KEY_RE,
)
from databricks_bundle_decorators.dashboard._data import (
    COLOR_COMPLETED,
    COLOR_FAILED,
    COLOR_IN_PROGRESS,
    BackfillCoverage,
    JobOverview,
)
from databricks_bundle_decorators.dashboard._display import (
    _coverages_to_records,
    _fmt_duration,
    _overviews_to_records,
)
from databricks_bundle_decorators.dashboard._figures import (
    _build_daily_calendar,
    _build_hourly_calendar,
    _build_monthly_calendar,
    _build_partition_grid,
    _build_weekly_calendar,
)

# ---------------------------------------------------------------------------
# KPI card helper
# ---------------------------------------------------------------------------

_MAX_MISSING_DISPLAY = 500


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
            "style": {"color": COLOR_COMPLETED, "fontWeight": "bold"},
        },
        {
            "condition": "params.value == 'FAILED'",
            "style": {"color": COLOR_FAILED, "fontWeight": "bold"},
        },
        {
            "condition": "params.value == 'RUNNING'",
            "style": {"color": COLOR_IN_PROGRESS, "fontWeight": "bold"},
        },
        {
            "condition": "params.value == 'INTERNAL_ERROR'",
            "style": {"color": COLOR_FAILED, "fontWeight": "bold"},
        },
    ],
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


def _backfill_date_bounds(
    kind: str,
    expected_keys: list[str],
    tz: str = "UTC",
) -> tuple[date | None, date | None, date | None, date | None]:
    """Derive (min_date, max_date, initial_start, initial_end) from keys.

    Works for ``daily``, ``weekly``, ``monthly``, and ``hourly`` kinds.
    Returns four ``None`` values when no valid dates can be parsed.
    """

    dates: list[date] = []
    if kind == "daily":
        for key in expected_keys:
            try:
                dates.append(date.fromisoformat(key))
            except ValueError:
                continue
    elif kind == "weekly":
        for key in expected_keys:
            m = _WEEK_KEY_RE.match(key)
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
                dates.append(date.fromisoformat(key[:10]))
            except ValueError:
                continue

    if not dates:
        return None, None, None, None

    unique = sorted(set(dates))
    min_d = unique[0]
    max_d = unique[-1]
    today = whenever.ZonedDateTime.now(tz).date().to_stdlib()
    # Anchor the visible window to today (clamped to the data range)
    init_end = min(today, max_d) if today >= min_d else max_d
    span_days = (max_d - min_d).days
    max_span, window_delta = _DATE_THRESHOLDS.get(kind, (180, timedelta(days=90)))
    init_start = max(min_d, init_end - window_delta) if span_days > max_span else min_d
    return min_d, max_d, init_start, init_end


# ---------------------------------------------------------------------------
# Backfill figure builder dispatch
# ---------------------------------------------------------------------------

#: Maps backfill kind → (label, figure_builder_fn).
_FIGURE_BUILDERS: dict[str, tuple[str, Any]] = {
    "daily": ("calendar", _build_daily_calendar),
    "weekly": ("week calendar", _build_weekly_calendar),
    "monthly": ("month calendar", _build_monthly_calendar),
    "hourly": ("hour calendar", _build_hourly_calendar),
}


def _build_coverage_figure(
    build_fn: Any,
    cov: BackfillCoverage,
    **kw: Any,
) -> Any:
    """Invoke a calendar figure builder with unpacked coverage data."""
    return build_fn(
        set(cov.expected_keys),
        set(cov.completed_keys),
        cov.completed_key_runs,
        errored_keys=set(cov.errored_keys or []),
        in_progress_keys=set(cov.in_progress_keys or []),
        tz=cov.tz,
        **kw,
    )


# ---------------------------------------------------------------------------
# Page: Overview
# ---------------------------------------------------------------------------


def _page_overview(
    overviews: list[JobOverview],
    coverages: dict[str, BackfillCoverage],
    workspace_url: str | None = None,
) -> Any:
    """Build the Overview page — KPI cards + unified job table.

    The table merges job stats and backfill completeness into a single
    view.  Job names link to the Databricks workspace when
    ``workspace_url`` is available; the Completeness column links to
    the backfill detail page.
    """
    total_jobs = len(overviews)
    deployed = sum(1 for o in overviews if o.job_id)
    total_runs = sum(o.total_runs for o in overviews)
    total_failures = sum(o.failures for o in overviews)
    all_durations = [
        o.avg_duration_seconds for o in overviews if o.avg_duration_seconds is not None
    ]
    avg_dur_s = round(sum(all_durations) / len(all_durations)) if all_durations else 0
    avg_dur = _fmt_duration(avg_dur_s)
    total_successes = sum(o.successes for o in overviews)
    total_terminal = total_successes + total_failures
    success_rate = (
        round(total_successes / total_terminal * 100, 1) if total_terminal else 0
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

    # Unified job table with workspace links and backfill completeness
    records = _overviews_to_records(
        overviews, coverages=coverages, workspace_url=workspace_url
    )

    column_defs = [
        {
            "field": "Job",
            "cellRenderer": "markdown",
            "linkTarget": "_blank",
            "minWidth": 140,
        },
        {"field": "Status", "cellStyle": _STATUS_CELL_STYLE, "maxWidth": 140},
        {"field": "Runs", "maxWidth": 160},
        {"field": "Success %", "maxWidth": 140},
        {"field": "Avg Duration", "maxWidth": 140},
        {"field": "Completeness", "cellRenderer": "markdown", "maxWidth": 160},
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

    return html.Div(
        [
            html.H4("Overview", className="mb-3"),
            kpi_row,
            html.H5("Jobs", className="mt-4 mb-3"),
            job_grid,
        ]
    )


# ---------------------------------------------------------------------------
# Page: Backfills
# ---------------------------------------------------------------------------


def _page_backfills(
    coverages: dict[str, BackfillCoverage],
) -> Any:
    """Build the Backfills page — AG Grid summary with clickable job links."""
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

    # Build records with clickable Job column and key status squares
    cov_records = _coverages_to_records(coverages)
    for r in cov_records:
        r["Job"] = f"[{r['Job']}](/backfills/{r['Job']})"

    cov_cols = [
        {"field": "Job", "cellRenderer": "markdown", "minWidth": 160},
        {"field": "Type", "maxWidth": 100},
        {"field": "Completeness", "minWidth": 140},
        {"field": "Errors", "maxWidth": 100},
        {"field": "Keys", "minWidth": 120},
    ]

    return html.Div(
        [
            html.H4("Backfill Completeness", className="mb-3"),
            html.P(
                "Completeness shows completed / due keys. "
                "Errors are keys with only failed runs. "
                "Click a job name for the completeness heatmap.",
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


# ---------------------------------------------------------------------------
# Page: Backfill Detail (completeness heatmap for a specific job)
# ---------------------------------------------------------------------------


def _page_backfill_detail(
    job_name: str,
    coverages: dict[str, BackfillCoverage],
    workspace_url: str | None = None,
    job_id: int | None = None,
) -> Any:
    """Build the Backfill Detail page — completeness heatmap for a job.

    Shows the backfill completeness chart with date-range picker for
    time-based backfills.  Includes a link to the Databricks
    workspace job page when ``workspace_url`` is available.
    """
    cov = coverages.get(job_name)
    if cov is None:
        return dbc.Alert(f"No backfill data for '{job_name}'.", color="warning")

    # Header with optional workspace link
    header_children: list[Any] = [html.H4(job_name, className="mb-1")]
    if workspace_url and job_id:
        header_children.append(
            html.P(
                html.A(
                    "View in Databricks workspace \u2197",
                    href=f"{workspace_url}/jobs/{job_id}",
                    target="_blank",
                    rel="noopener noreferrer",
                    className="text-decoration-none",
                ),
                className="mb-2",
            )
        )

    header_children.append(
        html.P(
            f"{cov.coverage_pct}% completeness "
            f"({len(cov.completed_keys)}/{len(cov.expected_keys)} keys)",
            className="text-muted",
        )
    )

    backfill_section: list[Any] = []

    if cov.kind in _FIGURE_BUILDERS:
        _, build_fn = _FIGURE_BUILDERS[cov.kind]
        min_d, max_d, init_start, init_end = _backfill_date_bounds(
            cov.kind, cov.expected_keys, tz=cov.tz
        )
        if (
            min_d is not None
            and max_d is not None
            and init_start is not None
            and init_end is not None
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
            fig = _build_coverage_figure(
                build_fn, cov, start_date=init_start, end_date=init_end
            )
        else:
            fig = _build_coverage_figure(build_fn, cov)
        if fig is not None:
            backfill_section.append(dcc.Graph(id="bf-graph", figure=fig))
    else:
        fig = _build_partition_grid(
            cov.expected_keys,
            set(cov.completed_keys),
            cov.completed_key_runs,
            errored_keys=set(cov.errored_keys or []),
            in_progress_keys=set(cov.in_progress_keys or []),
        )
        if fig is not None:
            backfill_section.append(dcc.Graph(figure=fig))

    if cov.missing_keys:
        if len(cov.missing_keys) <= _MAX_MISSING_DISPLAY:
            missing_text = "\n".join(cov.missing_keys)
        else:
            shown = cov.missing_keys[:_MAX_MISSING_DISPLAY]
            remaining = len(cov.missing_keys) - _MAX_MISSING_DISPLAY
            missing_text = "\n".join(shown) + f"\n\n… and {remaining} more"
        backfill_section.append(
            html.Details(
                [
                    html.Summary(
                        f"{len(cov.missing_keys)} missing keys",
                        className="text-warning mb-2",
                    ),
                    html.Pre(
                        missing_text,
                        className="bg-light p-3 rounded",
                    ),
                ]
            )
        )

    return html.Div(
        [
            dbc.Button(
                "\u2190 Back to Backfills",
                href="/backfills",
                color="link",
                className="mb-2 ps-0",
            ),
            *header_children,
            *backfill_section,
        ]
    )
