"""Dash page layout builders.

Each ``_page_*`` function returns a component tree — no side effects.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import dash_ag_grid as dag
import dash_bootstrap_components as dbc
from dash import dcc, html

from databricks_bundle_decorators.dashboard._data import (
    BackfillCoverage,
    JobOverview,
)
from databricks_bundle_decorators.dashboard._figures import (
    _build_daily_calendar,
    _build_hourly_calendar,
    _build_monthly_calendar,
    _build_partition_grid,
    _build_weekly_calendar,
)
from databricks_bundle_decorators.dashboard._display import (
    _coverages_to_records,
    _fmt_duration,
    _overviews_to_records,
)

# ---------------------------------------------------------------------------
# KPI card helper
# ---------------------------------------------------------------------------


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
    from datetime import datetime

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


# ---------------------------------------------------------------------------
# Backfill figure builder dispatch
# ---------------------------------------------------------------------------

#: Maps backfill kind → (label, figure_builder_fn). The builder accepts
#: a `BackfillCoverage` plus optional ``start_date`` / ``end_date``.
_FIGURE_BUILDERS: dict[str, tuple[str, Any]] = {
    "daily": (
        "calendar",
        lambda c, **kw: _build_daily_calendar(
            set(c.expected_keys),
            set(c.completed_keys),
            c.completed_key_runs,
            in_progress_keys=set(c.in_progress_keys or []),
            **kw,
        ),
    ),
    "weekly": (
        "week calendar",
        lambda c, **kw: _build_weekly_calendar(
            set(c.expected_keys),
            set(c.completed_keys),
            c.completed_key_runs,
            in_progress_keys=set(c.in_progress_keys or []),
            **kw,
        ),
    ),
    "monthly": (
        "month calendar",
        lambda c, **kw: _build_monthly_calendar(
            set(c.expected_keys),
            set(c.completed_keys),
            c.completed_key_runs,
            in_progress_keys=set(c.in_progress_keys or []),
            **kw,
        ),
    ),
    "hourly": (
        "hour calendar",
        lambda c, **kw: _build_hourly_calendar(
            set(c.expected_keys),
            set(c.completed_keys),
            c.completed_key_runs,
            in_progress_keys=set(c.in_progress_keys or []),
            **kw,
        ),
    ),
}


# ---------------------------------------------------------------------------
# Page: Overview
# ---------------------------------------------------------------------------


def _page_overview(
    overviews: list[JobOverview],
    coverages: dict[str, BackfillCoverage],
    workspace_url: str | None = None,
) -> Any:
    """Build the Overview page — KPI cards + unified job table.

    The table merges job stats and backfill coverage into a single
    view.  Job names link to the Databricks workspace when
    ``workspace_url`` is available; the Coverage column links to
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

    # Unified job table with workspace links and backfill coverage
    records = _overviews_to_records(
        overviews, coverages=coverages, workspace_url=workspace_url
    )

    column_defs = [
        {
            "field": "Job",
            "cellRenderer": "markdown",
            "linkTarget": "_blank",
            "minWidth": 180,
        },
        {"field": "Status", "cellStyle": _STATUS_CELL_STYLE, "maxWidth": 140},
        {"field": "Runs", "minWidth": 160},
        {"field": "Success Rate", "maxWidth": 120},
        {"field": "Avg Duration", "maxWidth": 120},
        {"field": "Coverage", "cellRenderer": "markdown", "maxWidth": 120},
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
        {"field": "Coverage", "minWidth": 140},
        {"field": "Errors", "maxWidth": 80},
        {"field": "Keys", "minWidth": 120},
    ]

    return html.Div(
        [
            html.H4("Backfill Coverage", className="mb-3"),
            html.P(
                "Coverage shows completed / due keys. "
                "Errors are keys with only failed runs. "
                "Click a job name for the coverage heatmap.",
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
# Page: Backfill Detail (coverage heatmap for a specific job)
# ---------------------------------------------------------------------------


def _page_backfill_detail(
    job_name: str,
    coverages: dict[str, BackfillCoverage],
    workspace_url: str | None = None,
    job_id: int | None = None,
) -> Any:
    """Build the Backfill Detail page — coverage heatmap for a job.

    Shows the backfill coverage chart with date-range picker for
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
            f"{cov.coverage_pct}% coverage "
            f"({len(cov.completed_keys)}/{len(cov.expected_keys)} keys)",
            className="text-muted",
        )
    )

    backfill_section: list[Any] = []

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
            in_progress_keys=set(cov.in_progress_keys or []),
        )
        if fig is not None:
            backfill_section.append(dcc.Graph(figure=fig))

    if cov.missing_keys:
        backfill_section.append(
            html.Details(
                [
                    html.Summary(
                        f"{len(cov.missing_keys)} missing keys",
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
                "\u2190 Back to Backfills",
                href="/backfills",
                color="link",
                className="mb-2 ps-0",
            ),
            *header_children,
            *backfill_section,
        ]
    )
