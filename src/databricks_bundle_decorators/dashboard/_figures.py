"""Plotly figure builders for the observability dashboard.

Calendar heatmaps, task DAG, and partition grid visualisations.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import whenever

from databricks_bundle_decorators.dashboard._compute import _effective_state
from databricks_bundle_decorators.dashboard._data import TaskRunInfo

# ---------------------------------------------------------------------------
# Coverage heatmap colorscale & legend
# ---------------------------------------------------------------------------

#: Discrete 3-state colorscale: 0=not-in-range, 1=not-launched, 2=completed.
_COVERAGE_COLORSCALE: list[list[object]] = [
    [0.0, "#f3f4f6"],
    [0.25, "#f3f4f6"],
    [0.25, "#f59e0b"],
    [0.75, "#f59e0b"],
    [0.75, "#22c55e"],
    [1.0, "#22c55e"],
]


def _add_coverage_legend(fig: Any) -> None:
    """Add a green/amber/gray legend to a coverage heatmap figure."""
    import plotly.graph_objects as go

    for label, color in [
        ("Completed", "#22c55e"),
        ("Not launched", "#f59e0b"),
        ("Not in range", "#f3f4f6"),
    ]:
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="markers",
                marker=dict(size=10, color=color, symbol="square"),
                name=label,
                showlegend=True,
            )
        )
    fig.update_layout(
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="left", x=0),
    )


#: Type alias for the per-key run info mapping.
_KeyRunInfo = dict[str, tuple[int, int | None]]


def _hover_completed(key: str, key_run_info: _KeyRunInfo | None) -> str:
    """Build hover text for a completed backfill cell.

    When run info is available, shows the run ID and timestamp of
    the most recent successful run targeting this key.
    """
    if key_run_info is not None and key in key_run_info:
        run_id, start_ms = key_run_info[key]
        if start_ms:
            from datetime import datetime, timezone

            dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
            ts = dt.strftime("%Y-%m-%d %H:%M UTC")
            return f"{key}: Completed<br>Run {run_id} \u00b7 {ts}"
        return f"{key}: Completed<br>Run {run_id}"
    return f"{key}: Completed"


# ---------------------------------------------------------------------------
# Calendar / partition heatmaps
# ---------------------------------------------------------------------------


def _build_daily_calendar(
    expected_keys: set[str],
    completed_keys: set[str],
    key_run_info: _KeyRunInfo | None = None,
) -> Any:
    """Build a Plotly heatmap calendar for daily backfill keys.

    Renders a GitHub-contribution-graph-style grid: rows are
    weekdays (Mon–Sun) and columns are weeks.
    """
    import plotly.graph_objects as go

    expected_dates: set[whenever.Date] = set()
    for key in expected_keys:
        try:
            expected_dates.add(whenever.Date.parse_iso(key))
        except ValueError:
            continue

    if not expected_dates:
        return None

    completed_dates: set[whenever.Date] = set()
    for key in completed_keys:
        try:
            completed_dates.add(whenever.Date.parse_iso(key))
        except ValueError:
            continue

    min_d = min(expected_dates)
    max_d = max(expected_dates)

    # Align to Monday/Sunday boundaries
    start = min_d.subtract(days=min_d.day_of_week().value - 1)
    end = max_d.add(days=7 - max_d.day_of_week().value)
    num_weeks = (end.py_date() - start.py_date()).days // 7 + 1

    z: list[list[int]] = [[0] * num_weeks for _ in range(7)]
    hover: list[list[str]] = [[""] * num_weeks for _ in range(7)]

    for week_idx in range(num_weeks):
        for dow in range(7):
            d = start.add(days=week_idx * 7 + dow)
            if d in expected_dates:
                key_str = d.format_iso()
                if d in completed_dates:
                    z[dow][week_idx] = 2
                    hover[dow][week_idx] = _hover_completed(key_str, key_run_info)
                else:
                    z[dow][week_idx] = 1
                    hover[dow][week_idx] = f"{key_str}: Not launched"
            else:
                hover[dow][week_idx] = d.format_iso()

    # Build month-boundary tick labels for x-axis
    month_ticks: list[int] = []
    month_labels: list[str] = []
    prev_ym: tuple[int, int] | None = None
    prev_year: int | None = None
    for week_idx in range(num_weeks):
        monday = start.add(days=week_idx * 7)
        ym = (monday.year, monday.month)
        if ym != prev_ym:
            prev_ym = ym
            month_ticks.append(week_idx)
            label = monday.py_date().strftime("%b")
            if monday.year != prev_year:
                label = monday.py_date().strftime("%b '%y")
                prev_year = monday.year
            month_labels.append(label)

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            hovertext=hover,
            hoverinfo="text",
            colorscale=_COVERAGE_COLORSCALE,
            zmin=0,
            zmax=2,
            showscale=False,
            xgap=2,
            ygap=2,
        )
    )
    xaxis_opts: dict[str, Any] = dict(tickvals=month_ticks, ticktext=month_labels)
    # Show at most ~26 weeks (6 months) initially; user can pan to see more
    _MAX_VISIBLE_WEEKS = 26
    if num_weeks > _MAX_VISIBLE_WEEKS:
        xaxis_opts["range"] = [num_weeks - _MAX_VISIBLE_WEEKS - 0.5, num_weeks - 0.5]

    fig.update_layout(
        height=230,
        margin=dict(l=50, r=20, t=10, b=60),
        yaxis=dict(
            tickvals=list(range(7)),
            ticktext=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            autorange="reversed",
        ),
        xaxis=xaxis_opts,
    )
    _add_coverage_legend(fig)
    return fig


def _build_weekly_calendar(
    expected_keys: set[str],
    completed_keys: set[str],
    key_run_info: _KeyRunInfo | None = None,
) -> Any:
    """Build a Plotly heatmap for weekly backfill keys.

    Renders a year x week grid (rows=years, columns=W01-W53).
    """
    import re

    import plotly.graph_objects as go

    week_re = re.compile(r"^(\d{4})-W(\d{2})$")

    expected: dict[tuple[int, int], str] = {}
    for key in expected_keys:
        m = week_re.match(key)
        if m:
            expected[(int(m.group(1)), int(m.group(2)))] = key

    if not expected:
        return None

    completed_parsed: set[tuple[int, int]] = set()
    for key in completed_keys:
        m = week_re.match(key)
        if m:
            completed_parsed.add((int(m.group(1)), int(m.group(2))))

    min_year = min(y for y, _ in expected)
    max_year = max(y for y, _ in expected)
    week_cols = max(max(w for _, w in expected), 52)

    years = list(range(min_year, max_year + 1))
    z: list[list[int]] = []
    hover: list[list[str]] = []

    for year in years:
        row_z: list[int] = []
        row_h: list[str] = []
        for w in range(1, week_cols + 1):
            if (year, w) in expected:
                key_str = expected[(year, w)]
                if (year, w) in completed_parsed:
                    row_z.append(2)
                    row_h.append(_hover_completed(key_str, key_run_info))
                else:
                    row_z.append(1)
                    row_h.append(f"{key_str}: Not launched")
            else:
                row_z.append(0)
                row_h.append(f"{year}-W{w:02d}")
        z.append(row_z)
        hover.append(row_h)

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=[f"W{w:02d}" for w in range(1, week_cols + 1)],
            y=[str(y) for y in years],
            hovertext=hover,
            hoverinfo="text",
            colorscale=_COVERAGE_COLORSCALE,
            zmin=0,
            zmax=2,
            showscale=False,
            xgap=1,
            ygap=2,
        )
    )
    fig.update_layout(
        height=max(150, len(years) * 50 + 80),
        margin=dict(l=50, r=20, t=10, b=60),
        xaxis=dict(dtick=4),
    )
    _add_coverage_legend(fig)
    return fig


def _build_monthly_calendar(
    expected_keys: set[str],
    completed_keys: set[str],
    key_run_info: _KeyRunInfo | None = None,
) -> Any:
    """Build a Plotly heatmap for monthly backfill keys.

    Renders a year x month grid (rows=years, columns=Jan-Dec).
    """
    import calendar as _calendar

    import plotly.graph_objects as go

    expected: dict[tuple[int, int], str] = {}
    for key in expected_keys:
        try:
            d = date.fromisoformat(key)
            expected[(d.year, d.month)] = key
        except ValueError:
            continue

    if not expected:
        return None

    completed_parsed: set[tuple[int, int]] = set()
    for key in completed_keys:
        try:
            d = date.fromisoformat(key)
            completed_parsed.add((d.year, d.month))
        except ValueError:
            continue

    min_year = min(y for y, _ in expected)
    max_year = max(y for y, _ in expected)
    years = list(range(min_year, max_year + 1))
    month_labels = [_calendar.month_abbr[m] for m in range(1, 13)]

    z: list[list[int]] = []
    hover: list[list[str]] = []

    for year in years:
        row_z: list[int] = []
        row_h: list[str] = []
        for m in range(1, 13):
            if (year, m) in expected:
                key_str = expected[(year, m)]
                if (year, m) in completed_parsed:
                    row_z.append(2)
                    row_h.append(_hover_completed(key_str, key_run_info))
                else:
                    row_z.append(1)
                    row_h.append(f"{key_str}: Not launched")
            else:
                row_z.append(0)
                row_h.append(f"{year}-{m:02d}")
        z.append(row_z)
        hover.append(row_h)

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=month_labels,
            y=[str(y) for y in years],
            hovertext=hover,
            hoverinfo="text",
            colorscale=_COVERAGE_COLORSCALE,
            zmin=0,
            zmax=2,
            showscale=False,
            xgap=3,
            ygap=3,
        )
    )
    fig.update_layout(
        height=max(150, len(years) * 50 + 80),
        margin=dict(l=50, r=20, t=10, b=60),
    )
    _add_coverage_legend(fig)
    return fig


def _build_hourly_calendar(
    expected_keys: set[str],
    completed_keys: set[str],
    key_run_info: _KeyRunInfo | None = None,
) -> Any:
    """Build a Plotly heatmap for hourly backfill keys.

    Renders a date x hour grid (rows=dates, columns=00-23).
    """
    from datetime import datetime as _dt

    import plotly.graph_objects as go

    _FMT = "%Y-%m-%dT%H"

    expected: dict[tuple[date, int], str] = {}
    for key in expected_keys:
        try:
            parsed = _dt.strptime(key, _FMT)
            expected[(parsed.date(), parsed.hour)] = key
        except ValueError:
            continue

    if not expected:
        return None

    completed_parsed: set[tuple[date, int]] = set()
    for key in completed_keys:
        try:
            parsed = _dt.strptime(key, _FMT)
            completed_parsed.add((parsed.date(), parsed.hour))
        except ValueError:
            continue

    all_days = sorted({d for d, _ in expected})

    z: list[list[int]] = []
    hover: list[list[str]] = []

    for day in all_days:
        row_z: list[int] = []
        row_h: list[str] = []
        for h in range(24):
            if (day, h) in expected:
                key_str = expected[(day, h)]
                if (day, h) in completed_parsed:
                    row_z.append(2)
                    row_h.append(_hover_completed(key_str, key_run_info))
                else:
                    row_z.append(1)
                    row_h.append(f"{key_str}: Not launched")
            else:
                row_z.append(0)
                row_h.append(f"{day.isoformat()}T{h:02d}")
        z.append(row_z)
        hover.append(row_h)

    y_labels = [d.isoformat() for d in all_days]

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=[f"{h:02d}" for h in range(24)],
            y=y_labels,
            hovertext=hover,
            hoverinfo="text",
            colorscale=_COVERAGE_COLORSCALE,
            zmin=0,
            zmax=2,
            showscale=False,
            xgap=1,
            ygap=1,
        )
    )

    # Show at most 14 days initially; user can pan/zoom to see more
    _MAX_VISIBLE_DAYS = 14
    yaxis_opts: dict[str, Any] = {}
    if len(all_days) > _MAX_VISIBLE_DAYS:
        yaxis_opts["range"] = [
            len(all_days) - _MAX_VISIBLE_DAYS - 0.5,
            len(all_days) - 0.5,
        ]

    fig.update_layout(
        height=max(150, min(len(all_days), _MAX_VISIBLE_DAYS) * 30 + 80),
        margin=dict(l=80, r=20, t=10, b=60),
        yaxis=yaxis_opts,
    )
    _add_coverage_legend(fig)
    return fig


def _build_partition_grid(
    expected_keys: list[str],
    completed_keys: set[str],
    key_run_info: _KeyRunInfo | None = None,
) -> Any:
    """Build a Plotly heatmap for static backfill keys.

    Renders a single-row grid with one cell per partition key.
    Used for `StaticBackfill` and any unknown backfill types.
    """
    import plotly.graph_objects as go

    if not expected_keys:
        return None

    z = [[2 if k in completed_keys else 1 for k in expected_keys]]
    hover = [
        [
            _hover_completed(k, key_run_info)
            if k in completed_keys
            else f"{k}: Not launched"
            for k in expected_keys
        ]
    ]

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=expected_keys,
            y=[""],
            hovertext=hover,
            hoverinfo="text",
            colorscale=_COVERAGE_COLORSCALE,
            zmin=0,
            zmax=2,
            showscale=False,
            xgap=3,
            ygap=3,
        )
    )

    # Show at most 50 keys initially; user can pan to see more
    _MAX_VISIBLE_KEYS = 50
    xaxis_opts: dict[str, Any] = {}
    if len(expected_keys) > _MAX_VISIBLE_KEYS:
        xaxis_opts["range"] = [-0.5, _MAX_VISIBLE_KEYS - 0.5]

    fig.update_layout(
        height=120,
        margin=dict(l=20, r=20, t=10, b=60),
        xaxis=xaxis_opts,
    )
    # Static grid only has completed/not-launched — no "not in range"
    for label, color in [("Completed", "#22c55e"), ("Not launched", "#f59e0b")]:
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="markers",
                marker=dict(size=10, color=color, symbol="square"),
                name=label,
                showlegend=True,
            )
        )
    fig.update_layout(
        legend=dict(orientation="h", yanchor="top", y=-0.3, xanchor="left", x=0),
    )
    return fig


# ---------------------------------------------------------------------------
# Task DAG figure
# ---------------------------------------------------------------------------


def _build_task_dag_figure(task_runs: list[TaskRunInfo]) -> Any:
    """Build a Plotly figure showing the task DAG for a single run.

    Renders a left-to-right layered graph using topological ordering.
    Nodes are colored by result state.
    """
    import plotly.graph_objects as go

    if not task_runs:
        return None

    # Build adjacency and compute layers via topological sort
    task_map = {t.task_key: t for t in task_runs}
    children: dict[str, list[str]] = {t.task_key: [] for t in task_runs}
    parents: dict[str, list[str]] = {t.task_key: list(t.depends_on) for t in task_runs}
    for t in task_runs:
        for dep in t.depends_on:
            if dep in children:
                children[dep].append(t.task_key)

    # Assign layers (longest path from any root)
    layers: dict[str, int] = {}
    visited: set[str] = set()

    def _assign_layer(key: str) -> int:
        if key in layers:
            return layers[key]
        if key in visited:
            return 0
        visited.add(key)
        if not parents[key]:
            layers[key] = 0
        else:
            layers[key] = (
                max(_assign_layer(p) for p in parents[key] if p in task_map) + 1
            )
        return layers[key]

    for t in task_runs:
        _assign_layer(t.task_key)

    # Position nodes: x by layer, y spread within layer
    max_layer = max(layers.values()) if layers else 0
    layer_groups: dict[int, list[str]] = {}
    for key, layer in layers.items():
        layer_groups.setdefault(layer, []).append(key)

    positions: dict[str, tuple[float, float]] = {}
    for layer, keys in layer_groups.items():
        x = layer / max(max_layer, 1)
        for i, key in enumerate(sorted(keys)):
            y = (i + 1) / (len(keys) + 1)
            positions[key] = (x, y)

    # Color map
    _STATE_COLORS = {
        "SUCCESS": "#22c55e",
        "FAILED": "#ef4444",
        "RUNNING": "#3b82f6",
        "PENDING": "#a3a3a3",
        "CANCELED": "#f59e0b",
        "TIMED_OUT": "#f59e0b",
        "INTERNAL_ERROR": "#ef4444",
        "SKIPPED": "#a3a3a3",
    }

    # Draw edges
    edge_x: list[float | None] = []
    edge_y: list[float | None] = []
    for t in task_runs:
        x1, y1 = positions[t.task_key]
        for dep in t.depends_on:
            if dep in positions:
                x0, y0 = positions[dep]
                edge_x.extend([x0, x1, None])
                edge_y.extend([y0, y1, None])

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=edge_x,
            y=edge_y,
            mode="lines",
            line=dict(width=1, color="#d1d5db"),
            hoverinfo="none",
            showlegend=False,
        )
    )

    # Draw nodes
    node_x = [positions[t.task_key][0] for t in task_runs]
    node_y = [positions[t.task_key][1] for t in task_runs]
    node_colors = [
        _STATE_COLORS.get(
            _effective_state(t.result_state, t.life_cycle_state), "#a3a3a3"
        )
        for t in task_runs
    ]
    node_text = [
        f"{t.task_key}<br>{_effective_state(t.result_state, t.life_cycle_state)}<br>{t.duration_seconds or 0}s"
        for t in task_runs
    ]
    node_labels = [t.task_key for t in task_runs]

    fig.add_trace(
        go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers+text",
            marker=dict(size=24, color=node_colors, line=dict(width=1, color="white")),
            text=node_labels,
            textposition="top center",
            hovertext=node_text,
            hoverinfo="text",
            showlegend=False,
        )
    )

    fig.update_layout(
        height=max(200, len(task_runs) * 40 + 80),
        margin=dict(l=20, r=20, t=10, b=10),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig
