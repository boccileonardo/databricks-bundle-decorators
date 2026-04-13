"""Plotly figure builders for the observability dashboard.

Calendar heatmaps and partition grid visualisations.
"""

from __future__ import annotations

import calendar as _calendar
from datetime import UTC, date, datetime
from typing import Any

import plotly.graph_objects as go
import whenever

from databricks_bundle_decorators.dashboard._compute import (
    _HOURLY_FMT,
    _WEEK_KEY_RE,
)
from databricks_bundle_decorators.dashboard._data import (
    COLOR_COMPLETED,
    COLOR_FAILED,
    COLOR_IN_PROGRESS,
    COLOR_MISSING,
    COLOR_NOT_IN_RANGE,
    COLOR_NOT_STARTED,
)

# ---------------------------------------------------------------------------
# Coverage heatmap colorscale & legend
# ---------------------------------------------------------------------------

#: Discrete 6-state colorscale:
#:   0=not-in-range, 1=missing, 2=completed,
#:   3=not started (future), 4=in progress, 5=failed.
#:
#: z values are integers 0-5 with zmin=0, zmax=5, so normalised positions
#: are 0, 0.2, 0.4, 0.6, 0.8, 1.  Boundaries sit at midpoints (0.1, 0.3,
#: 0.5, 0.7, 0.9) so each z value falls squarely inside its colour band.
_COVERAGE_COLORSCALE: list[list[object]] = [
    [0.0, COLOR_NOT_IN_RANGE],
    [0.1, COLOR_NOT_IN_RANGE],
    [0.1, COLOR_MISSING],
    [0.3, COLOR_MISSING],
    [0.3, COLOR_COMPLETED],
    [0.5, COLOR_COMPLETED],
    [0.5, COLOR_NOT_STARTED],
    [0.7, COLOR_NOT_STARTED],
    [0.7, COLOR_IN_PROGRESS],
    [0.9, COLOR_IN_PROGRESS],
    [0.9, COLOR_FAILED],
    [1.0, COLOR_FAILED],
]

#: Labels and colors for the full 6-state heatmap legend.
_LEGEND_ITEMS: list[tuple[str, str]] = [
    ("Completed", COLOR_COMPLETED),
    ("In progress", COLOR_IN_PROGRESS),
    ("Failed", COLOR_FAILED),
    ("Missing", COLOR_MISSING),
    ("Not started", COLOR_NOT_STARTED),
    ("Not in range", COLOR_NOT_IN_RANGE),
]

# ---------------------------------------------------------------------------
# Figure builder thresholds
# ---------------------------------------------------------------------------

_MAX_UNFILTERED_DAYS = 180
_DEFAULT_WINDOW_DAYS = 90
_MAX_UNFILTERED_WEEKS = 104
_DEFAULT_WINDOW_WEEKS = 52
_MAX_UNFILTERED_MONTHS = 48
_DEFAULT_WINDOW_MONTHS = 24
_MAX_UNFILTERED_HOURLY_DAYS = 14
_MAX_VISIBLE_KEYS = 50


def _add_coverage_legend(
    fig: Any,
    items: list[tuple[str, str]] | None = None,
) -> None:
    """Add a colored-square legend to a coverage heatmap figure.

    Parameters
    ----------
    fig:
        Plotly ``Figure`` to update.
    items:
        Optional (label, color) pairs.  Defaults to the full
        6-state legend.
    """
    for label, color in items or _LEGEND_ITEMS:
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="markers",
                marker={"size": 10, "color": color, "symbol": "square"},
                name=label,
                showlegend=True,
            )
        )
    fig.update_layout(
        legend={
            "orientation": "h",
            "yanchor": "top",
            "y": -0.15,
            "xanchor": "left",
            "x": 0,
            "itemclick": False,
            "itemdoubleclick": False,
        },
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
            dt = datetime.fromtimestamp(start_ms / 1000, tz=UTC)
            ts = dt.strftime("%Y-%m-%d %H:%M UTC")
            return f"{key}: Completed<br>Run {run_id} \u00b7 {ts}"
        return f"{key}: Completed<br>Run {run_id}"
    return f"{key}: Completed"


#: Z-values for the 6-state heatmap (indices into ``_COVERAGE_COLORSCALE``).
_Z_NOT_IN_RANGE = 0
_Z_MISSING = 1
_Z_COMPLETED = 2
_Z_NOT_STARTED = 3
_Z_IN_PROGRESS = 4
_Z_FAILED = 5


def _classify_cell(
    key_str: str,
    *,
    is_completed: bool,
    is_future: bool,
    in_progress_keys: set[str],
    errored_keys: set[str],
    key_run_info: _KeyRunInfo | None,
) -> tuple[int, str]:
    """Return ``(z_value, hover_text)`` for a single heatmap cell."""
    if is_completed:
        return _Z_COMPLETED, _hover_completed(key_str, key_run_info)
    if key_str in in_progress_keys:
        return _Z_IN_PROGRESS, f"{key_str}: In progress"
    if key_str in errored_keys:
        return _Z_FAILED, f"{key_str}: Failed"
    if is_future:
        return _Z_NOT_STARTED, f"{key_str}: Not started"
    return _Z_MISSING, f"{key_str}: Missing"


# ---------------------------------------------------------------------------
# Calendar / partition heatmaps
# ---------------------------------------------------------------------------


def _build_daily_calendar(
    expected_keys: set[str],
    completed_keys: set[str],
    key_run_info: _KeyRunInfo | None = None,
    *,
    errored_keys: set[str] | None = None,
    in_progress_keys: set[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    tz: str = "UTC",
) -> Any:
    """Build a Plotly heatmap calendar for daily backfill keys.

    Renders a GitHub-contribution-graph-style grid: rows are
    weekdays (Mon-Sun) and columns are weeks.
    """
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

    # Apply date-range filter
    if start_date is not None or end_date is not None:
        lo = whenever.Date(start_date) if start_date else min(expected_dates)
        hi = whenever.Date(end_date) if end_date else max(expected_dates)
        expected_dates = {d for d in expected_dates if lo <= d <= hi}
        completed_dates = {d for d in completed_dates if lo <= d <= hi}
    elif len(expected_dates) > _MAX_UNFILTERED_DAYS:
        sorted_all = sorted(expected_dates)
        cutoff = sorted_all[-_DEFAULT_WINDOW_DAYS]
        expected_dates = {d for d in expected_dates if d >= cutoff}
        completed_dates = {d for d in completed_dates if d >= cutoff}

    if not expected_dates:
        return None

    min_d = min(expected_dates)
    max_d = max(expected_dates)

    # Align to Monday/Sunday boundaries
    start = min_d.subtract(days=min_d.day_of_week().value - 1)
    end = max_d.add(days=7 - max_d.day_of_week().value)
    num_weeks = (end.to_stdlib() - start.to_stdlib()).days // 7 + 1

    z: list[list[int]] = [[0] * num_weeks for _ in range(7)]
    hover: list[list[str]] = [[""] * num_weeks for _ in range(7)]

    today_wd = whenever.ZonedDateTime.now(tz).date()
    _ip = in_progress_keys or set()
    _err = errored_keys or set()

    for week_idx in range(num_weeks):
        for dow in range(7):
            d = start.add(days=week_idx * 7 + dow)
            if d in expected_dates:
                key_str = d.format_iso()
                is_completed = d in completed_dates
                z[dow][week_idx], hover[dow][week_idx] = _classify_cell(
                    key_str,
                    is_completed=is_completed,
                    is_future=d > today_wd,
                    in_progress_keys=_ip,
                    errored_keys=_err,
                    key_run_info=key_run_info,
                )
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
            label = monday.format("MMM")
            if monday.year != prev_year:
                label = monday.format("MMM ''YY")
                prev_year = monday.year
            month_labels.append(label)

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            hovertext=hover,
            hoverinfo="text",
            colorscale=_COVERAGE_COLORSCALE,
            zmin=0,
            zmax=5,
            showscale=False,
            xgap=2,
            ygap=2,
        )
    )
    xaxis_opts: dict[str, Any] = {"tickvals": month_ticks, "ticktext": month_labels}

    fig.update_layout(
        height=230,
        margin={"l": 50, "r": 20, "t": 10, "b": 60},
        yaxis={
            "tickvals": list(range(7)),
            "ticktext": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            "autorange": "reversed",
        },
        xaxis=xaxis_opts,
    )
    _add_coverage_legend(fig)
    return fig


def _build_weekly_calendar(
    expected_keys: set[str],
    completed_keys: set[str],
    key_run_info: _KeyRunInfo | None = None,
    *,
    errored_keys: set[str] | None = None,
    in_progress_keys: set[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    tz: str = "UTC",
) -> Any:
    """Build a Plotly heatmap for weekly backfill keys.

    Renders a year x week grid (rows=years, columns=W01-W53).

    When ``start_date`` / ``end_date`` are given, only weeks whose
    Monday falls in that range are shown.  Defaults to the most
    recent 52 weeks when the total span exceeds 104 weeks.
    """
    expected: dict[tuple[int, int], str] = {}
    for key in expected_keys:
        m = _WEEK_KEY_RE.match(key)
        if m:
            expected[(int(m.group(1)), int(m.group(2)))] = key

    if not expected:
        return None

    completed_parsed: set[tuple[int, int]] = set()
    for key in completed_keys:
        m = _WEEK_KEY_RE.match(key)
        if m:
            completed_parsed.add((int(m.group(1)), int(m.group(2))))

    # Apply date-range filter (uses the Monday of each ISO week)
    if start_date is not None or end_date is not None:
        min_yw = min(expected)
        max_yw = max(expected)
        lo = start_date or date.fromisocalendar(min_yw[0], min_yw[1], 1)
        hi = end_date or date.fromisocalendar(max_yw[0], max_yw[1], 1)
        expected = {
            k: v
            for k, v in expected.items()
            if lo <= date.fromisocalendar(k[0], k[1], 1) <= hi
        }
        completed_parsed = {
            k
            for k in completed_parsed
            if lo <= date.fromisocalendar(k[0], k[1], 1) <= hi
        }
    elif len(expected) > _MAX_UNFILTERED_WEEKS:
        sorted_keys = sorted(expected)
        keep = set(sorted_keys[-_DEFAULT_WINDOW_WEEKS:])
        expected = {k: v for k, v in expected.items() if k in keep}
        completed_parsed = {k for k in completed_parsed if k in keep}

    if not expected:
        return None

    min_year = min(y for y, _ in expected)
    max_year = max(y for y, _ in expected)
    week_cols = max(*(w for _, w in expected), 52)

    years = list(range(min_year, max_year + 1))
    z: list[list[int]] = []
    hover: list[list[str]] = []

    today_iwd = whenever.ZonedDateTime.now(tz).date().iso_week_date()
    today_yw = (today_iwd.year, today_iwd.week)
    _ip = in_progress_keys or set()
    _err = errored_keys or set()

    for year in years:
        row_z: list[int] = []
        row_h: list[str] = []
        for w in range(1, week_cols + 1):
            if (year, w) in expected:
                key_str = expected[(year, w)]
                z_val, h_val = _classify_cell(
                    key_str,
                    is_completed=(year, w) in completed_parsed,
                    is_future=(year, w) > today_yw,
                    in_progress_keys=_ip,
                    errored_keys=_err,
                    key_run_info=key_run_info,
                )
                row_z.append(z_val)
                row_h.append(h_val)
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
            zmax=5,
            showscale=False,
            xgap=1,
            ygap=2,
        )
    )
    fig.update_layout(
        height=max(150, len(years) * 50 + 80),
        margin={"l": 50, "r": 20, "t": 10, "b": 60},
        xaxis={"dtick": 4},
    )
    _add_coverage_legend(fig)
    return fig


def _build_monthly_calendar(
    expected_keys: set[str],
    completed_keys: set[str],
    key_run_info: _KeyRunInfo | None = None,
    *,
    errored_keys: set[str] | None = None,
    in_progress_keys: set[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    tz: str = "UTC",
) -> Any:
    """Build a Plotly heatmap for monthly backfill keys.

    Renders a year x month grid (rows=years, columns=Jan-Dec).

    When ``start_date`` / ``end_date`` are given, only months whose
    first day falls in that range are shown.  Defaults to the most
    recent 24 months when the total span exceeds 48 months.
    """
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

    # Apply date-range filter
    if start_date is not None or end_date is not None:
        lo = start_date or date(min(expected)[0], min(expected)[1], 1)
        hi = end_date or date(max(expected)[0], max(expected)[1], 1)
        expected = {
            k: v for k, v in expected.items() if lo <= date(k[0], k[1], 1) <= hi
        }
        completed_parsed = {
            k for k in completed_parsed if lo <= date(k[0], k[1], 1) <= hi
        }
    elif len(expected) > _MAX_UNFILTERED_MONTHS:
        sorted_keys = sorted(expected)
        keep = set(sorted_keys[-_DEFAULT_WINDOW_MONTHS:])
        expected = {k: v for k, v in expected.items() if k in keep}
        completed_parsed = {k for k in completed_parsed if k in keep}

    if not expected:
        return None

    min_year = min(y for y, _ in expected)
    max_year = max(y for y, _ in expected)
    years = list(range(min_year, max_year + 1))
    month_labels = [_calendar.month_abbr[m] for m in range(1, 13)]

    z: list[list[int]] = []
    hover: list[list[str]] = []

    today = whenever.ZonedDateTime.now(tz).date()
    today_ym = (today.year, today.month)
    _ip = in_progress_keys or set()
    _err = errored_keys or set()

    for year in years:
        row_z: list[int] = []
        row_h: list[str] = []
        for m in range(1, 13):
            if (year, m) in expected:
                key_str = expected[(year, m)]
                z_val, h_val = _classify_cell(
                    key_str,
                    is_completed=(year, m) in completed_parsed,
                    is_future=(year, m) > today_ym,
                    in_progress_keys=_ip,
                    errored_keys=_err,
                    key_run_info=key_run_info,
                )
                row_z.append(z_val)
                row_h.append(h_val)
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
            zmax=5,
            showscale=False,
            xgap=3,
            ygap=3,
        )
    )
    fig.update_layout(
        height=max(150, len(years) * 50 + 80),
        margin={"l": 50, "r": 20, "t": 10, "b": 60},
    )
    _add_coverage_legend(fig)
    return fig


def _build_hourly_calendar(
    expected_keys: set[str],
    completed_keys: set[str],
    key_run_info: _KeyRunInfo | None = None,
    *,
    errored_keys: set[str] | None = None,
    in_progress_keys: set[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    tz: str = "UTC",
) -> Any:
    """Build a Plotly heatmap for hourly backfill keys.

    Renders a date x hour grid (rows=dates, columns=00-23).

    When ``start_date`` / ``end_date`` are given, only days in that
    range are rendered.  Defaults to the most recent 7 days of data
    when the total span exceeds 14 days.
    """
    expected: dict[tuple[date, int], str] = {}
    for key in expected_keys:
        try:
            parsed = datetime.strptime(key, _HOURLY_FMT)  # noqa: DTZ007
            expected[(parsed.date(), parsed.hour)] = key
        except ValueError:
            continue

    if not expected:
        return None

    completed_parsed: set[tuple[date, int]] = set()
    for key in completed_keys:
        try:
            parsed = datetime.strptime(key, _HOURLY_FMT)  # noqa: DTZ007
            completed_parsed.add((parsed.date(), parsed.hour))
        except ValueError:
            continue

    all_days = sorted({d for d, _ in expected})

    # Apply date range filter
    if start_date is not None or end_date is not None:
        lo = start_date or all_days[0]
        hi = end_date or all_days[-1]
        all_days = [d for d in all_days if lo <= d <= hi]
    elif len(all_days) > _MAX_UNFILTERED_HOURLY_DAYS:
        # Default: show last 7 days of data
        all_days = all_days[-7:]

    if not all_days:
        return None

    z: list[list[int]] = []
    hover: list[list[str]] = []

    now_zdt = whenever.ZonedDateTime.now(tz)
    _ip = in_progress_keys or set()
    _err = errored_keys or set()

    for day in all_days:
        row_z: list[int] = []
        row_h: list[str] = []
        for h in range(24):
            if (day, h) in expected:
                key_str = expected[(day, h)]
                z_val, h_val = _classify_cell(
                    key_str,
                    is_completed=(day, h) in completed_parsed,
                    is_future=(
                        whenever.ZonedDateTime(day.year, day.month, day.day, h, tz=tz)
                        > now_zdt
                    ),
                    in_progress_keys=_ip,
                    errored_keys=_err,
                    key_run_info=key_run_info,
                )
                row_z.append(z_val)
                row_h.append(h_val)
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
            zmax=5,
            showscale=False,
            xgap=1,
            ygap=1,
        )
    )

    fig.update_layout(
        height=max(150, len(all_days) * 30 + 80),
        margin={"l": 80, "r": 20, "t": 10, "b": 60},
    )
    _add_coverage_legend(fig)
    return fig


def _build_partition_grid(
    expected_keys: list[str],
    completed_keys: set[str],
    key_run_info: _KeyRunInfo | None = None,
    *,
    errored_keys: set[str] | None = None,
    in_progress_keys: set[str] | None = None,
) -> Any:
    """Build a Plotly heatmap for static backfill keys.

    Renders a single-row grid with one cell per partition key.
    Used for `StaticBackfill` and any unknown backfill types.
    """
    if not expected_keys:
        return None

    _ip = in_progress_keys or set()
    _err = errored_keys or set()

    def _cell_z(k: str) -> int:
        if k in completed_keys:
            return _Z_COMPLETED
        if k in _ip:
            return _Z_IN_PROGRESS
        if k in _err:
            return _Z_FAILED
        return _Z_MISSING

    def _cell_hover(k: str) -> str:
        if k in completed_keys:
            return _hover_completed(k, key_run_info)
        if k in _ip:
            return f"{k}: In progress"
        if k in _err:
            return f"{k}: Failed"
        return f"{k}: Missing"

    z = [[_cell_z(k) for k in expected_keys]]
    hover = [[_cell_hover(k) for k in expected_keys]]

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=expected_keys,
            y=[""],
            hovertext=hover,
            hoverinfo="text",
            colorscale=_COVERAGE_COLORSCALE,
            zmin=0,
            zmax=5,
            showscale=False,
            xgap=3,
            ygap=3,
        )
    )

    # Show at most 50 keys initially; user can pan to see more
    xaxis_opts: dict[str, Any] = {}
    if len(expected_keys) > _MAX_VISIBLE_KEYS:
        xaxis_opts["range"] = [-0.5, _MAX_VISIBLE_KEYS - 0.5]

    fig.update_layout(
        height=120,
        margin={"l": 20, "r": 20, "t": 10, "b": 60},
        xaxis=xaxis_opts,
    )
    # Static grid only has completed/in-progress/failed/missing
    _add_coverage_legend(
        fig,
        items=[
            ("Completed", COLOR_COMPLETED),
            ("In progress", COLOR_IN_PROGRESS),
            ("Failed", COLOR_FAILED),
            ("Missing", COLOR_MISSING),
        ],
    )
    fig.update_layout(
        legend={
            "orientation": "h",
            "yanchor": "top",
            "y": -0.3,
            "xanchor": "left",
            "x": 0,
            "itemclick": False,
            "itemdoubleclick": False,
        },
    )
    return fig
