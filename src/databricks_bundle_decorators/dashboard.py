"""Observability dashboard for framework-managed pipeline jobs.

Uses the **Databricks CLI** to fetch job/task execution data.  The CLI
inherits the same unified credentials used for ``databricks bundle
deploy`` — no additional auth configuration needed.

The dashboard is **bundle-scoped**: only jobs deployed from the current
bundle are shown.

Install the optional dependency::

    uv add databricks-bundle-decorators[observability]

Launch the dashboard::

    dbxdec dashboard

Data functions can also be used programmatically::

    from databricks_bundle_decorators.dashboard import (
        fetch_job_runs,
        compute_backfill_coverage,
    )

    runs = fetch_job_runs(12345, profile="work")
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any

import whenever


# ---------------------------------------------------------------------------
# Data classes (stdlib only — no external dependencies)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunInfo:
    """Summary of a single job run from the Jobs API."""

    run_id: int
    result_state: str | None
    start_time_ms: int | None
    end_time_ms: int | None
    duration_seconds: float | None
    backfill_key: str | None = None
    life_cycle_state: str | None = None
    state_message: str | None = None


@dataclass(frozen=True)
class TaskRunInfo:
    """Summary of a single task run within a job run."""

    task_key: str
    result_state: str | None
    start_time_ms: int | None
    end_time_ms: int | None
    duration_seconds: float | None
    depends_on: tuple[str, ...] = ()
    life_cycle_state: str | None = None
    state_message: str | None = None


@dataclass
class JobOverview:
    """Aggregated stats for a job over recent runs."""

    job_name: str
    job_id: int | None = None
    total_runs: int = 0
    successes: int = 0
    failures: int = 0
    last_run_time_ms: int | None = None
    last_run_state: str | None = None
    avg_duration_seconds: float | None = None
    has_backfill: bool = False


@dataclass(frozen=True)
class BackfillCoverage:
    """Expected-vs-actual backfill key comparison.

    Uses exact key-level matching from run parameters — not
    approximate counts like system table queries would give.
    """

    job_name: str
    expected_keys: list[str]
    completed_keys: list[str]
    missing_keys: list[str]
    coverage_pct: float
    kind: str = "static"


# ---------------------------------------------------------------------------
# Bundle integration
# ---------------------------------------------------------------------------


def resolve_job_ids(
    *,
    target: str | None = None,
    profile: str | None = None,
) -> dict[str, int]:
    """Resolve registered job names to deployed Databricks job IDs.

    Shells out to ``databricks bundle summary`` to read the mapping
    from bundle deployment state.  Only jobs from **this bundle** are
    returned — workspace jobs outside the bundle are excluded.

    Must be run from a directory that contains ``databricks.yaml``.

    Parameters
    ----------
    target:
        Bundle target (e.g. ``dev``, ``prod``).
    profile:
        Databricks CLI profile name.

    Returns
    -------
    dict[str, int]
        Mapping of job name to numeric job ID.  Empty if the CLI
        is unavailable or the command fails.
    """
    if shutil.which("databricks") is None:
        print(
            "Warning: 'databricks' CLI not found on PATH.",
            file=sys.stderr,
        )
        return {}

    cmd: list[str] = ["databricks", "bundle", "summary", "--output", "json"]
    if target:
        cmd += ["--target", target]
    if profile:
        cmd += ["--profile", profile]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        err = result.stderr.strip() or result.stdout.strip()
        print(
            f"Warning: 'databricks bundle summary' failed: {err}",
            file=sys.stderr,
        )
        return {}

    summary = json.loads(result.stdout)
    jobs: dict[str, Any] = summary.get("resources", {}).get("jobs", {})
    mapping: dict[str, int] = {}
    for name, info in jobs.items():
        job_id = info.get("id")
        if job_id:
            mapping[name] = int(job_id)
    return mapping


# ---------------------------------------------------------------------------
# Data fetching (uses Databricks CLI — same creds as bundle deploy)
# ---------------------------------------------------------------------------


def fetch_job_runs(
    job_id: int,
    *,
    profile: str | None = None,
) -> list[RunInfo]:
    """Fetch recent runs for a job via the Databricks CLI.

    Uses ``databricks jobs list-runs`` with the same credential
    handling as ``databricks bundle deploy``.

    Parameters
    ----------
    job_id:
        Numeric Databricks job ID.
    profile:
        Databricks CLI profile name.
    """
    cmd: list[str] = [
        "databricks",
        "jobs",
        "list-runs",
        "--job-id",
        str(job_id),
        "--output",
        "json",
    ]
    if profile:
        cmd += ["--profile", profile]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return []

    runs_data: list[dict[str, Any]] = json.loads(result.stdout)
    runs: list[RunInfo] = []
    for run in runs_data:
        state = run.get("state", {})
        result_state = state.get("result_state")
        life_cycle_state = state.get("life_cycle_state")
        state_message = state.get("state_message") or None

        start_ms = run.get("start_time")
        end_ms = run.get("end_time")
        duration = None
        if start_ms and end_ms:
            duration = round((end_ms - start_ms) / 1000.0, 1)

        backfill_key = None
        for param in run.get("job_parameters", []):
            if param.get("name") == "backfill_key":
                backfill_key = param["value"]
                break

        runs.append(
            RunInfo(
                run_id=run["run_id"],
                result_state=result_state,
                start_time_ms=start_ms,
                end_time_ms=end_ms,
                duration_seconds=duration,
                backfill_key=backfill_key,
                life_cycle_state=life_cycle_state,
                state_message=state_message,
            )
        )
    return runs


def fetch_task_runs(
    run_id: int,
    *,
    profile: str | None = None,
) -> list[TaskRunInfo]:
    """Fetch task-level details for a specific job run via the CLI.

    Uses ``databricks jobs get-run`` with the same credential
    handling as ``databricks bundle deploy``.

    Parameters
    ----------
    run_id:
        The job run ID to inspect.
    profile:
        Databricks CLI profile name.
    """
    cmd: list[str] = [
        "databricks",
        "jobs",
        "get-run",
        str(run_id),
        "--output",
        "json",
    ]
    if profile:
        cmd += ["--profile", profile]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return []

    run_data: dict[str, Any] = json.loads(result.stdout)
    tasks: list[TaskRunInfo] = []
    for task in run_data.get("tasks", []):
        state = task.get("state", {})
        result_state = state.get("result_state")
        life_cycle_state = state.get("life_cycle_state")
        state_message = state.get("state_message") or None
        start_ms = task.get("start_time")
        end_ms = task.get("end_time")
        duration = None
        if start_ms and end_ms:
            duration = round((end_ms - start_ms) / 1000.0, 1)
        deps = tuple(
            d["task_key"] for d in task.get("depends_on", []) if "task_key" in d
        )
        tasks.append(
            TaskRunInfo(
                task_key=task["task_key"],
                result_state=result_state,
                start_time_ms=start_ms,
                end_time_ms=end_ms,
                duration_seconds=duration,
                depends_on=deps,
                life_cycle_state=life_cycle_state,
                state_message=state_message,
            )
        )
    return tasks


# ---------------------------------------------------------------------------
# Pure computation (no I/O — easy to test)
# ---------------------------------------------------------------------------

#: Terminal ``life_cycle_state`` values that indicate an error when
#: there is no ``result_state``.
_ERROR_LIFECYCLE_STATES = frozenset(
    {
        "INTERNAL_ERROR",
        "SKIPPED",
    }
)

#: ``life_cycle_state`` values that indicate the run is still in progress.
_ACTIVE_LIFECYCLE_STATES = frozenset(
    {
        "PENDING",
        "RUNNING",
        "TERMINATING",
        "BLOCKED",
        "WAITING_FOR_RETRY",
    }
)


def _effective_state(
    result_state: str | None,
    life_cycle_state: str | None,
) -> str:
    """Return the best display state for a run or task.

    Prefers ``result_state`` when set.  Falls back to
    ``life_cycle_state`` which captures infrastructure errors
    (``INTERNAL_ERROR``, ``SKIPPED``) that never produce a
    ``result_state``.
    """
    if result_state is not None:
        return result_state
    if life_cycle_state is not None:
        return life_cycle_state
    return "UNKNOWN"


def _is_terminal_failure(
    result_state: str | None,
    life_cycle_state: str | None,
) -> bool:
    """Return True if the run/task ended in a failure state."""
    if result_state is not None:
        return result_state != "SUCCESS"
    return life_cycle_state in _ERROR_LIFECYCLE_STATES


def build_job_overview(
    job_name: str,
    job_id: int | None,
    runs: list[RunInfo],
    *,
    has_backfill: bool = False,
) -> JobOverview:
    """Compute aggregated job stats from a list of runs.

    This is a pure function — no API calls.
    """
    if not runs:
        return JobOverview(job_name=job_name, job_id=job_id, has_backfill=has_backfill)

    successes = sum(1 for r in runs if r.result_state == "SUCCESS")
    failures = sum(
        1 for r in runs if _is_terminal_failure(r.result_state, r.life_cycle_state)
    )
    durations = [r.duration_seconds for r in runs if r.duration_seconds is not None]

    most_recent = runs[0]

    return JobOverview(
        job_name=job_name,
        job_id=job_id,
        total_runs=len(runs),
        successes=successes,
        failures=failures,
        last_run_time_ms=most_recent.start_time_ms,
        last_run_state=_effective_state(
            most_recent.result_state, most_recent.life_cycle_state
        ),
        avg_duration_seconds=(
            round(sum(durations) / len(durations), 1) if durations else None
        ),
        has_backfill=has_backfill,
    )


def _filter_past_keys(keys: list[str], kind: str) -> list[str]:
    """Remove keys that represent future time periods.

    For time-based backfills, keys representing periods that have
    not yet completed are excluded.  Static backfills are returned
    unchanged.
    """
    from datetime import datetime as _dt

    today = whenever.ZonedDateTime.now("UTC").date()

    if kind == "daily":
        cutoff = today.subtract(days=1)  # yesterday is the last complete day
        result: list[str] = []
        for k in keys:
            try:
                d = whenever.Date.parse_iso(k)
            except ValueError:
                result.append(k)
                continue
            if d <= cutoff:
                result.append(k)
        return result

    if kind == "weekly":
        import re

        week_re = re.compile(r"^(\d{4})-W(\d{2})$")
        # Current ISO week: consider last week as the last complete one
        cur_iso = today.py_date().isocalendar()
        cutoff_year, cutoff_week = cur_iso[0], cur_iso[1] - 1
        if cutoff_week < 1:
            cutoff_year -= 1
            cutoff_week = 52
        result = []
        for k in keys:
            m = week_re.match(k)
            if not m:
                result.append(k)
                continue
            y, w = int(m.group(1)), int(m.group(2))
            if (y, w) <= (cutoff_year, cutoff_week):
                result.append(k)
        return result

    if kind == "monthly":
        # Last complete month
        first_of_month = today.py_date().replace(day=1)
        result = []
        for k in keys:
            try:
                d = date.fromisoformat(k)
            except ValueError:
                result.append(k)
                continue
            if d < first_of_month:
                result.append(k)
        return result

    if kind == "hourly":
        fmt = "%Y-%m-%dT%H"
        now_utc = whenever.ZonedDateTime.now("UTC")
        cutoff_str = now_utc.subtract(hours=1).py_datetime().strftime(fmt)
        result = []
        for k in keys:
            try:
                _dt.strptime(k, fmt)  # validate format
            except ValueError:
                result.append(k)
                continue
            if k <= cutoff_str:
                result.append(k)
        return result

    return keys


def compute_backfill_coverage(
    job_name: str,
    runs: list[RunInfo],
    expected_keys: list[str],
    *,
    kind: str = "static",
) -> BackfillCoverage:
    """Compute backfill coverage by matching run parameters to expected keys.

    Compares the ``backfill_key`` parameter of **successful** runs
    against the expected keys from the job's `BackfillDef`.  This
    gives **exact key-level matching** — unlike the approximate
    count-based approach that system tables would provide.

    Future keys (time periods that have not completed yet) are
    automatically excluded so they are not counted as missing.

    This is a pure function — no API calls.

    Parameters
    ----------
    job_name:
        Name of the job.
    runs:
        List of run info objects.
    expected_keys:
        All expected backfill keys from the ``BackfillDef``.
    kind:
        Backfill type: ``"daily"``, ``"weekly"``, ``"monthly"``,
        ``"hourly"``, or ``"static"``.
    """
    # Filter out future keys so they aren't counted as missing
    due_keys = _filter_past_keys(expected_keys, kind)

    completed = {
        r.backfill_key
        for r in runs
        if r.result_state == "SUCCESS" and r.backfill_key is not None
    }
    due_set = set(due_keys)
    completed_list = sorted(due_set & completed)
    missing = sorted(due_set - completed)
    pct = round(len(completed_list) / len(due_keys) * 100, 1) if due_keys else 0.0
    return BackfillCoverage(
        job_name=job_name,
        expected_keys=expected_keys,
        completed_keys=completed_list,
        missing_keys=missing,
        coverage_pct=pct,
        kind=kind,
    )


def _backfill_kind(backfill: Any) -> str:
    """Map a ``BackfillDef`` subclass to a kind string."""
    from databricks_bundle_decorators.backfill import (
        DailyBackfill,
        HourlyBackfill,
        MonthlyBackfill,
        WeeklyBackfill,
    )

    if isinstance(backfill, DailyBackfill):
        return "daily"
    if isinstance(backfill, WeeklyBackfill):
        return "weekly"
    if isinstance(backfill, MonthlyBackfill):
        return "monthly"
    if isinstance(backfill, HourlyBackfill):
        return "hourly"
    return "static"


# ---------------------------------------------------------------------------
# Calendar & partition visualization (Plotly heatmaps)
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
    """Add a green/red/gray legend to a coverage heatmap figure."""
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


def _build_daily_calendar(
    expected_keys: set[str],
    completed_keys: set[str],
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
    # whenever: day_of_week().value is 1=Monday..7=Sunday
    start = min_d.subtract(days=min_d.day_of_week().value - 1)
    end = max_d.add(days=7 - max_d.day_of_week().value)
    num_weeks = (end.py_date() - start.py_date()).days // 7 + 1

    z: list[list[int]] = [[0] * num_weeks for _ in range(7)]
    hover: list[list[str]] = [[""] * num_weeks for _ in range(7)]

    for week_idx in range(num_weeks):
        for dow in range(7):
            d = start.add(days=week_idx * 7 + dow)
            if d in expected_dates:
                if d in completed_dates:
                    z[dow][week_idx] = 2
                    hover[dow][week_idx] = f"{d.format_iso()}: Completed"
                else:
                    z[dow][week_idx] = 1
                    hover[dow][week_idx] = f"{d.format_iso()}: Not launched"
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
    fig.update_layout(
        height=230,
        margin=dict(l=50, r=20, t=10, b=60),
        yaxis=dict(
            tickvals=list(range(7)),
            ticktext=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            autorange="reversed",
        ),
        xaxis=dict(tickvals=month_ticks, ticktext=month_labels),
    )
    _add_coverage_legend(fig)
    return fig


def _build_weekly_calendar(
    expected_keys: set[str],
    completed_keys: set[str],
) -> Any:
    """Build a Plotly heatmap for weekly backfill keys.

    Renders a year × week grid (rows=years, columns=W01–W53).
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
                if (year, w) in completed_parsed:
                    row_z.append(2)
                    row_h.append(f"{expected[(year, w)]}: Completed")
                else:
                    row_z.append(1)
                    row_h.append(f"{expected[(year, w)]}: Not launched")
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
) -> Any:
    """Build a Plotly heatmap for monthly backfill keys.

    Renders a year × month grid (rows=years, columns=Jan–Dec).
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
                if (year, m) in completed_parsed:
                    row_z.append(2)
                    row_h.append(f"{expected[(year, m)]}: Completed")
                else:
                    row_z.append(1)
                    row_h.append(f"{expected[(year, m)]}: Not launched")
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
) -> Any:
    """Build a Plotly heatmap for hourly backfill keys.

    Renders a date × hour grid (rows=dates, columns=00–23).
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
                if (day, h) in completed_parsed:
                    row_z.append(2)
                    row_h.append(f"{expected[(day, h)]}: Completed")
                else:
                    row_z.append(1)
                    row_h.append(f"{expected[(day, h)]}: Not launched")
            else:
                row_z.append(0)
                row_h.append(f"{day.isoformat()}T{h:02d}")
        z.append(row_z)
        hover.append(row_h)

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=[f"{h:02d}" for h in range(24)],
            y=[d.isoformat() for d in all_days],
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
    fig.update_layout(
        height=max(150, len(all_days) * 30 + 80),
        margin=dict(l=80, r=20, t=10, b=60),
    )
    _add_coverage_legend(fig)
    return fig


def _build_partition_grid(
    expected_keys: list[str],
    completed_keys: set[str],
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
            f"{k}: {'Completed' if k in completed_keys else 'Not launched'}"
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
    fig.update_layout(
        height=120,
        margin=dict(l=20, r=20, t=10, b=60),
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


# ---------------------------------------------------------------------------
# Polars data helpers — convert dataclasses to DataFrames for display
# ---------------------------------------------------------------------------


def _overviews_to_records(overviews: list[JobOverview]) -> list[dict[str, Any]]:
    """Convert job overviews to display-ready table records via polars."""
    import polars as pl

    if not overviews:
        return []

    df = pl.DataFrame(
        {
            "Job": [o.job_name for o in overviews],
            "Deployed": [o.job_id is not None for o in overviews],
            "Runs": [o.total_runs for o in overviews],
            "Pass": [o.successes for o in overviews],
            "Fail": [o.failures for o in overviews],
            "Last Run": [o.last_run_time_ms for o in overviews],
            "Status": [o.last_run_state for o in overviews],
            "Avg Duration (s)": [o.avg_duration_seconds for o in overviews],
            "Backfill": [o.has_backfill for o in overviews],
        }
    )
    df = df.with_columns(
        pl.when(pl.col("Runs") > 0)
        .then((pl.col("Pass") / pl.col("Runs") * 100).round(0).cast(pl.Utf8) + "%")
        .otherwise(pl.lit("\u2014"))
        .alias("Rate"),
        pl.when(pl.col("Last Run").is_not_null())
        .then(
            pl.from_epoch(pl.col("Last Run"), time_unit="ms").dt.to_string(
                "%Y-%m-%d %H:%M UTC"
            )
        )
        .otherwise(pl.lit("\u2014"))
        .alias("Last Run"),
        pl.when(pl.col("Avg Duration (s)").is_not_null())
        .then(pl.col("Avg Duration (s)").cast(pl.Utf8))
        .otherwise(pl.lit("\u2014"))
        .alias("Avg Duration (s)"),
        pl.when(pl.col("Status").is_not_null())
        .then(pl.col("Status"))
        .otherwise(pl.lit("\u2014"))
        .alias("Status"),
        pl.when(pl.col("Deployed"))
        .then(pl.lit("\u2713"))
        .otherwise(pl.lit("\u2717"))
        .alias("Deployed"),
        pl.when(pl.col("Backfill"))
        .then(pl.lit("\u2713"))
        .otherwise(pl.lit(""))
        .alias("Backfill"),
    )
    return df.select(
        "Job",
        "Deployed",
        "Runs",
        "Pass",
        "Fail",
        "Rate",
        "Last Run",
        "Status",
        "Avg Duration (s)",
        "Backfill",
    ).to_dicts()


def _runs_to_records(runs: list[RunInfo]) -> list[dict[str, Any]]:
    """Convert run info list to display-ready table records via polars."""
    import polars as pl

    if not runs:
        return []

    df = pl.DataFrame(
        {
            "Run ID": [r.run_id for r in runs],
            "Status": [
                _effective_state(r.result_state, r.life_cycle_state) for r in runs
            ],
            "Start": [r.start_time_ms for r in runs],
            "Duration (s)": [r.duration_seconds for r in runs],
            "Backfill Key": [r.backfill_key or "" for r in runs],
        }
    )
    df = df.with_columns(
        pl.when(pl.col("Start").is_not_null())
        .then(
            pl.from_epoch(pl.col("Start"), time_unit="ms").dt.to_string(
                "%Y-%m-%d %H:%M"
            )
        )
        .otherwise(pl.lit("\u2014"))
        .alias("Start"),
        pl.when(pl.col("Duration (s)").is_not_null())
        .then(pl.col("Duration (s)").cast(pl.Utf8))
        .otherwise(pl.lit("\u2014"))
        .alias("Duration (s)"),
    )
    return df.to_dicts()


def _tasks_to_records(task_runs: list[TaskRunInfo]) -> list[dict[str, Any]]:
    """Convert task run info list to display-ready table records via polars."""
    import polars as pl

    if not task_runs:
        return []

    df = pl.DataFrame(
        {
            "Task": [t.task_key for t in task_runs],
            "Status": [
                _effective_state(t.result_state, t.life_cycle_state) for t in task_runs
            ],
            "Duration (s)": [t.duration_seconds for t in task_runs],
            "Error": [t.state_message or "" for t in task_runs],
        }
    )
    df = df.with_columns(
        pl.when(pl.col("Duration (s)").is_not_null())
        .then(pl.col("Duration (s)").cast(pl.Utf8))
        .otherwise(pl.lit("\u2014"))
        .alias("Duration (s)"),
    )
    return df.to_dicts()


def _coverages_to_records(
    coverages: dict[str, BackfillCoverage],
) -> list[dict[str, Any]]:
    """Convert backfill coverages to display-ready table records via polars."""
    import polars as pl

    if not coverages:
        return []

    sorted_covs = sorted(coverages.values(), key=lambda c: c.coverage_pct)
    df = pl.DataFrame(
        {
            "Job": [c.job_name for c in sorted_covs],
            "Type": [c.kind.title() for c in sorted_covs],
            "Expected": [len(c.expected_keys) for c in sorted_covs],
            "Completed": [len(c.completed_keys) for c in sorted_covs],
            "Missing": [len(c.missing_keys) for c in sorted_covs],
            "Coverage": [f"{c.coverage_pct}%" for c in sorted_covs],
        }
    )
    return df.to_dicts()


# ---------------------------------------------------------------------------
# App template (scaffolded by ``dbxdec dashboard``)
# ---------------------------------------------------------------------------


APP_TEMPLATE = '''\
"""Pipeline observability dashboard.

Launch with::

    python {app_path}

Requires::

    uv add databricks-bundle-decorators[observability]
"""

import {package_name}.pipelines  # noqa: F401 — populate the job registry

from databricks_bundle_decorators.dashboard import run_app

run_app()
'''


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
    import dash_bootstrap_components as dbc

    return dbc.Badge(
        state,
        color=_STATE_BADGE_COLORS.get(state, "light"),
        className="me-1",
    )


# ---------------------------------------------------------------------------
# KPI card helper
# ---------------------------------------------------------------------------


def _kpi_card(title: str, value: str | int, color: str = "primary") -> Any:
    """Return a Bootstrap card showing a single KPI metric."""
    import dash_bootstrap_components as dbc
    from dash import html

    return dbc.Card(
        dbc.CardBody(
            [
                html.H6(title, className="card-subtitle mb-1 text-muted"),
                html.H3(str(value), className=f"card-title text-{color} mb-0"),
            ],
            className="text-center py-3",
        ),
        className="shadow-sm",
    )


# ---------------------------------------------------------------------------
# Dash app — layout builders (pure functions returning component trees)
# ---------------------------------------------------------------------------

_FIGURE_BUILDERS: dict[str, tuple[str, Any]] = {
    "daily": (
        "calendar",
        lambda c: _build_daily_calendar(set(c.expected_keys), set(c.completed_keys)),
    ),
    "weekly": (
        "week calendar",
        lambda c: _build_weekly_calendar(set(c.expected_keys), set(c.completed_keys)),
    ),
    "monthly": (
        "month calendar",
        lambda c: _build_monthly_calendar(set(c.expected_keys), set(c.completed_keys)),
    ),
    "hourly": (
        "hour calendar",
        lambda c: _build_hourly_calendar(set(c.expected_keys), set(c.completed_keys)),
    ),
}


def _page_overview(
    overviews: list[JobOverview],
    coverages: dict[str, BackfillCoverage],
) -> Any:
    """Build the Overview page layout — KPI cards + job status grid."""
    import dash_bootstrap_components as dbc
    from dash import html

    total_jobs = len(overviews)
    deployed = sum(1 for o in overviews if o.job_id)
    total_runs = sum(o.total_runs for o in overviews)
    total_failures = sum(o.failures for o in overviews)
    all_durations = [
        o.avg_duration_seconds for o in overviews if o.avg_duration_seconds is not None
    ]
    avg_dur = round(sum(all_durations) / len(all_durations), 1) if all_durations else 0
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
            dbc.Col(_kpi_card("Deployed", deployed, "info"), md=2),
            dbc.Col(_kpi_card("Total Runs", total_runs), md=2),
            dbc.Col(_kpi_card("Success Rate", f"{success_rate}%", "success"), md=2),
            dbc.Col(_kpi_card("Failures", total_failures, "danger"), md=2),
            dbc.Col(_kpi_card("Avg Duration", f"{avg_dur}s"), md=2),
        ],
        className="mb-4 g-3",
    )

    # Job status cards — one per job
    job_cards = []
    for o in sorted(overviews, key=lambda x: x.job_name):
        state = o.last_run_state or "UNKNOWN"
        rate = (
            f"{o.successes / o.total_runs * 100:.0f}%" if o.total_runs > 0 else "\u2014"
        )
        badge = _state_badge(state)
        backfill_badge = (
            dbc.Badge("Backfill", color="info", className="ms-1")
            if o.has_backfill
            else None
        )

        deployed_icon = (
            html.Span("\u2713 Deployed", className="text-success small")
            if o.job_id
            else html.Span("\u2717 Not deployed", className="text-muted small")
        )

        card = dbc.Card(
            dbc.CardBody(
                [
                    html.Div(
                        [
                            html.H6(
                                o.job_name,
                                className="card-title mb-1 text-truncate",
                            ),
                            html.Div(
                                [badge, backfill_badge] if backfill_badge else [badge],
                            ),
                        ],
                    ),
                    html.Hr(className="my-2"),
                    html.Div(
                        [
                            html.Span(
                                f"{o.total_runs} runs",
                                className="small text-muted me-2",
                            ),
                            html.Span(
                                f"{rate} pass rate",
                                className="small text-muted me-2",
                            ),
                        ],
                    ),
                    deployed_icon,
                ]
            ),
            className="shadow-sm h-100",
        )
        job_cards.append(dbc.Col(card, md=3, className="mb-3"))

    # Backfill summary (if any)
    backfill_section: list[Any] = []
    if coverages:
        cov_records = _coverages_to_records(coverages)
        from dash import dash_table

        backfill_section = [
            html.H5("Backfill Coverage", className="mt-4 mb-3"),
            dash_table.DataTable(
                data=cov_records,  # type: ignore[invalid-argument-type]
                columns=[{"name": k, "id": k} for k in cov_records[0]],
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left", "padding": "8px"},
                style_header={
                    "backgroundColor": "#f8f9fa",
                    "fontWeight": "bold",
                },
                style_data_conditional=[
                    {
                        "if": {"column_id": "Coverage"},
                        "fontWeight": "bold",
                    },
                ],
                page_size=20,
            ),
        ]

    return html.Div(
        [
            html.H4("Overview", className="mb-3"),
            kpi_row,
            html.H5("Job Status", className="mt-4 mb-3"),
            dbc.Row(job_cards),
            *backfill_section,
        ]
    )


def _page_jobs(overviews: list[JobOverview]) -> Any:
    """Build the Jobs page layout — sortable table of all jobs."""
    from dash import dash_table, html

    records = _overviews_to_records(overviews)
    if not records:
        return html.Div(
            html.P("No jobs registered.", className="text-muted"),
        )

    return html.Div(
        [
            html.H4("Jobs", className="mb-3"),
            html.P(
                f"{len(records)} registered jobs across the bundle.",
                className="text-muted",
            ),
            dash_table.DataTable(
                id="jobs-table",
                data=records,  # type: ignore[invalid-argument-type]
                columns=[{"name": k, "id": k} for k in records[0]],
                sort_action="native",
                filter_action="native",
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left", "padding": "8px"},
                style_header={
                    "backgroundColor": "#f8f9fa",
                    "fontWeight": "bold",
                },
                style_data_conditional=[
                    {
                        "if": {
                            "filter_query": '{Status} = "FAILED"',
                            "column_id": "Status",
                        },
                        "color": "#dc3545",
                        "fontWeight": "bold",
                    },
                    {
                        "if": {
                            "filter_query": '{Status} = "SUCCESS"',
                            "column_id": "Status",
                        },
                        "color": "#198754",
                    },
                    {
                        "if": {
                            "filter_query": '{Status} = "RUNNING"',
                            "column_id": "Status",
                        },
                        "color": "#0d6efd",
                    },
                ],
                page_size=25,
            ),
        ]
    )


def _page_runs(
    all_runs: dict[str, list[RunInfo]],
    job_names: list[str],
) -> Any:
    """Build the Runs page layout — all runs across all jobs."""
    import dash_bootstrap_components as dbc
    from dash import dash_table, html

    all_records: list[dict[str, Any]] = []
    for name in job_names:
        for r in all_runs.get(name, []):
            start = "\u2014"
            if r.start_time_ms:
                from datetime import datetime, timezone

                dt = datetime.fromtimestamp(r.start_time_ms / 1000, tz=timezone.utc)
                start = dt.strftime("%Y-%m-%d %H:%M")
            all_records.append(
                {
                    "Run ID": r.run_id,
                    "Job": name,
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

    return html.Div(
        [
            html.H4("Runs", className="mb-3"),
            html.P(
                f"{len(all_records)} runs across {len(job_names)} jobs.",
                className="text-muted",
            ),
            dash_table.DataTable(
                id="runs-table",
                data=all_records,  # type: ignore[invalid-argument-type]
                columns=[{"name": k, "id": k} for k in all_records[0]],
                sort_action="native",
                filter_action="native",
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left", "padding": "8px"},
                style_header={
                    "backgroundColor": "#f8f9fa",
                    "fontWeight": "bold",
                },
                style_data_conditional=[
                    {
                        "if": {
                            "filter_query": '{Status} = "FAILED"',
                            "column_id": "Status",
                        },
                        "color": "#dc3545",
                        "fontWeight": "bold",
                    },
                    {
                        "if": {
                            "filter_query": '{Status} = "SUCCESS"',
                            "column_id": "Status",
                        },
                        "color": "#198754",
                    },
                    {
                        "if": {
                            "filter_query": '{Status} = "INTERNAL_ERROR"',
                            "column_id": "Status",
                        },
                        "color": "#dc3545",
                        "fontWeight": "bold",
                    },
                ],
                page_size=50,
            ),
        ]
    )


def _page_job_detail(
    job_name: str,
    overviews: list[JobOverview],
    runs: list[RunInfo],
    coverages: dict[str, BackfillCoverage],
    profile: str | None,
) -> Any:
    """Build the Job Detail page — run history, task DAG, backfill coverage."""
    import dash_bootstrap_components as dbc
    from dash import dcc, html

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

    # Run history table
    run_records = _runs_to_records(runs)
    from dash import dash_table

    run_table = (
        dash_table.DataTable(
            data=run_records,  # type: ignore[invalid-argument-type]
            columns=[{"name": k, "id": k} for k in run_records[0]]
            if run_records
            else [],
            sort_action="native",
            style_table={"overflowX": "auto"},
            style_cell={"textAlign": "left", "padding": "8px"},
            style_header={"backgroundColor": "#f8f9fa", "fontWeight": "bold"},
            style_data_conditional=[
                {
                    "if": {
                        "filter_query": '{Status} = "FAILED"',
                        "column_id": "Status",
                    },
                    "color": "#dc3545",
                    "fontWeight": "bold",
                },
                {
                    "if": {
                        "filter_query": '{Status} = "SUCCESS"',
                        "column_id": "Status",
                    },
                    "color": "#198754",
                },
            ],
            page_size=25,
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
                dag_section.append(html.H5("Task Breakdown", className="mt-3 mb-2"))
                dag_section.append(
                    dash_table.DataTable(
                        data=task_records,  # type: ignore[invalid-argument-type]
                        columns=[{"name": k, "id": k} for k in task_records[0]],
                        style_table={"overflowX": "auto"},
                        style_cell={"textAlign": "left", "padding": "8px"},
                        style_header={
                            "backgroundColor": "#f8f9fa",
                            "fontWeight": "bold",
                        },
                        style_data_conditional=[
                            {
                                "if": {
                                    "filter_query": '{Status} = "FAILED"',
                                    "column_id": "Status",
                                },
                                "color": "#dc3545",
                                "fontWeight": "bold",
                            },
                            {
                                "if": {
                                    "filter_query": '{Status} = "SUCCESS"',
                                    "column_id": "Status",
                                },
                                "color": "#198754",
                            },
                        ],
                        page_size=50,
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
        builder = _FIGURE_BUILDERS.get(cov.kind)
        if builder:
            _, build_fn = builder
            fig = build_fn(cov)
            if fig is not None:
                backfill_section.append(dcc.Graph(figure=fig))
        else:
            fig = _build_partition_grid(cov.expected_keys, set(cov.completed_keys))
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


def _page_backfills(coverages: dict[str, BackfillCoverage]) -> Any:
    """Build the Backfills page — coverage summary + per-job visualizations."""
    import dash_bootstrap_components as dbc
    from dash import dcc, html

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

    # Summary table
    cov_records = _coverages_to_records(coverages)
    from dash import dash_table

    summary_table = dash_table.DataTable(
        data=cov_records,  # type: ignore[invalid-argument-type]
        columns=[{"name": k, "id": k} for k in cov_records[0]],
        sort_action="native",
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "8px"},
        style_header={"backgroundColor": "#f8f9fa", "fontWeight": "bold"},
        page_size=20,
    )

    # Per-job detail sections
    detail_sections: list[Any] = []
    for name, cov in sorted(coverages.items()):
        builder = _FIGURE_BUILDERS.get(cov.kind)
        fig = None
        label = "partition grid"
        if builder:
            label, build_fn = builder
            fig = build_fn(cov)
        else:
            fig = _build_partition_grid(cov.expected_keys, set(cov.completed_keys))

        content: list[Any] = [
            html.P(
                f"{cov.coverage_pct}% coverage  \u00b7  "
                f"{cov.kind.title()} backfill  \u00b7  "
                f"{len(cov.completed_keys)}/{len(cov.expected_keys)} keys ({label})",
                className="text-muted mb-2",
            ),
        ]
        if fig is not None:
            content.append(dcc.Graph(figure=fig))
        if cov.missing_keys:
            content.append(
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

        detail_sections.append(
            dbc.Card(
                [
                    dbc.CardHeader(html.H6(name, className="mb-0")),
                    dbc.CardBody(content),
                ],
                className="mb-3",
            )
        )

    return html.Div(
        [
            html.H4("Backfill Coverage", className="mb-3"),
            html.P(
                "Expected keys from BackfillDef vs successful runs "
                "with matching backfill_key parameter. "
                "For exact key-level catchup, use: dbxdec catchup",
                className="text-muted",
            ),
            summary_table,
            html.Div(detail_sections, className="mt-4"),
        ]
    )


# ---------------------------------------------------------------------------
# App entry point
# ---------------------------------------------------------------------------


def run_app(
    *,
    host: str = "127.0.0.1",
    port: int = 8050,
    debug: bool = False,
) -> None:
    """Launch the Dash observability dashboard.

    Import your pipeline package **before** calling this so the
    job registry is populated.  Requires the ``[observability]``
    optional dependency (``dash``).

    The dashboard is **bundle-scoped** — only jobs deployed from
    the current bundle are shown.  It uses the Databricks CLI for
    data access, inheriting the same unified credentials used for
    ``databricks bundle deploy``.

    Parameters
    ----------
    host:
        Host to bind the server to.
    port:
        Port number.
    debug:
        Enable Dash debug mode with hot-reloading.
    """
    try:
        import dash
        import dash_bootstrap_components as dbc
    except ImportError as exc:
        raise ImportError(
            "dash and dash-bootstrap-components are required for the "
            "observability dashboard. "
            "Install with: uv add databricks-bundle-decorators[observability]"
        ) from exc

    from dash import Input, Output, dcc, html

    from databricks_bundle_decorators.registry import _JOB_REGISTRY

    job_names = sorted(_JOB_REGISTRY.keys())
    if not job_names:
        print(
            "Error: No jobs found in registry. "
            "Ensure your pipeline package is imported before run_app().",
            file=sys.stderr,
        )
        sys.exit(1)

    # --- Fetch data ---
    _data: dict[str, Any] = {
        "job_names": job_names,
        "job_id_map": {},
        "all_runs": {},
        "overviews": [],
        "coverages": {},
    }

    def _refresh_data(target: str | None, profile: str | None) -> None:
        job_id_map = resolve_job_ids(target=target, profile=profile)
        _data["job_id_map"] = job_id_map

        all_runs: dict[str, list[RunInfo]] = {}
        overviews: list[JobOverview] = []
        coverages: dict[str, BackfillCoverage] = {}

        for name in job_names:
            meta = _JOB_REGISTRY[name]
            job_id = job_id_map.get(name)
            runs = fetch_job_runs(job_id, profile=profile) if job_id else []
            all_runs[name] = runs

            has_bf = meta.backfill is not None
            overviews.append(
                build_job_overview(name, job_id, runs, has_backfill=has_bf)
            )

            if has_bf and meta.backfill is not None:
                expected = meta.backfill.keys()
                kind = _backfill_kind(meta.backfill)
                coverages[name] = compute_backfill_coverage(
                    name, runs, expected, kind=kind
                )

        _data["all_runs"] = all_runs
        _data["overviews"] = overviews
        _data["coverages"] = coverages

    # --- Build Dash app ---
    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.COSMO],
        suppress_callback_exceptions=True,
    )
    app.title = "Pipeline Observability"

    navbar = dbc.Navbar(
        dbc.Container(
            [
                dbc.NavbarBrand(
                    [
                        html.Span(
                            "\u26a1",
                            className="me-2",
                        ),
                        "Pipeline Observability",
                    ],
                    href="/",
                    className="fw-bold",
                ),
                dbc.Nav(
                    [
                        dbc.NavItem(dbc.NavLink("Overview", href="/")),
                        dbc.NavItem(dbc.NavLink("Jobs", href="/jobs")),
                        dbc.NavItem(dbc.NavLink("Runs", href="/runs")),
                        dbc.NavItem(dbc.NavLink("Backfills", href="/backfills")),
                    ],
                    navbar=True,
                    className="me-auto",
                ),
                dbc.Nav(
                    [
                        dbc.NavItem(
                            dbc.Input(
                                id="input-target",
                                placeholder="Target (e.g. dev)",
                                size="sm",
                                className="me-2",
                                style={"width": "140px"},
                            )
                        ),
                        dbc.NavItem(
                            dbc.Input(
                                id="input-profile",
                                placeholder="CLI profile",
                                size="sm",
                                className="me-2",
                                style={"width": "140px"},
                            )
                        ),
                        dbc.NavItem(
                            dbc.Button(
                                "\u21bb Refresh",
                                id="btn-refresh",
                                color="outline-light",
                                size="sm",
                            )
                        ),
                    ],
                    navbar=True,
                ),
            ],
            fluid=True,
        ),
        color="dark",
        dark=True,
        className="mb-4",
    )

    app.layout = html.Div(
        [
            dcc.Location(id="url", refresh=False),
            navbar,
            dbc.Container(
                [
                    dcc.Loading(
                        id="page-loading",
                        children=html.Div(id="page-content"),
                        type="default",
                    ),
                ],
                fluid=True,
                className="pb-4",
            ),
        ]
    )

    # --- Callbacks ---

    @app.callback(
        Output("page-content", "children"),
        [
            Input("url", "pathname"),
            Input("btn-refresh", "n_clicks"),
        ],
        [
            dash.State("input-target", "value"),
            dash.State("input-profile", "value"),
        ],
    )
    def _display_page(
        pathname: str | None,
        n_clicks: int | None,
        target: str | None,
        profile: str | None,
    ) -> Any:
        target_val = target if target else None
        profile_val = profile if profile else None

        _refresh_data(target_val, profile_val)

        overviews = _data["overviews"]
        all_runs = _data["all_runs"]
        coverages = _data["coverages"]

        if pathname is None or pathname == "/":
            return _page_overview(overviews, coverages)

        if pathname == "/jobs":
            return _page_jobs(overviews)

        if pathname.startswith("/jobs/"):
            name = pathname[len("/jobs/") :]
            runs = all_runs.get(name, [])
            return _page_job_detail(name, overviews, runs, coverages, profile_val)

        if pathname == "/runs":
            return _page_runs(all_runs, job_names)

        if pathname == "/backfills":
            return _page_backfills(coverages)

        return dbc.Alert(
            f"Page not found: {pathname}",
            color="warning",
        )

    app.run(host=host, port=port, debug=debug)
