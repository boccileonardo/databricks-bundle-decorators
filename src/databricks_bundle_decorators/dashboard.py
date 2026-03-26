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


# ---------------------------------------------------------------------------
# App template (scaffolded by ``dbxdec dashboard``)
# ---------------------------------------------------------------------------


APP_TEMPLATE = '''\
"""Pipeline observability dashboard.

Launch with::

    streamlit run {app_path}

Requires::

    uv add databricks-bundle-decorators[observability]
"""

import {package_name}.pipelines  # noqa: F401 — populate the job registry

from databricks_bundle_decorators.dashboard import run_app

run_app()
'''


# ---------------------------------------------------------------------------
# Streamlit app — rendering helpers
# ---------------------------------------------------------------------------


def _render_overview(overviews: list[JobOverview]) -> None:
    import streamlit as st

    # --- KPI row ---
    total_jobs = len(overviews)
    deployed = sum(1 for o in overviews if o.job_id)
    total_runs = sum(o.total_runs for o in overviews)
    total_failures = sum(o.failures for o in overviews)
    all_durations = [
        o.avg_duration_seconds for o in overviews if o.avg_duration_seconds is not None
    ]
    avg_dur = round(sum(all_durations) / len(all_durations), 1) if all_durations else 0

    cols = st.columns(5)
    cols[0].metric("Jobs", total_jobs)
    cols[1].metric("Deployed", deployed)
    cols[2].metric("Total Runs", total_runs)
    cols[3].metric("Failures", total_failures, delta_color="inverse")
    cols[4].metric("Avg Duration", f"{avg_dur}s")

    st.markdown("---")

    # --- Job table ---
    from datetime import datetime, timezone

    rows = []
    for o in overviews:
        rate = (
            f"{o.successes / o.total_runs * 100:.0f}%" if o.total_runs > 0 else "\u2014"
        )
        last_run = "\u2014"
        if o.last_run_time_ms:
            dt = datetime.fromtimestamp(o.last_run_time_ms / 1000, tz=timezone.utc)
            last_run = dt.strftime("%Y-%m-%d %H:%M UTC")

        rows.append(
            {
                "Job": o.job_name,
                "Deployed": "\u2713" if o.job_id else "\u2717",
                "Runs": o.total_runs,
                "Pass": o.successes,
                "Fail": o.failures,
                "Rate": rate,
                "Last Run": last_run,
                "Status": o.last_run_state or "\u2014",
                "Avg (s)": str(o.avg_duration_seconds)
                if o.avg_duration_seconds
                else "\u2014",
                "Backfill": "\u2713" if o.has_backfill else "",
            }
        )
    st.dataframe(rows, width="stretch", hide_index=True)


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


def _render_run_history(
    job_names: list[str],
    all_runs: dict[str, list[RunInfo]],
    overviews: list[JobOverview],
    profile: str | None,
) -> None:
    import streamlit as st

    selected = st.selectbox("Job", job_names, key="rh_job")
    runs = all_runs.get(selected, [])

    if not runs:
        st.info(f"No runs found for **{selected}**.")
        return

    from datetime import datetime, timezone

    rows = []
    for r in runs:
        start = "\u2014"
        if r.start_time_ms:
            dt = datetime.fromtimestamp(r.start_time_ms / 1000, tz=timezone.utc)
            start = dt.strftime("%Y-%m-%d %H:%M")
        rows.append(
            {
                "Run ID": r.run_id,
                "Status": _effective_state(r.result_state, r.life_cycle_state),
                "Start": start,
                "Duration (s)": r.duration_seconds or "\u2014",
                "Backfill Key": r.backfill_key or "",
            }
        )
    st.dataframe(rows, width="stretch", hide_index=True)

    # Show error message for the most recent failed run
    errored = [
        r
        for r in runs
        if _is_terminal_failure(r.result_state, r.life_cycle_state) and r.state_message
    ]
    if errored:
        latest = errored[0]
        st.error(
            f"**Run {latest.run_id}** "
            f"({_effective_state(latest.result_state, latest.life_cycle_state)}): "
            f"{latest.state_message}"
        )

    # --- Task details for selected run ---
    overview = next((o for o in overviews if o.job_name == selected), None)
    if not overview or not overview.job_id:
        return

    run_ids = [r.run_id for r in runs]
    selected_run_id = st.selectbox("Inspect run", run_ids, key="rh_run_detail")
    if selected_run_id is None:
        return

    with st.spinner("Fetching task details\u2026"):
        try:
            task_runs = fetch_task_runs(selected_run_id, profile=profile)
        except Exception as exc:  # noqa: BLE001
            st.error(f"Failed to fetch task details: {exc}")
            return

    if not task_runs:
        st.info("No task data for this run.")
        return

    dag_fig = _build_task_dag_figure(task_runs)
    if dag_fig is not None:
        st.plotly_chart(dag_fig, width="stretch")

    task_rows = []
    for t in task_runs:
        row: dict[str, object] = {
            "Task": t.task_key,
            "Status": _effective_state(t.result_state, t.life_cycle_state),
            "Duration (s)": t.duration_seconds or "\u2014",
        }
        if t.state_message:
            row["Error"] = t.state_message
        task_rows.append(row)
    st.dataframe(task_rows, width="stretch", hide_index=True)


def _render_backfill(
    coverages: dict[str, BackfillCoverage],
) -> None:
    import streamlit as st

    st.caption(
        "Expected keys from BackfillDef vs successful runs "
        "with matching backfill_key parameter.  "
        "For exact key-level catchup, use: dbxdec catchup"
    )

    # Summary table
    rows = [
        {
            "Job": c.job_name,
            "Type": c.kind.title(),
            "Expected": len(c.expected_keys),
            "Completed": len(c.completed_keys),
            "Missing": len(c.missing_keys),
            "Coverage": f"{c.coverage_pct}%",
        }
        for c in sorted(coverages.values(), key=lambda c: c.coverage_pct)
    ]
    st.dataframe(rows, width="stretch", hide_index=True)

    # Per-job visualizations
    _FIGURE_BUILDERS = {
        "daily": (
            "calendar",
            lambda c: _build_daily_calendar(
                set(c.expected_keys), set(c.completed_keys)
            ),
        ),
        "weekly": (
            "week calendar",
            lambda c: _build_weekly_calendar(
                set(c.expected_keys), set(c.completed_keys)
            ),
        ),
        "monthly": (
            "month calendar",
            lambda c: _build_monthly_calendar(
                set(c.expected_keys), set(c.completed_keys)
            ),
        ),
        "hourly": (
            "hour calendar",
            lambda c: _build_hourly_calendar(
                set(c.expected_keys), set(c.completed_keys)
            ),
        ),
    }

    for name, cov in sorted(coverages.items()):
        builder = _FIGURE_BUILDERS.get(cov.kind)
        if builder:
            label, build_fn = builder
            with st.expander(f"{name} \u2014 {cov.coverage_pct}% coverage ({label})"):
                fig = build_fn(cov)
                if fig is not None:
                    st.plotly_chart(fig, width="stretch")
                if cov.missing_keys:
                    st.markdown(f"**{len(cov.missing_keys)} not launched:**")
                    st.code("\n".join(cov.missing_keys))
        else:
            with st.expander(
                f"{name} \u2014 {cov.coverage_pct}% coverage (partition grid)"
            ):
                fig = _build_partition_grid(cov.expected_keys, set(cov.completed_keys))
                if fig is not None:
                    st.plotly_chart(fig, width="stretch")
                if cov.missing_keys:
                    st.markdown(f"**{len(cov.missing_keys)} not launched:**")
                    st.code("\n".join(cov.missing_keys))


# ---------------------------------------------------------------------------
# App entry point
# ---------------------------------------------------------------------------


def run_app() -> None:
    """Launch the Streamlit observability dashboard.

    Import your pipeline package **before** calling this so the
    job registry is populated.  Requires the ``[observability]``
    optional dependency (``streamlit``).

    The dashboard is **bundle-scoped** — only jobs deployed from
    the current bundle are shown.  It uses the Databricks CLI for
    data access, inheriting the same unified credentials used for
    ``databricks bundle deploy``.
    """
    try:
        import streamlit as st
    except ImportError as exc:
        raise ImportError(
            "streamlit is required for the observability dashboard. "
            "Install with: uv add databricks-bundle-decorators[observability]"
        ) from exc

    from databricks_bundle_decorators.registry import _JOB_REGISTRY

    st.set_page_config(page_title="Pipeline Observability", layout="wide")

    job_names = sorted(_JOB_REGISTRY.keys())
    if not job_names:
        st.error(
            "No jobs found in registry. "
            "Ensure your pipeline package is imported before run_app()."
        )
        return

    # --- Sidebar ---
    st.sidebar.title("Pipeline Observability")
    st.sidebar.markdown(f"**{len(job_names)}** registered jobs")

    with st.sidebar.expander("Settings", expanded=False):
        target = st.text_input("Bundle target", value="", help="e.g. dev, prod")
        profile = st.text_input("CLI profile", value="", help="Databricks CLI profile")

    profile_val = profile or None
    target_val = target or None

    # --- Resolve job IDs & fetch data with caching + spinner ---
    @st.cache_data(ttl=120, show_spinner=False)
    def _cached_resolve(target: str | None, profile: str | None) -> dict[str, int]:
        return resolve_job_ids(target=target, profile=profile)

    @st.cache_data(ttl=60, show_spinner=False)
    def _cached_job_runs(job_id: int, profile: str | None) -> list[RunInfo]:
        return fetch_job_runs(job_id, profile=profile)

    with st.spinner("Resolving bundle jobs\u2026"):
        job_id_map = _cached_resolve(target_val, profile_val)

    if not job_id_map:
        st.warning(
            "Could not resolve job IDs from bundle summary. "
            "Ensure the bundle is deployed and you're running "
            "from the project root."
        )

    with st.spinner("Fetching run data\u2026"):
        all_runs: dict[str, list[RunInfo]] = {}
        overviews: list[JobOverview] = []
        coverages: dict[str, BackfillCoverage] = {}

        for name in job_names:
            meta = _JOB_REGISTRY[name]
            job_id = job_id_map.get(name)
            runs = _cached_job_runs(job_id, profile_val) if job_id else []
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

    # --- Tab navigation (persisted via query params) ---
    tab_labels = ["Overview", "Run History"]
    if coverages:
        tab_labels.append("Backfill Coverage")

    params = st.query_params
    saved_tab = params.get("tab", "Overview")
    default_idx = tab_labels.index(saved_tab) if saved_tab in tab_labels else 0

    tabs = st.tabs(tab_labels)

    with tabs[0]:
        if default_idx == 0:
            params["tab"] = "Overview"
        _render_overview(overviews)

    with tabs[1]:
        if default_idx == 1:
            params["tab"] = "Run History"
        _render_run_history(job_names, all_runs, overviews, profile_val)

    if coverages:
        with tabs[2]:
            if default_idx == 2:
                params["tab"] = "Backfill Coverage"
            _render_backfill(coverages)
