"""Pure computation functions for the observability dashboard.

No I/O — easy to test.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from typing import Any

import whenever

from databricks_bundle_decorators.app._data import (
    BackfillCoverage,
    JobOverview,
    RunInfo,
)

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

#: Pre-compiled regex for ISO week keys (``YYYY-WNN``).
_WEEK_KEY_RE: re.Pattern[str] = re.compile(r"^(\d{4})-W(\d{2})$")

#: ``whenever`` format for hourly backfill keys.
_HOURLY_FMT: str = "YYYY-MM-DD'T'hh"

#: ``whenever`` format for daily backfill keys.
_DAILY_FMT: str = "YYYY-MM-DD"


def _infer_key_from_start_time(
    start_time_ms: int | None, kind: str, tz: str = "UTC", data_lag: int = 0
) -> str | None:
    """Infer the backfill key a run would have used based on its start time.

    When a run is triggered on-demand (e.g. via the "Run Now" button)
    without an explicit ``backfill_key``, the runtime falls back to
    ``BackfillDef.current_key()`` which derives the key from "now".
    This function reproduces that logic using the run's start time
    so the dashboard can credit on-demand runs to the correct partition.

    Returns ``None`` for static backfills or if start_time_ms is absent.
    """
    if start_time_ms is None or kind == "static":
        return None

    # Convert epoch millis to a ZonedDateTime in the backfill's tz
    utc_dt = datetime.fromtimestamp(start_time_ms / 1000.0, tz=UTC)
    zdt = whenever.ZonedDateTime(
        utc_dt.year,
        utc_dt.month,
        utc_dt.day,
        utc_dt.hour,
        utc_dt.minute,
        utc_dt.second,
        tz="UTC",
    ).to_tz(tz)

    if kind == "daily":
        d = zdt.date()
        if data_lag:
            d = d.subtract(days=data_lag)
        return d.format(_DAILY_FMT)
    if kind == "weekly":
        d = zdt.date()
        if data_lag:
            d = d.subtract(weeks=data_lag)
        iwd = d.iso_week_date()
        return f"{iwd.year}-W{iwd.week:02d}"
    if kind == "monthly":
        d = zdt.date()
        if data_lag:
            d = d.subtract(months=data_lag)
        return d.replace(day=1).format(_DAILY_FMT)
    if kind == "hourly":
        h = zdt.replace(minute=0, second=0, nanosecond=0)
        if data_lag:
            h = h.subtract(hours=data_lag)
        return h.format(_HOURLY_FMT)
    return None


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


def _is_active(
    result_state: str | None,
    life_cycle_state: str | None,
) -> bool:
    """Return True if the run is still in progress."""
    if result_state is not None:
        return False  # has a result → terminal
    return life_cycle_state in _ACTIVE_LIFECYCLE_STATES


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
    in_progress = sum(1 for r in runs if _is_active(r.result_state, r.life_cycle_state))
    durations = [r.duration_seconds for r in runs if r.duration_seconds is not None]

    # Most recent run by start_time_ms (not relying on API sort order).
    most_recent = max(runs, key=lambda r: r.start_time_ms or 0)

    return JobOverview(
        job_name=job_name,
        job_id=job_id,
        total_runs=len(runs),
        successes=successes,
        failures=failures,
        in_progress=in_progress,
        last_run_time_ms=most_recent.start_time_ms,
        last_run_state=_effective_state(
            most_recent.result_state, most_recent.life_cycle_state
        ),
        avg_duration_seconds=(
            round(sum(durations) / len(durations), 1) if durations else None
        ),
        has_backfill=has_backfill,
    )


def _filter_past_keys(keys: list[str], kind: str, tz: str = "UTC") -> list[str]:
    """Remove keys that represent future time periods.

    For time-based backfills, keys representing periods that have
    not yet completed are excluded.  Static backfills are returned
    unchanged.
    """
    today = whenever.ZonedDateTime.now(tz).date()

    if kind == "daily":
        # Include today — its data should be materializable.
        cutoff = today
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
        # Include the current ISO week — its data should be materializable.
        cur_iwd = today.iso_week_date()
        cutoff_year, cutoff_week = cur_iwd.year, cur_iwd.week
        result = []
        for k in keys:
            m = _WEEK_KEY_RE.match(k)
            if not m:
                result.append(k)
                continue
            y, w = int(m.group(1)), int(m.group(2))
            if (y, w) <= (cutoff_year, cutoff_week):
                result.append(k)
        return result

    if kind == "monthly":
        # Include the current month — its data should be materializable.
        first_of_month = today.replace(day=1).to_stdlib()
        result = []
        for k in keys:
            try:
                d = date.fromisoformat(k)
            except ValueError:
                result.append(k)
                continue
            if d <= first_of_month:
                result.append(k)
        return result

    if kind == "hourly":
        # Include the current hour — its data should be materializable.
        now_zdt = whenever.ZonedDateTime.now(tz)
        cutoff_str = now_zdt.replace(minute=0, second=0, nanosecond=0).format(
            _HOURLY_FMT
        )
        result = []
        for k in keys:
            if k <= cutoff_str:
                result.append(k)
        return result

    return keys


def _safe_compute_gap_keys(
    backfill: Any, primary_key: str, quartz_cron: str
) -> list[str]:
    """Compute schedule gap keys, returning [] on any error.

    Wraps ``_compute_schedule_gap_keys`` so that failures (e.g.
    malformed cron, key format mismatch) don't crash the dashboard.
    """
    from databricks_bundle_decorators.backfill import (  # noqa: PLC0415
        _compute_schedule_gap_keys,
    )

    try:
        return _compute_schedule_gap_keys(backfill, primary_key, quartz_cron)
    except Exception:  # noqa: BLE001
        return []


def compute_backfill_coverage(
    job_name: str,
    runs: list[RunInfo],
    expected_keys: list[str],
    *,
    kind: str = "static",
    tz: str = "UTC",
    backfill: Any = None,
    schedule_cron: str | None = None,
) -> BackfillCoverage:
    """Compute backfill coverage by matching run parameters to expected keys.

    Compares the ``backfill_key`` parameter of **successful** runs
    against the expected keys from the job's `BackfillDef`.  This
    gives **exact key-level matching** — unlike the approximate
    count-based approach that system tables would provide.

    When ``collect_schedule_gaps`` is enabled on the backfill definition
    and a ``schedule_cron`` is provided, successful runs also credit
    their schedule gap keys (keys between the previous cron fire and
    the primary key that the run processed as part of gap collection).

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
    tz:
        IANA timezone name used by the backfill definition.
    backfill:
        The ``BackfillDef`` instance (optional). Required for
        schedule gap crediting.
    schedule_cron:
        Quartz cron expression for the job's schedule (optional).
        Required for schedule gap crediting.
    """
    # Filter out future keys so they aren't counted as missing
    due_keys = _filter_past_keys(expected_keys, kind, tz=tz)

    # Determine if schedule gap crediting is applicable
    collect_gaps = (
        backfill is not None
        and schedule_cron is not None
        and getattr(backfill, "collect_schedule_gaps", False)
    )

    # Extract data_lag for key inference on on-demand runs
    data_lag: int = getattr(backfill, "data_lag", 0) if backfill is not None else 0

    # Single pass over runs to classify by backfill key.
    key_runs: dict[str, tuple[int, int | None]] = {}
    errored: set[str] = set()
    active: set[str] = set()
    for r in runs:
        effective_key = r.backfill_key
        # On-demand runs (e.g. "Run Now") have no backfill_key but the
        # runtime falls back to current_key() for time-based backfills.
        # Infer the key from the run's start time so these runs get credit.
        if effective_key is None:
            effective_key = _infer_key_from_start_time(
                r.start_time_ms, kind, tz, data_lag
            )
        if effective_key is None:
            continue
        if r.result_state == "SUCCESS":
            prev = key_runs.get(effective_key)
            if prev is None or (r.start_time_ms or 0) > (prev[1] or 0):
                key_runs[effective_key] = (r.run_id, r.start_time_ms)
            # Credit schedule gap keys covered by this run
            if collect_gaps and schedule_cron is not None:
                gap_keys = _safe_compute_gap_keys(
                    backfill,
                    effective_key,
                    schedule_cron,
                )
                for gk in gap_keys:
                    prev_gk = key_runs.get(gk)
                    if prev_gk is None or (r.start_time_ms or 0) > (prev_gk[1] or 0):
                        key_runs[gk] = (r.run_id, r.start_time_ms)
        elif _is_terminal_failure(r.result_state, r.life_cycle_state):
            errored.add(effective_key)
        elif _is_active(r.result_state, r.life_cycle_state):
            active.add(effective_key)

    # Remove keys that also have a successful run
    errored -= set(key_runs)
    active -= set(key_runs)

    completed = set(key_runs)
    due_set = set(due_keys)
    completed_list = sorted(due_set & completed)
    missing = sorted(due_set - completed - active)
    errored_list = sorted(due_set & errored)
    in_progress_list = sorted(due_set & active)
    pct = round(len(completed_list) / len(due_keys) * 100, 1) if due_keys else 0.0
    # Only keep entries for keys in the due set
    due_key_runs = {k: v for k, v in key_runs.items() if k in due_set}
    return BackfillCoverage(
        job_name=job_name,
        expected_keys=expected_keys,
        completed_keys=completed_list,
        missing_keys=missing,
        coverage_pct=pct,
        kind=kind,
        tz=tz,
        completed_key_runs=due_key_runs,
        errored_keys=errored_list,
        in_progress_keys=in_progress_list,
        due_keys=due_keys,
    )


def _backfill_kind(backfill: Any) -> str:
    """Map a ``BackfillDef`` subclass to a kind string."""
    from databricks_bundle_decorators.backfill import (  # noqa: PLC0415
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
