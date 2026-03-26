"""Pure computation functions for the observability dashboard.

No I/O — easy to test.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

import whenever

from databricks_bundle_decorators.dashboard._data import (
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

    most_recent = runs[0]

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


def _filter_past_keys(keys: list[str], kind: str) -> list[str]:
    """Remove keys that represent future time periods.

    For time-based backfills, keys representing periods that have
    not yet completed are excluded.  Static backfills are returned
    unchanged.
    """
    today = whenever.ZonedDateTime.now("UTC").date()

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
        week_re = re.compile(r"^(\d{4})-W(\d{2})$")
        # Include the current ISO week — its data should be materializable.
        cur_iso = today.py_date().isocalendar()
        cutoff_year, cutoff_week = cur_iso[0], cur_iso[1]
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
        # Include the current month — its data should be materializable.
        first_of_month = today.py_date().replace(day=1)
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
        fmt = "%Y-%m-%dT%H"
        now_utc = whenever.ZonedDateTime.now("UTC")
        cutoff_str = now_utc.py_datetime().strftime(fmt)
        result = []
        for k in keys:
            try:
                datetime.strptime(k, fmt)  # validate format
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

    # Build mapping of key → most recent successful run (by start_time_ms)
    key_runs: dict[str, tuple[int, int | None]] = {}
    for r in runs:
        if r.result_state == "SUCCESS" and r.backfill_key is not None:
            prev = key_runs.get(r.backfill_key)
            if prev is None or (r.start_time_ms or 0) > (prev[1] or 0):
                key_runs[r.backfill_key] = (r.run_id, r.start_time_ms)

    # Track keys that were attempted but only have failures
    errored: set[str] = set()
    for r in runs:
        if r.backfill_key is not None and _is_terminal_failure(
            r.result_state, r.life_cycle_state
        ):
            errored.add(r.backfill_key)
    # Remove keys that also have a successful run
    errored -= set(key_runs)

    # Track keys with an active (running/pending) run but no success yet
    active: set[str] = set()
    for r in runs:
        if r.backfill_key is not None and _is_active(
            r.result_state, r.life_cycle_state
        ):
            active.add(r.backfill_key)
    # Remove keys that already have a successful run
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
        completed_key_runs=due_key_runs,
        errored_keys=errored_list,
        in_progress_keys=in_progress_list,
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
