"""Data classes for the observability dashboard.

Stdlib-only — no external dependencies required.
"""

from __future__ import annotations

from dataclasses import dataclass


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
    completed_key_runs: dict[str, tuple[int, int | None]] | None = None
    """Mapping of completed backfill key to ``(run_id, start_time_ms)``
    for the most recent successful run targeting that key.
    ``None`` when run info is unavailable."""
