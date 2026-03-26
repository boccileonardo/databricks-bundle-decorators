"""Polars data helpers — convert dataclasses to DataFrames for display."""

from __future__ import annotations

from typing import Any

from databricks_bundle_decorators.dashboard._compute import _effective_state
from databricks_bundle_decorators.dashboard._data import (
    BackfillCoverage,
    JobOverview,
    RunInfo,
    TaskRunInfo,
)


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
