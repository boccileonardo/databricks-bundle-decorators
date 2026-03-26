"""Polars data helpers — convert dataclasses to DataFrames for display."""

from __future__ import annotations

from typing import Any

from databricks_bundle_decorators.dashboard._data import (
    BackfillCoverage,
    JobOverview,
)


def _overviews_to_records(
    overviews: list[JobOverview],
    workspace_url: str | None = None,
) -> list[dict[str, Any]]:
    """Convert job overviews to display-ready table records via polars.

    When ``workspace_url`` is provided, the Job column is rendered as
    a Markdown link to the Databricks workspace job page.
    """
    import polars as pl

    if not overviews:
        return []

    df = pl.DataFrame(
        {
            "Job": [o.job_name for o in overviews],
            "Job ID": [o.job_id for o in overviews],
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
    records = df.select(
        "Job",
        "Job ID",
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

    # Add workspace links when URL is available
    if workspace_url:
        for r in records:
            job_id = r.get("Job ID")
            if job_id is not None:
                r["Job"] = f"[{r['Job']}]({workspace_url}/jobs/{job_id})"

    # Drop internal Job ID column from output
    for r in records:
        r.pop("Job ID", None)

    return records


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
