"""Display helpers — convert dataclasses to table records for Dash AG Grid."""

from __future__ import annotations

from typing import Any

from databricks_bundle_decorators.dashboard._data import (
    BackfillCoverage,
    JobOverview,
)


def _overviews_to_records(
    overviews: list[JobOverview],
    coverages: dict[str, BackfillCoverage] | None = None,
    workspace_url: str | None = None,
) -> list[dict[str, Any]]:
    """Convert job overviews to display-ready table records.

    Produces a unified table with optional workspace links and
    backfill completeness columns merged in.

    Parameters
    ----------
    overviews:
        Job overview objects.
    coverages:
        Optional backfill coverages, keyed by job name.
        When provided, a *Completeness* column is added with a
        clickable link to ``/backfills/<name>``.
    workspace_url:
        Databricks workspace base URL.  When provided, job
        names become clickable links to the workspace job page.
    """
    if not overviews:
        return []

    cov_map = coverages or {}
    records: list[dict[str, Any]] = []

    for o in overviews:
        # Job name — link to workspace when available
        if workspace_url and o.job_id is not None:
            job_cell = f"[{o.job_name}]({workspace_url}/jobs/{o.job_id})"
        else:
            job_cell = o.job_name

        # Status
        status = o.last_run_state or "\u2014"

        # Runs summary  (e.g. "10  (8 \u2713 / 2 \u2717)")
        if o.total_runs:
            runs_cell = f"{o.total_runs}  ({o.successes} \u2713 / {o.failures} \u2717)"
        else:
            runs_cell = "\u2014"

        # Success rate (over terminal runs only — excludes in-progress)
        terminal = o.successes + o.failures
        if terminal:
            rate = round(o.successes / terminal * 100)
            rate_cell = f"{rate}%"
        else:
            rate_cell = "\u2014"

        # Avg duration
        if o.avg_duration_seconds is not None:
            avg_dur = _fmt_duration(o.avg_duration_seconds)
        else:
            avg_dur = "\u2014"

        # Completeness — link to backfill detail when available
        cov = cov_map.get(o.job_name)
        if cov is not None:
            cov_cell = f"[{cov.coverage_pct}%](/backfills/{o.job_name})"
        else:
            cov_cell = ""

        records.append(
            {
                "Job": job_cell,
                "Status": status,
                "Runs": runs_cell,
                "Success %": rate_cell,
                "Avg Dur.": avg_dur,
                "Completeness": cov_cell,
            }
        )

    return records


def _fmt_duration(seconds: float) -> str:
    """Format seconds as a compact human-readable string."""
    total = int(seconds)
    if total < 60:
        return f"{total}s"
    m, s = divmod(total, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m {s:02d}s"


_TIME_BASED_KINDS = frozenset({"daily", "weekly", "monthly", "hourly"})

# Unicode squares for key status display in AG Grid cells.
_SQ_COMPLETED = "\U0001f7e9"  # green square
_SQ_IN_PROGRESS = "\U0001f7e6"  # blue square
_SQ_FAILED = "\U0001f7e5"  # red square
_SQ_MISSING = "\U0001f7e8"  # yellow square


def _build_key_squares(cov: BackfillCoverage, max_squares: int) -> list[str]:
    """Build colored-square strings from backfill coverage keys.

    For **static** backfills, shows up to ``max_squares`` keys with
    failures sorted first (so problems are immediately visible).

    For **time-based** backfills, shows the last ``max_squares``
    logical periods from the due keys (completed + missing + in-progress),
    sorted chronologically.
    """
    completed_set = set(cov.completed_keys)
    errored_set = set(cov.errored_keys) if cov.errored_keys else set()
    in_progress_set = set(cov.in_progress_keys) if cov.in_progress_keys else set()

    # Use pre-computed due_keys when available; otherwise reconstruct.
    if cov.due_keys is not None:
        due_keys = cov.due_keys
    else:
        seen: set[str] = set()
        due_keys = []
        for k in (
            cov.completed_keys
            + cov.missing_keys
            + list(in_progress_set)
            + list(errored_set)
        ):
            if k not in seen:
                seen.add(k)
                due_keys.append(k)

    if cov.kind in _TIME_BASED_KINDS:
        # Chronological sort, take the most recent N
        selected = sorted(due_keys)[-max_squares:]
    else:
        # Static: failed first, then in-progress, then success, capped at N
        failed = [
            k for k in due_keys if k not in completed_set and k not in in_progress_set
        ]
        running = [k for k in due_keys if k in in_progress_set]
        succeeded = [k for k in due_keys if k in completed_set]
        selected = (failed + running + succeeded)[:max_squares]

    squares: list[str] = []
    for k in selected:
        if k in completed_set:
            squares.append(_SQ_COMPLETED)
        elif k in in_progress_set:
            squares.append(_SQ_IN_PROGRESS)
        elif k in errored_set:
            squares.append(_SQ_FAILED)
        else:
            squares.append(_SQ_MISSING)
    return squares


def _coverages_to_records(
    coverages: dict[str, BackfillCoverage],
    *,
    max_squares: int = 5,
) -> list[dict[str, Any]]:
    """Convert backfill coverages to display-ready table records.

    The *Keys* column shows colored squares derived from backfill
    keys (not raw chronological runs):

    - **Static** backfills: up to ``max_squares`` keys, sorted
      with failures first then successes — giving immediate
      visibility to what still needs attention.
    - **Time-based** backfills (daily, weekly, monthly, hourly):
      the most recent ``max_squares`` logical periods from the
      backfill definition, regardless of which runs exist.

    Parameters
    ----------
    coverages:
        Backfill coverages keyed by job name.
    max_squares:
        Maximum number of key status squares to show.
    """
    if not coverages:
        return []

    sorted_covs = sorted(coverages.values(), key=lambda c: c.coverage_pct)

    records: list[dict[str, Any]] = []
    for c in sorted_covs:
        done = len(c.completed_keys)
        # "due" = all non-future keys (completed + missing + in-progress)
        due = len(
            set(c.completed_keys) | set(c.missing_keys) | set(c.in_progress_keys or [])
        )
        errored = len(c.errored_keys) if c.errored_keys else 0

        # Completeness column: "45 / 90  (50%)"
        cov_cell = f"{done} / {due}  ({c.coverage_pct}%)"

        # Key status squares
        squares = _build_key_squares(c, max_squares)
        keys_cell = "".join(squares) if squares else "\u2014"

        rec: dict[str, Any] = {
            "Job": c.job_name,
            "Type": c.kind.title(),
            "Completeness": cov_cell,
            "Keys": keys_cell,
        }
        if errored:
            rec["Errors"] = errored
        else:
            rec["Errors"] = ""

        records.append(rec)

    return records
