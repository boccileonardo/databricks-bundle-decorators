"""CLI data fetching for the observability dashboard.

Uses the Databricks CLI — same credentials as ``databricks bundle deploy``.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from typing import Any

from databricks_bundle_decorators.dashboard._data import RunInfo, TaskRunInfo


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
