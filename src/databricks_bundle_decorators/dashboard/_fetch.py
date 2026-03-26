"""CLI data fetching for the observability dashboard.

Uses the Databricks CLI — same credentials as ``databricks bundle deploy``.
"""

from __future__ import annotations

import json
import re as _re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from databricks_bundle_decorators.dashboard._data import RunInfo


def resolve_bundle_targets() -> list[str]:
    """Read target names from ``databricks.yaml`` in the working directory.

    Parses the YAML file with a lightweight regex — no PyYAML
    dependency required.  Returns an empty list when the file is
    missing or contains no targets section.
    """
    for name in ("databricks.yaml", "databricks.yml"):
        path = Path.cwd() / name
        if path.is_file():
            break
    else:
        return []

    text = path.read_text(encoding="utf-8")

    # Find the ``targets:`` top-level key and collect indented child keys.
    targets: list[str] = []
    in_targets = False
    for line in text.splitlines():
        stripped = line.rstrip()
        if stripped == "targets:" or stripped == "targets: ":
            in_targets = True
            continue
        if in_targets:
            # A non-indented, non-blank line ends the targets block.
            if stripped and not stripped.startswith((" ", "\t", "#")):
                break
            m = _re.match(r"^  (\w[\w-]*):", line)
            if m:
                targets.append(m.group(1))
    return targets


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


def resolve_workspace_url(
    *,
    profile: str | None = None,
) -> str | None:
    """Resolve the Databricks workspace URL via the CLI.

    Uses ``databricks auth describe`` to discover the workspace
    host.  Returns ``None`` when the CLI is unavailable or the
    command fails.
    """
    if shutil.which("databricks") is None:
        return None

    cmd: list[str] = ["databricks", "auth", "describe", "--output", "json"]
    if profile:
        cmd += ["--profile", profile]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None

    try:
        data = json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError):
        return None

    # The host can appear at the top level or nested under details.
    host = data.get("host")
    if not host:
        details = data.get("details")
        if isinstance(details, dict):
            host = details.get("host")
    if host and isinstance(host, str):
        return host.rstrip("/")
    return None
