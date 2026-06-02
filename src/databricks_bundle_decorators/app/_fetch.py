"""SDK-based data fetching for the Databricks App dashboard.

Uses the Databricks Python SDK with credentials auto-injected by the
Databricks Apps runtime (``DATABRICKS_CLIENT_ID`` /
``DATABRICKS_CLIENT_SECRET``).  Job IDs are discovered from the app's
resource bindings via ``WorkspaceClient().apps.get()``.
"""

from __future__ import annotations

import os

from databricks_bundle_decorators.app._data import RunInfo


def resolve_job_ids_from_sdk() -> dict[str, int]:
    """Look up job IDs from the app's resource bindings via the SDK.

    Uses ``DATABRICKS_APP_NAME`` (set by the Databricks Apps runtime)
    to query the app's own configuration and extract job resource
    bindings.  Each resource binding for a job contains the resolved
    job ID.

    Returns
    -------
    dict[str, int]
        Mapping of job name (underscores) to numeric job ID.
        Returns an empty dict if not running in a Databricks App
        or if the SDK call fails.
    """
    app_name = os.environ.get("DATABRICKS_APP_NAME")
    if not app_name:
        return {}

    try:
        from databricks.sdk import WorkspaceClient  # noqa: PLC0415

        w = WorkspaceClient()
        app = w.apps.get(name=app_name)
    except Exception:  # noqa: BLE001
        return {}

    mapping: dict[str, int] = {}
    for resource in app.resources or []:
        if not hasattr(resource, "job") or resource.job is None:
            continue
        # Reverse the name transformation: hyphens → underscores
        job_name = resource.name.replace("-", "_")
        try:
            mapping[job_name] = int(resource.job.id)
        except (ValueError, TypeError, AttributeError):
            continue
    return mapping


def _compute_exec_duration(tasks: list | None) -> float | None:
    """Compute execution duration from task-level timestamps.

    Uses ``end_time - execution_duration`` per task to derive the true
    execution start (excluding queue/pending time), then returns the
    window from the earliest execution start to the latest task end.
    This matches the Databricks UI behavior: real wall-clock from first
    task start to last task finish, excluding queue wait.

    Returns ``None`` if tasks are missing or any task lacks the
    required ``end_time``/``execution_duration`` fields (signalling
    that the caller should fetch authoritative data via ``get_run``).
    """
    if not tasks:
        return None

    exec_starts: list[int] = []
    end_times: list[int] = []

    for task in tasks:
        end_time = getattr(task, "end_time", None)
        exec_duration_ms = getattr(task, "execution_duration", None)
        if end_time is None or exec_duration_ms is None:
            return None
        exec_starts.append(end_time - exec_duration_ms)
        end_times.append(end_time)

    if not exec_starts:
        return None

    exec_window_ms = max(end_times) - min(exec_starts)
    return round(exec_window_ms / 1000.0, 1)


def fetch_job_runs(
    job_id: int,
    *,
    limit: int = 25,
) -> list[RunInfo]:
    """Fetch recent runs for a job via the Databricks SDK.

    Uses ``WorkspaceClient`` with credentials auto-detected from
    the app runtime environment.

    Parameters
    ----------
    job_id:
        Numeric Databricks job ID.
    limit:
        Maximum number of runs to fetch.
    """
    try:
        from databricks.sdk import WorkspaceClient  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "databricks-sdk is required for the Databricks App dashboard. "
            "Install with: uv add databricks-sdk"
        ) from exc

    w = WorkspaceClient()
    runs: list[RunInfo] = []

    try:
        for run in w.jobs.list_runs(job_id=job_id, limit=limit, expand_tasks=True):
            state = run.state
            result_state = (
                state.result_state.value if state and state.result_state else None
            )
            life_cycle_state = (
                state.life_cycle_state.value
                if state and state.life_cycle_state
                else None
            )
            state_message = (state.state_message if state else None) or None

            start_ms = run.start_time
            end_ms = run.end_time

            # Compute duration from task-level execution_duration.
            # The list endpoint's expand_tasks is best-effort and may
            # omit execution_duration; fall back to get_run for the
            # authoritative task data.
            duration = _compute_exec_duration(run.tasks)
            if duration is None and run.run_id:
                try:
                    full_run = w.jobs.get_run(run_id=run.run_id)
                    duration = _compute_exec_duration(full_run.tasks)
                except Exception as _get_err:  # noqa: BLE001
                    print(
                        f"[dbxdec] get_run({run.run_id}) failed: {_get_err!r}",
                        flush=True,
                    )

            backfill_key = None
            for param in run.job_parameters or []:
                if param.name == "backfill_key":
                    backfill_key = param.value
                    break

            runs.append(
                RunInfo(
                    run_id=run.run_id or 0,
                    result_state=result_state,
                    start_time_ms=start_ms,
                    end_time_ms=end_ms,
                    duration_seconds=duration,
                    backfill_key=backfill_key,
                    life_cycle_state=life_cycle_state,
                    state_message=state_message,
                )
            )
    except Exception as exc:  # noqa: BLE001
        print(f"[dbxdec] fetch_job_runs({job_id}) failed: {exc!r}", flush=True)

    return runs


def resolve_workspace_url() -> str | None:
    """Resolve the Databricks workspace URL from the environment.

    The Databricks Apps runtime sets ``DATABRICKS_HOST`` automatically.
    """
    host = os.environ.get("DATABRICKS_HOST")
    if host:
        host = host.rstrip("/")
        if not host.startswith("https://"):
            host = f"https://{host}"
        return host
    return None
