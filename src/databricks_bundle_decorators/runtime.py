"""Runtime entry-point for ``python_wheel_task`` execution on Databricks.

Databricks invokes the ``dbxdec-run`` console-script, which calls
`main`.  The function:

1. Imports the user's pipeline definitions (populating the registries).
2. Parses CLI ``--key=value`` arguments produced by ``named_parameters``.
3. Resolves upstream data via `IoManager`.
4. Executes the task function.
5. Persists the return value via the task's ``IoManager`` (if configured).
"""

import argparse
import os
import sys
import typing
from typing import Any

from databricks_bundle_decorators.context import _populate_params
from databricks_bundle_decorators.io_manager import InputContext, OutputContext
from databricks_bundle_decorators.registry import _TASK_REGISTRY


def _parse_named_args(argv: list[str]) -> dict[str, str]:
    """Parse ``--key=value`` pairs from an argv list into a dict."""
    result: dict[str, str] = {}
    for arg in argv:
        if arg.startswith("--") and "=" in arg:
            key, value = arg[2:].split("=", 1)
            result[key] = value
    return result


def run_task(task_key: str, cli_params: dict[str, str]) -> None:
    """Execute a single registered task, wiring IoManagers and params.

    Parameters
    ----------
    task_key:
        The registry key of the task to run (function name).
    cli_params:
        All ``--key=value`` arguments received from the Databricks
        ``python_wheel_task`` invocation.
    """
    # ---- extract internal parameters -------------------------------------
    job_name = cli_params.pop("__job_name__", "unknown")
    cli_params.pop("__task_key__", None)
    run_id = cli_params.pop("__run_id__", os.environ.get("DATABRICKS_RUN_ID", "local"))

    # ---- extract upstream mappings (__upstream__<param>=<upstream_task>) ---
    upstream_map: dict[str, str] = {}
    for key in list(cli_params):
        if key.startswith("__upstream__"):
            param_name = key[len("__upstream__") :]
            upstream_map[param_name] = cli_params.pop(key)

    # ---- remaining keys are job-level parameters -------------------------
    _populate_params(cli_params)

    # ---- look up task metadata -------------------------------------------
    # Try qualified key (job_name.task_key) first, then short key.
    qualified_key = f"{job_name}.{task_key}"
    task_meta = _TASK_REGISTRY.get(qualified_key) or _TASK_REGISTRY.get(task_key)
    if task_meta is None:
        print(f"Error: Task '{task_key}' not found in registry.", file=sys.stderr)
        print(f"Tried: '{qualified_key}', '{task_key}'", file=sys.stderr)
        print(f"Available: {list(_TASK_REGISTRY.keys())}", file=sys.stderr)
        sys.exit(1)

    # ---- resolve type hints for the current task's parameters ------------
    try:
        type_hints = typing.get_type_hints(task_meta.fn)
    except Exception:  # noqa: BLE001 – graceful fallback
        type_hints = {}

    # ---- resolve upstream data via IoManager.read() ----------------------
    kwargs: dict[str, Any] = {}
    for param_name, upstream_task_key in upstream_map.items():
        upstream_qualified = f"{job_name}.{upstream_task_key}"
        upstream_meta = _TASK_REGISTRY.get(upstream_qualified) or _TASK_REGISTRY.get(
            upstream_task_key
        )
        if upstream_meta and upstream_meta.io_manager:
            upstream_meta.io_manager._ensure_setup()
            context = InputContext(
                job_name=job_name,
                task_key=task_key,
                upstream_task_key=upstream_task_key,
                run_id=run_id,
                expected_type=type_hints.get(param_name),
            )
            kwargs[param_name] = upstream_meta.io_manager.read(context)
        else:
            print(
                f"Warning: upstream task '{upstream_task_key}' has no IoManager – "
                f"cannot auto-load data for parameter '{param_name}'.",
                file=sys.stderr,
            )

    # ---- execute the task function ---------------------------------------
    from databricks_bundle_decorators import task_values as _tv

    _tv._current_task_key = task_key
    try:
        result = task_meta.fn(**kwargs)
    finally:
        _tv._current_task_key = None

    # ---- persist output via IoManager.write() ----------------------------
    if result is not None and task_meta.io_manager:
        task_meta.io_manager._ensure_setup()
        context = OutputContext(
            job_name=job_name,
            task_key=task_key,
            run_id=run_id,
        )
        task_meta.io_manager.write(context, result)
    elif result is not None and not task_meta.io_manager:
        print(
            f"Warning: task '{task_key}' returned a value but has no IoManager – "
            f"the return value will be discarded.",
            file=sys.stderr,
        )


def main() -> None:
    """Console-script entry-point (``dbxdec-run``).

    Invoked by Databricks ``python_wheel_task`` with ``named_parameters``.
    """
    # 1. Discover and import pipeline definitions to populate registries.
    #    Pipeline packages register themselves via the 'databricks_bundle_decorators.pipelines'
    #    entry-point group in their pyproject.toml.
    from databricks_bundle_decorators.discovery import discover_pipelines

    discover_pipelines()

    # 2. Parse CLI arguments.
    parser = argparse.ArgumentParser(description="dbxdec task runner")
    args, remaining = parser.parse_known_args()
    cli_params = _parse_named_args(remaining)

    task_key = cli_params.get("__task_key__")
    if not task_key:
        print("Error: --__task_key__=<name> is required.", file=sys.stderr)
        sys.exit(1)

    # 3. Run the task.
    run_task(task_key, cli_params)
