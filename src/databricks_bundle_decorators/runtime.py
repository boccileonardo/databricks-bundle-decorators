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
import json
import logging
import os
import typing
from datetime import datetime, timezone
from typing import Any

from databricks_bundle_decorators.context import _populate_params
from databricks_bundle_decorators.io_manager import (
    InputContext,
    OutputContext,
)
from databricks_bundle_decorators.backfill import _parse_logical_date_str
from databricks_bundle_decorators.registry import _TASK_REGISTRY

_logger = logging.getLogger(__name__)


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
    all_partitions_params: set[str] = set()
    for key in list(cli_params):
        if key.startswith("__upstream__"):
            param_name = key[len("__upstream__") :]
            upstream_map[param_name] = cli_params.pop(key)
        elif key.startswith("__all_partitions__"):
            param_name = key[len("__all_partitions__") :]
            all_partitions_params.add(param_name)
            cli_params.pop(key)

    # ---- extract for-each input (if present) -----------------------------
    for_each_input_raw: str | None = cli_params.pop("__for_each_input__", None)

    # ---- remaining keys are job-level parameters -------------------------
    _populate_params(cli_params)

    # ---- look up task metadata -------------------------------------------
    qualified_key = f"{job_name}.{task_key}"
    task_meta = _TASK_REGISTRY.get(qualified_key)
    if task_meta is None:
        available = list(_TASK_REGISTRY.keys())
        raise RuntimeError(
            f"Task '{task_key}' not found by qualified key '{qualified_key}'. "
            f"Available: {available}"
        )

    # ---- resolve logical_date ------------------------------------------
    # logical_date is available when the job has a backfill definition or
    # the user added the param explicitly.  When the param is present but
    # empty, fall back to the current UTC time.
    _raw_ld = cli_params.get("logical_date", "")
    if _raw_ld:
        logical_date: datetime | None = _parse_logical_date_str(_raw_ld)
    elif "logical_date" in cli_params:
        logical_date = datetime.now(tz=timezone.utc)
    else:
        logical_date = None

    # ---- resolve type hints for expected_type ----------------------------
    try:
        type_hints = typing.get_type_hints(task_meta.fn)
    except Exception:  # noqa: BLE001 – graceful fallback
        type_hints = {}

    # ---- resolve upstream data via IoManager.read() ----------------------
    kwargs: dict[str, Any] = {}
    for param_name, upstream_task_key in upstream_map.items():
        upstream_qualified = f"{job_name}.{upstream_task_key}"
        upstream_meta = _TASK_REGISTRY.get(upstream_qualified)
        if upstream_meta and upstream_meta.io_manager:
            upstream_meta.io_manager._ensure_setup()

            # Retrieve partition filter from upstream task values
            partition_filter: dict[str, list[str]] | None = None
            is_all_partitions = param_name in all_partitions_params
            if (
                upstream_meta.io_manager.auto_filter
                and upstream_meta.partition_by
                and not is_all_partitions
            ):
                from databricks_bundle_decorators.task_values import get_task_value

                partition_filter = get_task_value(
                    upstream_task_key, "__partition_values__"
                )
            elif (
                not upstream_meta.io_manager.auto_filter
                and upstream_meta.partition_by
                and not is_all_partitions
            ):
                non_ld = [c for c in upstream_meta.partition_by if c != "logical_date"]
                if non_ld:
                    _logger.warning(
                        "IoManager for task '%s' has auto_filter=False. "
                        "Downstream reads for partition columns %s will "
                        "not be filtered automatically. Use "
                        "all_partitions() to suppress this warning, or "
                        "filter manually in your task code.",
                        upstream_task_key,
                        non_ld,
                    )

            context = InputContext(
                job_name=job_name,
                task_key=task_key,
                upstream_task_key=upstream_task_key,
                run_id=run_id,
                expected_type=type_hints.get(param_name),
                logical_date=logical_date,
                all_partitions=is_all_partitions,
                partition_by=upstream_meta.partition_by,
                partition_filter=partition_filter,
            )
            kwargs[param_name] = upstream_meta.io_manager.read(context)
        else:
            _logger.warning(
                "Upstream task '%s' has no IoManager – "
                "cannot auto-load data for parameter '%s'.",
                upstream_task_key,
                param_name,
            )

    # ---- inject for-each input element -----------------------------------
    if for_each_input_raw is not None:
        try:
            kwargs["inputs"] = json.loads(for_each_input_raw)
        except (json.JSONDecodeError, TypeError):
            # Not valid JSON — pass as a plain string
            kwargs["inputs"] = for_each_input_raw

    # ---- execute the task function ---------------------------------------
    from databricks_bundle_decorators import task_values as _tv

    _tv._current_task_key = task_key
    try:
        result = task_meta.fn(**kwargs)

        # ---- persist output via IoManager.write() ------------------------
        if result is not None and task_meta.io_manager:
            task_meta.io_manager._ensure_setup()
            context = OutputContext(
                job_name=job_name,
                task_key=task_key,
                run_id=run_id,
                logical_date=logical_date,
                partition_by=task_meta.partition_by,
            )
            task_meta.io_manager.write(context, result)

            # Push partition values for downstream auto-filtering
            if task_meta.io_manager.auto_filter and task_meta.partition_by:
                partition_values = task_meta.io_manager._extract_partition_values(
                    context
                )
                from databricks_bundle_decorators.task_values import set_task_value

                set_task_value("__partition_values__", partition_values)  # type: ignore[arg-type]
        elif result is not None and not task_meta.io_manager:
            _logger.warning(
                "Task '%s' returned a value but has no IoManager – "
                "the return value will be discarded.",
                task_key,
            )
    finally:
        _tv._current_task_key = None


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
        raise RuntimeError("--__task_key__=<name> is required.")

    # 3. Run the task.
    run_task(task_key, cli_params)
