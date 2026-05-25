"""Explicit task-value helpers (small JSON-serializable data, akin to Airflow XComs).

For *large* data (DataFrames, datasets, etc.) use an
`IoManager` instead.  Task values are intended for small,
JSON-serializable payloads and must be opted-in explicitly by calling
`set_task_value` inside a ``@task`` function.
"""

import json
import os
from typing import Any

type TaskValue = (
    str | int | float | bool | None | list[TaskValue] | dict[str, TaskValue]
)

# Module-level fallback store used during local / test execution.
_local_task_values: dict[str, dict[str, Any]] = {}

# Set by the runtime runner before executing a task, so set_task_value
# can key the local store correctly for cross-task round-trips in tests.
_current_task_key: str | None = None


def _is_databricks_runtime() -> bool:
    """Return ``True`` when running inside a live Databricks job cluster."""
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def set_task_value(key: str, value: TaskValue) -> None:
    """Write a small value into Databricks task values.

    Parameters
    ----------
    key:
        Unique key for the value within this task.
    value:
        Any JSON-serializable value (``str``, ``int``, ``float``, ``bool``,
        ``None``, ``list``, or ``dict``).

    Raises
    ------
    TypeError
        If *value* is not JSON-serializable.
    """
    try:
        json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(
            f"TaskValues must be JSON-serializable, "
            f"got {type(value).__name__}: {exc}. "
            f"Use an IoManager for complex data."
        ) from exc

    if _is_databricks_runtime():
        # On Databricks: let any API/permission error propagate so that
        # real runtime failures are visible rather than silently swallowed.
        from pyspark.dbutils import (  # noqa: PLC0415  # ty: ignore[unresolved-import]
            DBUtils,  # Databricks-only
        )
        from pyspark.sql import SparkSession  # noqa: PLC0415

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key=key, value=value)
    else:
        # Local / testing fallback — use the current task key if set by
        # the runtime runner, otherwise fall back to "__current__".
        store_key = _current_task_key or "__current__"
        _local_task_values.setdefault(store_key, {})[key] = value


_MISSING = object()


def get_task_value(task_key: str, key: str, *, default: Any = _MISSING) -> Any:
    """Read a value previously written by an upstream task.

    Parameters
    ----------
    task_key:
        The ``task_key`` of the upstream task that called `set_task_value`.
    key:
        The key passed to `set_task_value`.
    default:
        Value to return when the key does not exist.  If omitted, raises
        ``ValueError`` on Databricks (matching the platform behaviour) or
        returns ``None`` locally.
    """
    if _is_databricks_runtime():
        # On Databricks: let any API/permission error propagate.
        from pyspark.dbutils import (  # noqa: PLC0415  # ty: ignore[unresolved-import]
            DBUtils,  # Databricks-only
        )
        from pyspark.sql import SparkSession  # noqa: PLC0415

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        if default is _MISSING:
            return dbutils.jobs.taskValues.get(taskKey=task_key, key=key)
        return dbutils.jobs.taskValues.get(taskKey=task_key, key=key, default=default)
    if default is not _MISSING:
        return _local_task_values.get(task_key, {}).get(key, default)
    return _local_task_values.get(task_key, {}).get(key)


#: Internal task-value key used to propagate partition values between tasks.
_PARTITION_VALUES_KEY: str = "__partition_values__"


def get_upstream_partition_values(task_key: str) -> dict[str, list[str]] | None:
    """Return the partition values written by an upstream task.

    When an IoManager with ``auto_filter=True`` writes partitioned data,
    the framework pushes the distinct partition column values as an
    internal task value.  This function retrieves those values, letting
    downstream task code inspect which partitions were produced without
    accessing internal task-value keys directly.

    Parameters
    ----------
    task_key:
        The ``task_key`` of the upstream task whose partition values
        you want to inspect.

    Returns
    -------
    dict[str, list[str]] | None
        A mapping of partition column names to their distinct values
        (e.g. ``{"date": ["2024-01-15"], "region": ["us", "eu"]}``),
        or ``None`` if the upstream task did not push partition values
        (e.g. it has no IoManager, ``auto_filter=False``, or no
        ``partition_by``).

    Example
    -------
    ::

        from databricks_bundle_decorators import (
            task,
            job,
            get_upstream_partition_values,
        )


        @job
        def my_pipeline():
            @task(io_manager=my_io)
            def produce(): ...

            @task
            def consume(data):
                parts = get_upstream_partition_values("produce")
                # parts == {"date": ["2024-01-15"]}
                ...

            consume(produce())
    """
    return get_task_value(task_key, _PARTITION_VALUES_KEY, default=None)
