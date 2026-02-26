"""Explicit task-value helpers (small scalar data, akin to Airflow XComs).

For *large* data (DataFrames, datasets, etc.) use an
`IoManager` instead.  Task values are limited to
small primitive payloads (~48 KB) and must be opted-in explicitly by calling
`set_task_value` inside a ``@task`` function.
"""

import os
from typing import Any

# Module-level fallback store used during local / test execution.
_local_task_values: dict[str, dict[str, Any]] = {}

# Set by the runtime runner before executing a task, so set_task_value
# can key the local store correctly for cross-task round-trips in tests.
_current_task_key: str | None = None


def _is_databricks_runtime() -> bool:
    """Return ``True`` when running inside a live Databricks job cluster."""
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def set_task_value(key: str, value: str | int | float | bool) -> None:
    """Write a small value into Databricks task values.

    Parameters
    ----------
    key:
        Unique key for the value within this task.
    value:
        A primitive (``str``, ``int``, ``float``, ``bool``).

    Raises
    ------
    TypeError
        If *value* is not a supported primitive type.
    """
    if not isinstance(value, (str, int, float, bool)):
        raise TypeError(
            f"TaskValues only support primitive types (str, int, float, bool), "
            f"got {type(value).__name__}. Use an IoManager for complex data."
        )

    if _is_databricks_runtime():
        # On Databricks: let any API/permission error propagate so that
        # real runtime failures are visible rather than silently swallowed.
        from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]
        from pyspark.sql import SparkSession  # type: ignore[import-untyped]

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key=key, value=value)
    else:
        # Local / testing fallback — use the current task key if set by
        # the runtime runner, otherwise fall back to "__current__".
        store_key = _current_task_key or "__current__"
        _local_task_values.setdefault(store_key, {})[key] = value


def get_task_value(task_key: str, key: str) -> Any:
    """Read a value previously written by an upstream task.

    Parameters
    ----------
    task_key:
        The ``task_key`` of the upstream task that called `set_task_value`.
    key:
        The key passed to `set_task_value`.
    """
    if _is_databricks_runtime():
        # On Databricks: let any API/permission error propagate.
        from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]
        from pyspark.sql import SparkSession  # type: ignore[import-untyped]

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        return dbutils.jobs.taskValues.get(taskKey=task_key, key=key)
    else:
        return _local_task_values.get(task_key, {}).get(key)
