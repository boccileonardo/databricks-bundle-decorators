"""databricks-bundle-decorators – decorator-based Databricks job/task framework.

Public API
----------
Decorators:
    ``@task``, ``@job``, ``@for_each_task``

Cluster configuration:
    ``job_cluster()``

DAG wiring:
    ``TaskProxy`` — returned by ``@task`` calls inside ``@job`` bodies;
    used for data dependencies (pass as args) or control-flow
    dependencies (pass to ``depends_on``).

    ``task_value()`` — creates a ``TaskValueRef`` referencing a specific
    task-value from an upstream task; used with
    ``@for_each_task(inputs=...)``.

Data management:
    ``IoManager``, ``OutputContext``, ``InputContext``

Cross-partition reads:
    ``all_partitions()`` — wrap a ``TaskProxy`` to read all partitions
    from that upstream dependency instead of only the current one.
    Alternatively, use ``@task(all_partitions=True)`` to apply to
    all upstream data dependencies of a task.

Task values (small JSON-serializable data):
    ``set_task_value``, ``get_task_value``, ``TaskValue``

Databricks utilities:
    ``get_dbutils``

Job parameters:
    ``params``
"""

from databricks_bundle_decorators.context import get_dbutils as get_dbutils
from databricks_bundle_decorators.context import params as params
from databricks_bundle_decorators.decorators import all_partitions as all_partitions
from databricks_bundle_decorators.decorators import for_each_task as for_each_task
from databricks_bundle_decorators.decorators import job as job
from databricks_bundle_decorators.decorators import job_cluster as job_cluster
from databricks_bundle_decorators.decorators import task as task
from databricks_bundle_decorators.decorators import task_value as task_value
from databricks_bundle_decorators.decorators import TaskProxy as TaskProxy
from databricks_bundle_decorators.discovery import (
    discover_pipelines as discover_pipelines,
)
from databricks_bundle_decorators.io_manager import InputContext as InputContext
from databricks_bundle_decorators.io_manager import IoManager as IoManager
from databricks_bundle_decorators.io_manager import OutputContext as OutputContext
from databricks_bundle_decorators.codegen import (
    generate_resources as generate_resources,
)
from databricks_bundle_decorators.backfill import (
    get_backfill_key as get_backfill_key,
    get_run_logical_date as get_run_logical_date,
    DailyBackfill as DailyBackfill,
    HourlyBackfill as HourlyBackfill,
    MonthlyBackfill as MonthlyBackfill,
    BackfillDef as BackfillDef,
    StaticBackfill as StaticBackfill,
    WeeklyBackfill as WeeklyBackfill,
)
from databricks_bundle_decorators.registry import (
    ClusterMeta as ClusterMeta,
    DuplicateResourceError as DuplicateResourceError,
    TaskValueRef as TaskValueRef,
)
from databricks_bundle_decorators.sdk_types import ClusterConfig as ClusterConfig
from databricks_bundle_decorators.sdk_types import JobConfig as JobConfig
from databricks_bundle_decorators.sdk_types import TaskConfig as TaskConfig
from databricks_bundle_decorators.dashboard import run_app as run_app
from databricks_bundle_decorators.task_values import get_task_value as get_task_value
from databricks_bundle_decorators.task_values import set_task_value as set_task_value
from databricks_bundle_decorators.task_values import TaskValue as TaskValue

__all__ = [
    "all_partitions",
    "task",
    "job",
    "job_cluster",
    "for_each_task",
    "task_value",
    "TaskProxy",
    "TaskValueRef",
    "discover_pipelines",
    "IoManager",
    "OutputContext",
    "InputContext",
    "ClusterMeta",
    "DuplicateResourceError",
    "ClusterConfig",
    "JobConfig",
    "TaskConfig",
    "set_task_value",
    "get_task_value",
    "TaskValue",
    "get_dbutils",
    "params",
    "BackfillDef",
    "DailyBackfill",
    "HourlyBackfill",
    "MonthlyBackfill",
    "WeeklyBackfill",
    "StaticBackfill",
    "get_backfill_key",
    "get_run_logical_date",
    "run_app",
    "generate_resources",
]
