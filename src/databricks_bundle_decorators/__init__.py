"""databricks-bundle-decorators - decorator-based Databricks job/task framework.

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

from databricks_bundle_decorators.app._codegen import (
    generate_app_config_yaml as generate_app_config_yaml,
)
from databricks_bundle_decorators.app._codegen import (
    generate_app_resource as generate_app_resource,
)
from databricks_bundle_decorators.backfill import (
    BackfillDef as BackfillDef,
)
from databricks_bundle_decorators.backfill import (
    DailyBackfill as DailyBackfill,
)
from databricks_bundle_decorators.backfill import (
    HourlyBackfill as HourlyBackfill,
)
from databricks_bundle_decorators.backfill import (
    MonthlyBackfill as MonthlyBackfill,
)
from databricks_bundle_decorators.backfill import (
    StaticBackfill as StaticBackfill,
)
from databricks_bundle_decorators.backfill import (
    WeeklyBackfill as WeeklyBackfill,
)
from databricks_bundle_decorators.backfill import (
    get_backfill_key as get_backfill_key,
)
from databricks_bundle_decorators.backfill import (
    get_run_logical_date as get_run_logical_date,
)
from databricks_bundle_decorators.codegen import (
    generate_resources as generate_resources,
)
from databricks_bundle_decorators.context import get_dbutils as get_dbutils
from databricks_bundle_decorators.context import params as params
from databricks_bundle_decorators.dashboard import run_app as run_app
from databricks_bundle_decorators.decorators import TaskProxy as TaskProxy
from databricks_bundle_decorators.decorators import all_partitions as all_partitions
from databricks_bundle_decorators.decorators import for_each_task as for_each_task
from databricks_bundle_decorators.decorators import job as job
from databricks_bundle_decorators.decorators import job_cluster as job_cluster
from databricks_bundle_decorators.decorators import task as task
from databricks_bundle_decorators.decorators import task_value as task_value
from databricks_bundle_decorators.discovery import (
    discover_pipelines as discover_pipelines,
)
from databricks_bundle_decorators.io_manager import InputContext as InputContext
from databricks_bundle_decorators.io_manager import IoManager as IoManager
from databricks_bundle_decorators.io_manager import OutputContext as OutputContext
from databricks_bundle_decorators.registry import (
    ClusterMeta as ClusterMeta,
)
from databricks_bundle_decorators.registry import (
    DuplicateResourceError as DuplicateResourceError,
)
from databricks_bundle_decorators.registry import (
    TaskValueRef as TaskValueRef,
)
from databricks_bundle_decorators.sdk_types import ClusterConfig as ClusterConfig
from databricks_bundle_decorators.sdk_types import JobConfig as JobConfig
from databricks_bundle_decorators.sdk_types import TaskConfig as TaskConfig
from databricks_bundle_decorators.task_values import TaskValue as TaskValue
from databricks_bundle_decorators.task_values import get_task_value as get_task_value
from databricks_bundle_decorators.task_values import set_task_value as set_task_value

__all__ = [
    "BackfillDef",
    "ClusterConfig",
    "ClusterMeta",
    "DailyBackfill",
    "DuplicateResourceError",
    "HourlyBackfill",
    "InputContext",
    "IoManager",
    "JobConfig",
    "MonthlyBackfill",
    "OutputContext",
    "StaticBackfill",
    "TaskConfig",
    "TaskProxy",
    "TaskValue",
    "TaskValueRef",
    "WeeklyBackfill",
    "all_partitions",
    "discover_pipelines",
    "for_each_task",
    "generate_app_config_yaml",
    "generate_app_resource",
    "generate_resources",
    "get_backfill_key",
    "get_dbutils",
    "get_run_logical_date",
    "get_task_value",
    "job",
    "job_cluster",
    "params",
    "run_app",
    "set_task_value",
    "task",
    "task_value",
]
