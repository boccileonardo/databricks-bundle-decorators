"""Example pipeline – fetches GitHub events and prints the first 10 rows.

Demonstrates the TaskFlow pattern:
- ``@job_cluster`` for shared job-cluster configuration
- ``@task`` defined *inside* the ``@job`` body (inline TaskFlow style)
- ``IoManager`` for DataFrame persistence between tasks
- Explicit task values via ``set_task_value``
- DAG wiring via normal Python call-and-assign
"""

from __future__ import annotations

from typing import Any

import polars as pl
import requests

from databricks_bundle_decorators import IoManager, InputContext, OutputContext, params
from databricks_bundle_decorators.decorators import job, job_cluster, task
from databricks_bundle_decorators.task_values import set_task_value


# ---------------------------------------------------------------------------
# IoManager – users implement one per storage target
# ---------------------------------------------------------------------------


class DeltaIoManager(IoManager):
    """Persist Polars DataFrames as Delta tables.

    In a real deployment the ``store`` / ``load`` methods would use
    ``polars.write_delta`` / ``polars.read_delta`` (backed by
    ``deltalake``).  This example uses workspace-local Parquet files as a
    placeholder so the pipeline can be tested without a running Spark
    cluster.
    """

    def __init__(self, catalog: str, schema: str) -> None:
        self.catalog = catalog
        self.schema = schema

    def _table_path(self, task_key: str) -> str:
        return f"{self.catalog}.{self.schema}.{task_key}"

    def store(self, context: OutputContext, obj: Any) -> None:
        table = self._table_path(context.task_key)
        # In production: obj.write_delta(table, mode="overwrite")
        print(f"[IoManager] Storing {len(obj)} rows → {table}")

    def load(self, context: InputContext) -> Any:
        table = self._table_path(context.upstream_task_key)
        # In production: return pl.read_delta(table)
        print(f"[IoManager] Loading from {table}")
        return pl.DataFrame()


staging_io = DeltaIoManager(catalog="main", schema="staging")


# ---------------------------------------------------------------------------
# Job cluster
# ---------------------------------------------------------------------------

small_cluster = job_cluster(
    name="small_cluster",
    spark_version="16.4.x-scala2.12",
    node_type_id="Standard_E8ds_v4",
    num_workers=1,
)

# ---------------------------------------------------------------------------
# Job – tasks are defined inline (TaskFlow style)
# ---------------------------------------------------------------------------


@job(
    tags={"source": "github", "type": "api"},
    params={"url": "https://api.github.com/events"},
    cluster="small_cluster",
)
def example_pydab_job():
    @task(io_manager=staging_io)
    def upstream():
        """Fetch events from the GitHub API and return a Polars DataFrame."""
        r = requests.get(params["url"])
        df = pl.DataFrame(r.json())
        # Explicit task value – small scalar, goes via dbutils.jobs.taskValues
        set_task_value("row_count", len(df))
        return df

    @task()
    def downstream(df):
        """Print the first rows received from upstream."""
        print(df.head(10))

    df = upstream()
    downstream(df)
