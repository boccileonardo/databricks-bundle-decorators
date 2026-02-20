"""Example pipeline – fetches GitHub events and prints the first 10 rows.

Demonstrates the TaskFlow pattern:
- ``@job_cluster`` for shared job-cluster configuration
- ``@task`` defined *inside* the ``@job`` body (inline TaskFlow style)
- Built-in ``PolarsParquetIoManager`` for DataFrame persistence between tasks
- ``get_dbutils`` for accessing secrets at runtime
- Explicit task values via ``set_task_value``
- ``params`` for job-level parameter access
- DAG wiring via normal Python call-and-assign
"""

import polars as pl

from databricks_bundle_decorators import (
    get_dbutils,
    job,
    job_cluster,
    params,
    set_task_value,
    task,
)
from databricks_bundle_decorators.io_managers import PolarsParquetIoManager


# ---------------------------------------------------------------------------
# IoManager – persist DataFrames as Parquet (works with any cloud or local path)
# ---------------------------------------------------------------------------


def _storage_options() -> dict[str, str]:
    """Resolve cloud storage credentials lazily at runtime.

    This callable is invoked by the IoManager only when reading/writing
    data on a Databricks cluster — never during local ``bundle deploy``.
    """
    dbutils = get_dbutils()
    key = dbutils.secrets.get(scope="my_scope", key="storage-access-key")
    return {"account_name": "mystorageaccount", "account_key": key}


staging_io = PolarsParquetIoManager(
    base_path="abfss://datalake@mystorageaccount.dfs.core.windows.net/staging",
    storage_options=_storage_options,
)


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
    params={"url": "https://api.github.com/events", "limit": "10"},
    cluster=small_cluster,
)
def example_pydab_job():
    @task(io_manager=staging_io)
    def extract() -> pl.DataFrame:
        """Fetch events from the GitHub API and return a Polars DataFrame."""
        import requests

        r = requests.get(params["url"])
        r.raise_for_status()
        df = pl.DataFrame(r.json())
        # Explicit task value – small scalar, goes via dbutils.jobs.taskValues
        set_task_value("row_count", len(df))
        return df

    @task(io_manager=staging_io)
    def transform(raw_df: pl.DataFrame) -> pl.DataFrame:
        """Apply filtering/transformations to the raw data."""
        limit = int(params["limit"])
        return raw_df.head(limit)

    @task
    def summarize(clean_df: pl.DataFrame) -> None:
        """Final consumer – print the result."""
        print(f"Loaded {len(clean_df)} rows:")
        print(clean_df)

    raw = extract()
    clean = transform(raw)
    summarize(clean)
