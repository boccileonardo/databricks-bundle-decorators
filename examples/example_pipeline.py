"""Example pipeline – fetches GitHub events and prints the first 10 rows.

Demonstrates the TaskFlow pattern:
- ``@job_cluster`` for shared job-cluster configuration
- ``@task`` defined *inside* the ``@job`` body (inline TaskFlow style)
- ``IoManager`` for DataFrame persistence between tasks
- Explicit task values via ``set_task_value``
- ``params`` for job-level parameter access
- DAG wiring via normal Python call-and-assign
"""

from typing import Any

import polars as pl

from databricks_bundle_decorators import (
    IoManager,
    InputContext,
    OutputContext,
    job,
    job_cluster,
    params,
    task,
    set_task_value,
)


# ---------------------------------------------------------------------------
# IoManager – persist DataFrames as Parquet on Azure Data Lake Storage Gen2
# ---------------------------------------------------------------------------


class AdlsParquetIoManager(IoManager):
    """Read/write Polars DataFrames as Parquet files on ADLS Gen2.

    Polars natively supports ``abfss://`` paths via ``storage_options``.
    Use ``dbutils.secrets.get()`` to retrieve the storage access key
    at runtime on Databricks.

    Parameters
    ----------
    storage_account:
        Azure storage account name.
    container:
        Blob container / filesystem name.
    base_path:
        Folder prefix inside the container (e.g. ``"staging"``).
    storage_options:
        Dict forwarded to ``polars.write_parquet`` /
        ``polars.read_parquet`` – must contain ``account_name``
        and ``account_key``.
    """

    def __init__(
        self,
        storage_account: str,
        container: str,
        base_path: str = "data",
        *,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        self.root = (
            f"abfss://{container}@{storage_account}.dfs.core.windows.net/{base_path}"
        )
        self.storage_options = storage_options or {
            "account_name": storage_account,
            "account_key": self._get_access_key(storage_account),
        }

    @staticmethod
    def _get_access_key(storage_account: str) -> str:
        """Retrieve the ADLS access key from Databricks secrets."""
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(
            scope="KeyVault_Scope", key=f"{storage_account}-access-key"
        )

    def _uri(self, task_key: str) -> str:
        return f"{self.root}/{task_key}.parquet"

    def store(self, context: OutputContext, obj: Any) -> None:
        uri = self._uri(context.task_key)
        obj.write_parquet(uri, storage_options=self.storage_options)
        print(f"[IoManager] Wrote {len(obj)} rows -> {uri}")

    def load(self, context: InputContext) -> Any:
        uri = self._uri(context.upstream_task_key)
        print(f"[IoManager] Reading from {uri}")
        return pl.read_parquet(uri, storage_options=self.storage_options)


staging_io = AdlsParquetIoManager(
    storage_account="mystorageaccount",
    container="datalake",
    base_path="staging",
    # Optionally pass storage_options explicitly:
    # storage_options={"account_name": "...", "account_key": "..."},
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
    cluster="small_cluster",
)
def example_pydab_job():
    @task(io_manager=staging_io)
    def extract():
        """Fetch events from the GitHub API and return a Polars DataFrame."""
        import requests

        r = requests.get(params["url"])
        r.raise_for_status()
        df = pl.DataFrame(r.json())
        # Explicit task value – small scalar, goes via dbutils.jobs.taskValues
        set_task_value("row_count", len(df))
        return df

    @task(io_manager=staging_io)
    def transform(raw_df):
        """Apply filtering/transformations to the raw data."""
        limit = int(params["limit"])
        return raw_df.head(limit)

    @task
    def load(clean_df):
        """Final consumer – print the result."""
        print(f"Loaded {len(clean_df)} rows:")
        print(clean_df)

    raw = extract()
    clean = transform(raw)
    load(clean)
