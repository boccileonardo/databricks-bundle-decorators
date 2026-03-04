# Basic ETL

A Spark ETL pipeline with IoManager-based data flow between tasks.

```python
from databricks_bundle_decorators import job, job_cluster, params, task, set_task_value, get_task_value
from databricks_bundle_decorators.io_managers import SparkDeltaIoManager

io = SparkDeltaIoManager(
    base_path="abfss://lake@account.dfs.core.windows.net/staging",
    mode="overwrite",
)

cluster = job_cluster(
    name="etl_cluster",
    spark_version="16.4.x-scala2.12",
    node_type_id="Standard_E8ds_v4",
    num_workers=2,
)

@job(
    cluster=cluster,
    params={"source_table": "raw.events", "limit": "1000"},
    tags={"team": "data-eng"},
    max_concurrent_runs=1,
)
def etl_pipeline():
    @task(io_manager=io)
    def extract():
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        df = spark.table(params["source_table"]).limit(int(params["limit"]))
        set_task_value("row_count", df.count())
        return df

    @task(io_manager=io)
    def transform(raw):
        from pyspark.sql import functions as F
        return raw.dropDuplicates(["id"]).withColumn("processed_at", F.current_timestamp())

    @task
    def load(clean):
        row_count = get_task_value("extract", "row_count")
        print(f"Expected {row_count} raw rows, loaded {clean.count()} clean rows")

    raw = extract()
    clean = transform(raw)
    load(clean)
```

Key points:

- `@task(io_manager=io)` on the **producer** — downstream tasks receive the data as a plain argument
- `params` are accessible inside any task at runtime
- `set_task_value` / `get_task_value` pass small scalars (row counts, flags) between tasks
- SDK fields like `max_concurrent_runs` pass through directly to the Job spec
