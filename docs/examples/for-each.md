# For-Each

Fan-out processing with `@for_each_task` — execute a task once per element in a list.

## Dynamic inputs (from upstream task value)

```python
from databricks_bundle_decorators import job, job_cluster, task, task_value, for_each_task, set_task_value
from databricks_bundle_decorators.io_managers import SparkDeltaIoManager

io = SparkDeltaIoManager(base_path="abfss://lake@account.dfs.core.windows.net/staging")
cluster = job_cluster(name="cluster", spark_version="16.4.x-scala2.12", node_type_id="Standard_E8ds_v4", num_workers=2)

@job(cluster=cluster)
def fan_out_job():
    @task
    def discover_regions():
        set_task_value("regions", ["us-east-1", "eu-west-1", "ap-southeast-1"])

    @task(io_manager=io)
    def load_data():
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        return spark.table("raw.events")

    @for_each_task(inputs=task_value(discover_regions, "regions"), concurrency=3)
    def process(inputs: str, events):
        filtered = events.filter(events.region == inputs)
        print(f"Processing {inputs}: {filtered.count()} rows")

    data = load_data()
    process(events=data)
```

How this works:

- `discover_regions` publishes the iteration list via `set_task_value("regions", ...)`. The key can be **any string** — it must match the second argument to `task_value()`.
- `task_value(discover_regions, "regions")` creates a reference to the `"regions"` task-value on the upstream task. At deploy time this generates `{{tasks.discover_regions.values.regions}}`.
- `events=data` is a regular IoManager data dependency — each iteration receives the full DataFrame from `load_data`. This is wired by **calling** the for-each function inside the `@job` body, the same way as `@task`.

## Static inputs

```python
@job(cluster=cluster)
def static_fan_out():
    @task(io_manager=io)
    def build_report():
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        return spark.createDataFrame([("metric", 100)], ["name", "value"])

    @for_each_task(inputs=["slack", "email", "pagerduty"], concurrency=3)
    def notify(inputs: str, report):
        print(f"[{inputs}] Sending report with {report.count()} rows")

    r = build_report()
    notify(report=r)
```

Static inputs are passed as a plain Python list (must be JSON-serialisable) to the decorator. Other parameters like `report` are regular IoManager data dependencies wired via the function call.
