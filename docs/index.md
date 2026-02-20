# databricks-bundle-decorators

Decorator-based framework for defining Databricks jobs and tasks as Python code. Define pipelines using `@task`, `@job`, and `job_cluster()` — they compile into [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/python/) resources.

## Why databricks-bundle-decorators?

Writing Databricks jobs in raw YAML is tedious and disconnects task logic from orchestration configuration. databricks-bundle-decorators lets you express both in Python:

- **Airflow TaskFlow-inspired pattern** — define `@task` functions inside a `@job` body; dependencies are captured automatically from call arguments.
- **IoManager pattern** — large data (DataFrames, datasets) flows through permanent storage (Delta tables, Unity Catalog volumes) — multi-hop architecture.
- **Explicit task values** — small scalars (`str`, `int`, `float`, `bool`) can be passed between tasks via `set_task_value` / `get_task_value`, like Airflow XComs.
- **Deploy-time codegen** — `databricks bundle deploy` imports your Python files, discovers all `@job`/`@task` definitions, and generates Databricks Job configurations.
- **Runtime dispatch** — each task executes on a cluster via the `dbxdec-run` entry point, which loads upstream data through IoManagers and calls your task function.

## Installation

```bash
uv add databricks-bundle-decorators
```

With cloud-specific extras:

```bash
uv add databricks-bundle-decorators[azure]  # or [aws], [gcp], [polars]
```

## Quick Example

```python
from databricks_bundle_decorators import job, job_cluster, params, task
from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

io = PolarsParquetIoManager(base_path="abfss://lake@account.dfs.core.windows.net/staging")

small_cluster = job_cluster(name="small", spark_version="16.4.x-scala2.12", node_type_id="Standard_E8ds_v4", num_workers=1)

@job(schedule="0 * * * *", cluster=small_cluster)
def my_pipeline():
    @task(io_manager=io)
    def extract():
        import polars as pl
        return pl.DataFrame({"x": [1, 2, 3]})

    @task
    def transform(df):
        print(df.head())

    data = extract()
    transform(data)
```
