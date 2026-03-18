# Getting Started

## 1. Scaffold your pipeline project

```bash
uv init my-pipeline && cd my-pipeline
uv add databricks-bundle-decorators[azure]  # or [aws], [gcp], [polars]
uv run dbxdec init
```

!!! tip "Docker deployment"

    If you pre-install your package in a custom Docker image instead of
    deploying a wheel, use `uv run dbxdec init --docker` to generate a
    Docker-ready example.  See [Docker Deployment](guides/docker-deployment.md)
    for details.

`dbxdec init` creates:

| File | Purpose |
|------|---------|
| `resources/__init__.py` | Tells `databricks bundle deploy` about your jobs |
| `src/<package>/pipelines/__init__.py` | Auto-imports all pipeline files in the directory |
| `src/<package>/pipelines/example.py` | Starter pipeline with `@task`, `@job`, `job_cluster()` |
| `databricks.yaml` | Databricks Asset Bundle configuration |
| `pyproject.toml` | Updated with the pipeline package entry point |

## 2. Define your pipeline

```python
# src/my_pipeline/pipelines/github_events.py

from databricks_bundle_decorators import job, job_cluster, params, task, set_task_value, for_each_task
from databricks_bundle_decorators.io_managers import SparkDeltaIoManager

staging_io = SparkDeltaIoManager(
    base_path="abfss://datalake@mystorageaccount.dfs.core.windows.net/staging",
    mode="overwrite",
)

small_cluster = job_cluster(
    name="small_cluster",
    spark_version="16.4.x-scala2.12",
    node_type_id="Standard_E8ds_v4",
    num_workers=1,
)

@job(
    tags={"source": "github", "type": "api"},
    schedule="0 * * * *",
    params={"source_table": "raw.github_events"},
    cluster=small_cluster,
)
def github_events():
    @task(io_manager=staging_io)
    def extract():
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        df = spark.table(params["source_table"])
        set_task_value("row_count", df.count())
        return df

    @task
    def transform(raw_df):
        raw_df.show(10)

    @for_each_task(inputs=["slack", "email", "pagerduty"], concurrency=3)
    def notify(inputs: str, raw_df):
        print(f"Sending alert for {inputs} ({raw_df.count()} rows)")

    df = extract()
    transform(df)
    notify(raw_df=df)
```

??? example "Polars variant"

    ```python
    import polars as pl

    from databricks_bundle_decorators import job, job_cluster, params, task, set_task_value, for_each_task
    from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

    staging_io = PolarsParquetIoManager(
        base_path="abfss://datalake@mystorageaccount.dfs.core.windows.net/staging",
    )

    # ... same cluster and @job definition ...

    def github_events():
        @task(io_manager=staging_io)
        def extract():
            import requests
            r = requests.get(params["url"])
            df = pl.DataFrame(r.json())
            set_task_value("row_count", len(df))
            return df

        @task
        def transform(raw_df):
            print(raw_df.head(10))

        df = extract()
        transform(df)
    ```

!!! tip "Important gotchas"

    The `@job` body runs at **deploy time**, not on a cluster — keep all
    business logic inside `@task` functions. Also avoid reserved parameter
    names (`__job_name__`, `__task_key__`, `__run_id__`, `__upstream__*`).
    See [How It Works — Limitations](how-it-works.md#limitations) for the full list.

## 3. Deploy

```bash
databricks bundle deploy --target dev
```

## Pipeline Discovery

`dbxdec init` sets up pipeline discovery automatically. If you need to
configure it manually (e.g. in an existing project), see
[Discovery](internals/discovery.md) in the Internals section.

## Incremental adoption

You don't have to migrate everything at once. The Databricks CLI merges
resources from all sources — YAML-defined jobs and decorator-defined
jobs coexist in the same bundle. Keep your existing
`resources.jobs` in `databricks.yaml` and add new jobs with decorators:

```yaml
# databricks.yaml
resources:
  jobs:
    existing_job:
      name: existing_job
      tasks:
        - task_key: ingest
          spark_python_task:
            python_file: src/etl/ingest.py

python:
  venv_path: .venv
  resources:
    - 'resources:load_resources'  # new decorator-defined jobs
```

Both sets of jobs deploy together. Migrate at your own pace.
