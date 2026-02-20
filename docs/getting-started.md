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
    Docker-ready example.  See [Docker Deployment](docker-deployment.md)
    for details.

`dbxdec init` creates:

| File | Purpose |
|------|---------|
| `resources/__init__.py` | `load_resources()` entry point for `databricks bundle deploy` |
| `src/<package>/pipelines/__init__.py` | Auto-discovery module that imports all pipeline files |
| `src/<package>/pipelines/example.py` | Starter pipeline with `@task`, `@job`, `job_cluster()` |
| `databricks.yaml` | Databricks Asset Bundle configuration (if not present) |
| `pyproject.toml` | Updated with the pipeline package entry point |

## 2. Define your pipeline

```python
# src/my_pipeline/pipelines/github_events.py

import polars as pl

from databricks_bundle_decorators import job, job_cluster, params, task, set_task_value
from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

staging_io = PolarsParquetIoManager(
    base_path="abfss://datalake@mystorageaccount.dfs.core.windows.net/staging",
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
    params={"url": "https://api.github.com/events"},
    cluster=small_cluster,
)
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

## 3. Deploy

```bash
databricks bundle deploy --target dev
```

## Pipeline Discovery

Pipeline packages register via [entry points](https://packaging.python.org/en/latest/specifications/entry-points/):

```toml
[project.entry-points."databricks_bundle_decorators.pipelines"]
my_pipeline = "my_pipeline.pipelines"
```

The referenced module should import all modules containing `@task`/`@job` decorators:

```python
# my_pipeline/pipelines/__init__.py
import importlib, pkgutil

for _loader, _name, _is_pkg in pkgutil.walk_packages(__path__):
    importlib.import_module(f"{__name__}.{_name}")
```
