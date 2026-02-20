# databricks-bundle-decorators

Decorator-based framework for defining Databricks jobs and tasks as Python code. Define pipelines using `@task`, `@job`, and `job_cluster()` — they compile into [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/python/) resources.

## Why databricks-bundle-decorators?

Writing Databricks jobs in raw YAML is tedious and disconnects task logic from orchestration configuration. databricks-bundle-decorators lets you express both in Python:

- **Airflow TaskFlow-inspired pattern** — define `@task` functions inside a `@job` body; dependencies are captured automatically from call arguments.
- **IoManager pattern** — large data (DataFrames, datasets) flows between tasks through external storage automatically.
- **Explicit task values** — small scalars (`str`, `int`, `float`, `bool`) can be passed between tasks via `set_task_value` / `get_task_value`, like Airflow XComs.
- **Pure Python** — write your jobs and tasks as decorated functions, run `databricks bundle deploy`, and the framework generates all Databricks Job configurations for you.

## Installation

```bash
uv add databricks-bundle-decorators
```

With cloud-specific extras for the built-in `PolarsParquetIoManager`:

```bash
uv add databricks-bundle-decorators[azure]  # or [aws], [gcp], [polars]
```

## Quickstart

```bash
uv init my-pipeline && cd my-pipeline
uv add databricks-bundle-decorators[azure]
uv run dbxdec init
```

This scaffolds a complete pipeline project. Define your jobs in `src/<package>/pipelines/`:

```python
import polars as pl

from databricks_bundle_decorators import job, job_cluster, params, task
from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

io = PolarsParquetIoManager(
    base_path="abfss://lake@account.dfs.core.windows.net/staging",
)

cluster = job_cluster(
    name="small",
    spark_version="16.4.x-scala2.12",
    node_type_id="Standard_E8ds_v4",
    num_workers=1,
)

@job(
    params={"url": "https://api.github.com/events"},
    cluster=cluster,
)
def my_pipeline():
    @task(io_manager=io)
    def extract() -> pl.DataFrame:
        import requests
        return pl.DataFrame(requests.get(params["url"]).json())

    @task
    def transform(df: pl.DataFrame):
        print(df.head(10))

    data = extract()
    transform(data)
```

Deploy:

```bash
databricks bundle deploy --target dev
```

## Documentation

Full documentation is available at **[boccileonardo.github.io/databricks-bundle-decorators](https://boccileonardo.github.io/databricks-bundle-decorators/)**:

- [Getting Started](https://boccileonardo.github.io/databricks-bundle-decorators/getting-started/) — scaffolding, first pipeline, deploy
- [How It Works](https://boccileonardo.github.io/databricks-bundle-decorators/how-it-works/) — task dependencies, IoManager, task values
- [Docker Deployment](https://boccileonardo.github.io/databricks-bundle-decorators/docker-deployment/) — pre-built container images
- [API Reference](https://boccileonardo.github.io/databricks-bundle-decorators/api/) — `@task`, `@job`, `IoManager`, and more

## Development

```bash
git clone https://github.com/<org>/databricks-bundle-decorators.git
cd databricks-bundle-decorators
uv sync
uv run pytest tests/ -v
```

## Releasing

### Automated (recommended)

Go to **Actions → "Release: Bump Version & Publish" → Run workflow**, pick `patch`/`minor`/`major`, and click **Run**. The workflow bumps the version in `pyproject.toml`, commits, tags, builds, creates a GitHub Release, and publishes to PyPI.

### Manual

```bash
uv version --bump patch  # or minor, major
git commit -am "release: v$(uv version)" && git push
# Create a GitHub Release with the new tag → publish.yaml pushes to PyPI
```
