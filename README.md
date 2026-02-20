# databricks-bundle-decorators

Decorator-based framework for defining Databricks jobs and tasks as Python code. Define pipelines using `@task`, `@job`, and `job_cluster()` — they compile into [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/python/) resources.

## Why databricks-bundle-decorators?

Writing Databricks jobs in raw YAML is tedious and disconnects task logic from orchestration configuration. databricks-bundle-decorators lets you express both in Python:

- **AirFlow TaskFlow-inspired pattern** — define `@task` functions inside a `@job` body; dependencies are captured automatically from call arguments.
- **IoManager pattern** — large data (DataFrames, datasets) flows through permanent storage (Delta tables, Unity Catalog volumes) - multi-hop architecture.
- **Explicit task values** — small scalars (`str`, `int`, `float`, `bool`) can be passed between tasks via `set_task_value` / `get_task_value`, like you would with Airflow XComs.
- **Deploy-time codegen** — when you run `databricks bundle deploy`, the framework imports your Python files, discovers all `@job`/`@task` definitions, and generates Databricks Job configurations. The result is a databricks job, with all tasks and dependencies set up.
- **Runtime dispatch** — when Databricks runs the job (on schedule or manually), each task executes on a cluster via the `dbxdec-run` entry point, which loads upstream data through IoManagers and calls your task function.

## Installation

```bash
uv add databricks-bundle-decorators
```

## Quickstart

### 1. Scaffold your pipeline project

```bash
uv init my-pipeline && cd my-pipeline
uv add databricks-bundle-decorators[azure]  # or [aws], [gcp], [polars]
uv run dbxdec init
```

`dbxdec init` creates:

| File | Purpose |
|------|---------|
| `resources/__init__.py` | `load_resources()` entry point for `databricks bundle deploy` |
| `src/<package>/pipelines/__init__.py` | Auto-discovery module that imports all pipeline files |
| `src/<package>/pipelines/example.py` | Starter pipeline with `@task`, `@job`, `job_cluster()` |
| `databricks.yaml` | Databricks Asset Bundle configuration (if not present) |
| `pyproject.toml` | Updated with the pipeline package entry point |

### 2. Define your pipeline

```python
# src/my_pipeline/pipelines/github_events.py

import polars as pl

from databricks_bundle_decorators import job, job_cluster, params, task, set_task_value
from databricks_bundle_decorators.io_managers import PolarsParquetIoManager


staging_io = PolarsParquetIoManager(
    base_path="abfss://datalake@mystorageaccount.dfs.core.windows.net/staging",
    # storage_options={"account_name": "...", "account_key": "..."},
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
    cluster="small_cluster",
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

### 3. Deploy

```bash
databricks bundle deploy --target dev
```

## How It Works

### Deploy time (`databricks bundle deploy`)

When you run `databricks bundle deploy`, the Databricks CLI imports your Python pipeline files. This triggers the `@job` and `@task` decorators, which **register** your tasks and their dependencies into an internal DAG — no task code actually runs yet. The framework then generates Databricks Job definitions from this DAG and uploads them to your workspace.

The result: a Databricks Job appears in the UI with all your tasks, their dependency edges, cluster configs, and parameters fully wired up.

```
your_pipeline.py
  @job / @task / job_cluster()
       ▼
  Framework builds task DAG from decorator metadata
       ▼
  codegen → Databricks Job definition
       ▼
  databricks bundle deploy → Job created in workspace
```

### Runtime (when the job runs on Databricks)

When the job is triggered (on schedule or manually), Databricks launches each task as a separate `python_wheel_task` on a cluster. For each task:

1. The `dbxdec-run` entry point starts.
2. It looks up the upstream tasks and calls `IoManager.read()` to fetch their outputs.
3. It injects the loaded data as arguments to your task function and calls it.
4. If the task has an IoManager, it calls `IoManager.write()` to persist the return value for downstream tasks.

```
Databricks triggers job
  → launches each task as python_wheel_task
       ▼
  dbxdec-run entry point
       ▼
  IoManager.read() upstream data → call your task function
       ▼
  IoManager.write() return value for downstream tasks
```

## API Reference

### `@task`

Registers a function as a Databricks task.

```python
@task
def my_task():
    ...

@task(io_manager=my_io_manager)
def my_task_with_io():
    return some_dataframe
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `io_manager` | `IoManager \| None` | Controls how the return value is persisted and read by downstream tasks. |
| `**kwargs` | `TaskConfig` | SDK-native `Task` fields (`max_retries`, `timeout_seconds`, `retry_on_timeout`, etc.). |

### `@job`

Registers a function as a Databricks job.

The `@job` body runs **once when Python imports the file** (not on Databricks). Its purpose is to let the framework discover which tasks exist and how they depend on each other. Inside the body, `@task` functions don't execute your business logic — they return lightweight `TaskProxy` objects that record dependency edges. Think of the `@job` body as a *declaration*, not execution.

```python
@job(
    tags={"team": "data-eng"},
    schedule="0 * * * *",
    params={"url": "https://api.example.com"},
    cluster="small_cluster",
)
def my_job():
    @task
    def extract(): ...        # Not called yet — just registered

    @task
    def transform(data): ...  # Not called yet — just registered

    data = extract()      # Returns a TaskProxy (not real data)
    transform(data)       # Records: transform depends on extract
```

When this file is imported during `databricks bundle deploy`, the framework sees the DAG: `extract → transform`. Your actual `extract()` and `transform()` code only runs later when Databricks executes the job.

| Parameter | Type | Description |
|-----------|------|-------------|
| `params` | `dict[str, str] \| None` | Default job-level parameters, accessible via `from databricks_bundle_decorators import params`. |
| `cluster` | `str \| None` | Name of a `job_cluster()` to use as the shared job cluster. |
| `**kwargs` | `JobConfig` | SDK-native `Job` fields (`tags`, `schedule`, `max_concurrent_runs`, etc.). |

**Task namespacing:** Tasks inside a `@job` body are registered under qualified keys (`job_name.task_name`), preventing name collisions across jobs.

### `job_cluster()`

Registers a reusable ephemeral job-cluster configuration and returns its name.

```python
small_cluster = job_cluster(
    name="gpu_cluster",
    spark_version="14.0.x-gpu-ml-scala2.12",
    node_type_id="Standard_NC6s_v3",
    num_workers=4,
)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Cluster name, referenced from `@job(cluster=…)`. |
| `**kwargs` | `ClusterConfig` | SDK-native `ClusterSpec` fields (`spark_version`, `node_type_id`, `num_workers`, etc.). |

### IoManager

Abstract base class for inter-task data persistence. The **producing task** declares its IoManager; downstream tasks receive data automatically.

```
IoManager (attached to producer)
┌──────────┐
│  write()  │ ← called after producer runs, persists return value
│  read()   │ ← called before consumer runs, injects data as argument
└──────────┘
```

```python
from databricks_bundle_decorators import IoManager, OutputContext, InputContext

class MyCustomIoManager(IoManager):
    def __init__(self, catalog: str, schema: str):
        self.catalog = catalog
        self.schema = schema

    def write(self, context: OutputContext, obj) -> None:
        table = f"{self.catalog}.{self.schema}.{context.task_key}"
        obj.write_delta(table, mode="overwrite")

    def read(self, context: InputContext):
        table = f"{self.catalog}.{self.schema}.{context.upstream_task_key}"
        return pl.read_delta(table)
```

Downstream tasks are **storage-agnostic** — they receive plain Python objects and don't need to know the storage backend.

#### Built-in IoManagers

| IoManager | Backend | Import |
|-----------|---------|--------|
| `PolarsParquetIoManager` | Polars Parquet on any cloud or local filesystem | `from databricks_bundle_decorators.io_managers import PolarsParquetIoManager` |

`PolarsParquetIoManager` dispatches automatically based on type:

| Return / parameter type | Write method | Read method |
|------------------------|-------------|------------|
| `pl.DataFrame` | `write_parquet` | `read_parquet` |
| `pl.LazyFrame` (or unannotated) | `sink_parquet` | `scan_parquet` |

#### IoManager vs Task Values

| Mechanism | Use case | Size limit |
|-----------|----------|------------|
| `IoManager` | DataFrames, datasets, large objects | Unlimited (external storage) |
| `set_task_value` / `get_task_value` | Row counts, status flags, small strings | < 48 KB |

#### Context Objects

**`OutputContext`** (passed to `write()`): `job_name`, `task_key`, `run_id`

**`InputContext`** (passed to `read()`): `job_name`, `task_key`, `upstream_task_key`, `run_id`, `expected_type`

The `expected_type` field contains the downstream parameter's type annotation (e.g. `polars.DataFrame` or `polars.LazyFrame`), allowing IoManagers to return the appropriate type. It is `None` when no annotation is present.

### Task Values

For small scalar data between tasks without permanent storage:

```python
from databricks_bundle_decorators.task_values import set_task_value, get_task_value

@task
def producer():
    set_task_value("row_count", 42)

@task
def consumer():
    count = get_task_value("producer", "row_count")
```

Maps to `dbutils.jobs.taskValues` at runtime, with a local dict fallback for testing.

### Parameters

Job-level parameters are accessible via the global `params` dict:

```python
from databricks_bundle_decorators import params

@task
def my_task():
    url = params["url"]
```

### Pipeline Discovery

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

### CLI

```bash
uv run dbxdec init    # Scaffold a pipeline project in the current directory
```

## Packaging Model

```
┌──────────────────────────────┐     ┌────────────────────────┐
│  databricks-bundle-decorators│     │  my-pipeline (repo)    │
│  (library, PyPI)             │◄────│                        │
│                              │     │  pyproject.toml        │
│  @task, @job, job_cluster()  │     │  src/my_pipeline/      │
│  IoManager ABC               │     │    pipelines/           │
│  codegen, runtime, discovery │     │  resources/__init__.py  │
│  dbxdec CLI                  │     │  databricks.yaml        │
└──────────────────────────────┘     └────────────────────────┘
```

The framework is a reusable library. Pipeline repos contain only business logic — upgrading is a single dependency bump.

## Development

```bash
git clone https://github.com/<org>/databricks-bundle-decorators.git
cd databricks-bundle-decorators
uv sync
uv run pytest tests/ -v
```

## Project Structure

```
├── pyproject.toml
├── examples/
│   ├── databricks.yaml
│   ├── resources/__init__.py
│   └── example_pipeline.py
├── src/databricks_bundle_decorators/
│   ├── __init__.py          # Public API exports
│   ├── cli.py               # dbxdec init command
│   ├── codegen.py           # Registry → Job objects
│   ├── context.py           # Global params dict
│   ├── decorators.py        # @task, @job, job_cluster(), TaskProxy
│   ├── discovery.py         # Entry-point pipeline discovery
│   ├── io_manager.py        # IoManager ABC, OutputContext, InputContext
│   ├── registry.py          # Global registries, DuplicateResourceError
│   ├── runtime.py           # dbxdec-run entry point
│   ├── sdk_types.py         # JobConfig, TaskConfig, ClusterConfig TypedDicts
│   └── task_values.py       # set_task_value / get_task_value
└── tests/
```

## Release

See [RELEASING.md](RELEASING.md) for the PyPI release process.
