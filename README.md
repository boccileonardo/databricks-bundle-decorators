# databricks-bundle-decorators

Decorator-based framework for defining Databricks jobs and tasks as Python code. Define pipelines using `@task`, `@job`, and `job_cluster()` — databricks-bundle-decorators compiles them into [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/python/) resources at deploy time and handles task dispatch at runtime.

## Why databricks-bundle-decorators?

Writing Databricks jobs in raw YAML is tedious and disconnects task logic from orchestration configuration. databricks-bundle-decorators lets you express both in Python:

- **TaskFlow pattern** — define `@task` functions inline inside a `@job` body; dependencies are captured automatically from call arguments — no manual `depends_on` wiring.
- **IoManager pattern** — large data (DataFrames, datasets) flows through permanent storage (Delta tables, Unity Catalog volumes) instead of Databricks task values.
- **Explicit task values** — small scalars (`str`, `int`, `float`, `bool`) can still be passed between tasks via `set_task_value` / `get_task_value`.
- **Deploy-time codegen** — decorators compile to `databricks.bundles.jobs.Job` objects consumed by `databricks bundle deploy`.
- **Runtime dispatch** — a single `pydabs_run` console-script entry point resolves upstream data via IoManagers, populates parameters, and executes the task function.

## Architecture

databricks-bundle-decorators separates **deploy time** from **run time**:

```
┌─────────────────────────────────────────────────────────────┐
│                       DEPLOY TIME                           │
│                                                             │
│  @task / @job decorators, job_cluster() function            │
│        │                                                    │
│        ▼                                                    │
│  Global registries (TaskMeta, JobMeta, ClusterMeta)         │
│        │                                                    │
│        ▼                                                    │
│  codegen.generate_resources()                               │
│        │                                                    │
│        ▼                                                    │
│  databricks.bundles.jobs.Job objects                        │
│        │                                                    │
│        ▼                                                    │
│  databricks bundle deploy                                   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                        RUN TIME                             │
│                                                             │
│  Databricks launches python_wheel_task                      │
│        │                                                    │
│        ▼                                                    │
│  pydabs_run CLI entry point                                 │
│        │                                                    │
│        ▼                                                    │
│  discover_pipelines() → import pipeline modules             │
│        │                                                    │
│        ▼                                                    │
│  Parse --key=value args (job params, upstream edges)        │
│        │                                                    │
│        ▼                                                    │
│  IoManager.load() upstream data → execute task fn           │
│        │                                                    │
│        ▼                                                    │
│  IoManager.store() return value                             │
└─────────────────────────────────────────────────────────────┘
```

## Installation

```bash
uv add databricks-bundle-decorators --git https://github.com/<org>/databricks-bundle-decorators.git
```

Or from a private PyPI registry:

```bash
uv add databricks-bundle-decorators
```

## Quickstart

### 1. Scaffold your pipeline project

Create a new project and initialize it:

```bash
uv init my-pipeline
cd my-pipeline
uv add databricks-bundle-decorators
uv run pydabs init
```

`pydabs init` creates:

| File | Purpose |
|------|---------|
| `resources/__init__.py` | `load_resources()` entry point for `databricks bundle deploy` |
| `src/<package>/pipelines/__init__.py` | Auto-discovery module that imports all pipeline files |
| `src/<package>/pipelines/example.py` | Starter pipeline with `@task`, `@job`, `job_cluster()` |
| `databricks.yaml` | Databricks Asset Bundle configuration (if not present) |

It also prints the entry-point registration line to add to your `pyproject.toml`:

```toml
[project.entry-points."databricks_bundle_decorators.pipelines"]
my_pipeline = "my_pipeline.pipelines"
```

### 2. Define your pipeline

Replace the generated example or add new files under `src/<package>/pipelines/`:

```python
# src/my_pipeline/pipelines/github_events.py

import polars as pl
import requests

from databricks_bundle_decorators import IoManager, InputContext, OutputContext, params
from databricks_bundle_decorators import job, job_cluster, task
from databricks_bundle_decorators.task_values import set_task_value


class DeltaIoManager(IoManager):
    def __init__(self, catalog: str, schema: str):
        self.catalog = catalog
        self.schema = schema

    def store(self, context: OutputContext, obj) -> None:
        table = f"{self.catalog}.{self.schema}.{context.task_key}"
        obj.write_delta(table, mode="overwrite")

    def load(self, context: InputContext):
        table = f"{self.catalog}.{self.schema}.{context.upstream_task_key}"
        return pl.read_delta(table)


staging_io = DeltaIoManager(catalog="main", schema="staging")


small_cluster = job_cluster(
    name="small_cluster",
    spark_version="13.2.x-scala2.12",
    node_type_id="Standard_DS3_v2",
    num_workers=2,
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

## API Reference

### Decorators

#### `@task`

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
| `io_manager` | `IoManager \| None` | IoManager instance that handles storing the return value and loading it for downstream tasks. |

#### `@job`

Registers a function as a Databricks job. The function body is **executed once at decoration time** to collect the task DAG — it is not executed again at Databricks runtime. Define `@task` functions and call them inside the body to wire up dependencies.

```python
@job(
    tags={"team": "data-eng"},
    schedule="0 * * * *",
    params={"url": "https://api.example.com"},
    cluster="small_cluster",
)
def my_job():
    @task
    def extract():
        ...

    @task
    def transform(data):
        ...

    data = extract()      # returns TaskProxy, no upstream deps
    transform(data)       # depends on extract (detected via proxy arg)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `tags` | `dict[str, str]` | Arbitrary key/value tags on the Databricks job. |
| `schedule` | `str \| None` | Cron expression (5-field standard or 6-field Quartz). |
| `params` | `dict[str, str]` | Default values for job-level parameters, accessible via `from databricks_bundle_decorators import params`. |
| `cluster` | `str \| None` | Name of a `job_cluster()` to use as the shared job cluster. |

**DAG extraction (TaskFlow pattern):** Inside a `@job` body, each `@task` call returns a lightweight `TaskProxy` object. When a proxy is passed as an argument to another task call, databricks-bundle-decorators records the dependency edge. No AST parsing is needed — the DAG is built by normal Python execution.

**Task namespacing:** Tasks defined inside a `@job` body are registered under qualified keys (`job_name.task_name`), preventing name collisions across jobs.

#### `job_cluster()`

Registers a reusable job-cluster configuration and returns its name.

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
| `name` | `str` | Cluster name (required). Used to reference this cluster from `@job(cluster=…)`. |
| `**kwargs` | `ClusterConfig` | Any SDK-native `ClusterSpec` fields (`spark_version`, `node_type_id`, `num_workers`, etc.). |

These are **ephemeral job clusters** (created per-run, torn down after). They are not interactive/all-purpose clusters.

### IoManager

Abstract base class for managing inter-task data persistence. The **producing task** (the one that returns data) is responsible for declaring an `IoManager`. Downstream tasks that depend on that data don't need to configure anything — the framework automatically calls the _upstream_ task's IoManager to load the data before executing the downstream function.

#### How it works

```
          IoManager (attached to producer)
          ┌──────────┐
          │  store()  │◄── called after producer runs, persists return value
          │  load()   │◄── called before consumer runs, injects data as argument
          └──────────┘
```

1. **Producer declares the IoManager** via `@task(io_manager=my_io)`.
2. At runtime, after the producer executes, `store()` is called with the return value and an `OutputContext`.
3. When a downstream task runs, the framework sees the dependency edge, looks up the **upstream task's** IoManager, and calls `load()` with an `InputContext` to retrieve the data.
4. The loaded data is passed as a keyword argument to the downstream function — matching the parameter name from the `@job` body.

This means downstream tasks are **storage-agnostic**: they receive plain Python objects and don't need to know whether the data came from a Delta table, a Parquet file, or a Unity Catalog volume.

#### Example

```python
from databricks_bundle_decorators import IoManager, OutputContext, InputContext, job, task

class DeltaIoManager(IoManager):
    def __init__(self, catalog: str, schema: str):
        self.catalog = catalog
        self.schema = schema

    def store(self, context: OutputContext, obj: Any) -> None:
        table = f"{self.catalog}.{self.schema}.{context.task_key}"
        obj.write_delta(table, mode="overwrite")

    def load(self, context: InputContext) -> Any:
        table = f"{self.catalog}.{self.schema}.{context.upstream_task_key}"
        return pl.read_delta(table)

delta_io = DeltaIoManager(catalog="main", schema="staging")

@job
def my_job():
    @task(io_manager=delta_io)       # ← producer owns the IoManager
    def extract():
        return pl.DataFrame(...)     # stored via delta_io.store()

    @task                            # ← consumer has NO IoManager
    def transform(raw_df):           # ← raw_df loaded via delta_io.load()
        print(raw_df.head(10))

    df = extract()
    transform(df)
```

In this example, `extract` declares `delta_io` so its return value is written to a Delta table. When `transform` runs, the framework calls `delta_io.load()` (the _upstream's_ IoManager) to read that table and passes the result as the `raw_df` argument.

#### When to use IoManager vs task values

| Mechanism | Use case | Size limit |
|-----------|----------|------------|
| `IoManager` | DataFrames, datasets, large objects | Unlimited (stored in external storage) |
| `set_task_value` / `get_task_value` | Row counts, status flags, small strings | < 48 KB (Databricks task values limit) |

If a task returns a value but has no IoManager, the runtime logs a warning and discards the return value.

#### Context objects

**`OutputContext`** — passed to `store()`:

| Field | Type | Description |
|-------|------|-------------|
| `job_name` | `str` | Name of the running job |
| `task_key` | `str` | Short name of the producing task |
| `run_id` | `str` | Databricks run ID (or `"local"` in tests) |

**`InputContext`** — passed to `load()`:

| Field | Type | Description |
|-------|------|-------------|
| `job_name` | `str` | Name of the running job |
| `task_key` | `str` | Short name of the consuming task |
| `upstream_task_key` | `str` | Short name of the producing task whose data is being loaded |
| `run_id` | `str` | Databricks run ID (or `"local"` in tests) |

### Task Values

For small scalar data (< 48 KB) that needs to pass between tasks without going through permanent storage:

```python
from databricks_bundle_decorators import set_task_value, get_task_value

@task
def producer():
    set_task_value("row_count", 42)

@task
def consumer():
    count = get_task_value("producer", "row_count")
```

These map to Databricks `dbutils.jobs.taskValues` at runtime, with a local dict fallback for testing.

### Parameters

Job-level parameters are accessible inside tasks via the global `params` dict:

```python
from databricks_bundle_decorators import params

@task
def my_task():
    url = params["url"]
```

Parameters are defined in the `@job` decorator's `params` argument and forwarded to each task at runtime via CLI `--key=value` arguments parsed by `argparse`.

### Pipeline Discovery

databricks-bundle-decorators discovers pipeline modules via Python [entry points](https://packaging.python.org/en/latest/specifications/entry-points/). Register your pipeline package under the `databricks_bundle_decorators.pipelines` group:

```toml
[project.entry-points."databricks_bundle_decorators.pipelines"]
my_pipeline = "my_pipeline.pipelines"
```

The module referenced by the entry point should import (directly or via auto-discovery) all modules containing `@task` / `@job` decorators and `job_cluster()` calls. A simple pattern:

```python
# my_pipeline/pipelines/__init__.py
import importlib
import pkgutil

for _loader, _name, _is_pkg in pkgutil.walk_packages(__path__):
    importlib.import_module(f"{__name__}.{_name}")
```

### Duplicate Detection

databricks-bundle-decorators raises `DuplicateResourceError` when:
- Two `@job` decorators use the same function name
- Two `job_cluster()` calls use the same name
- A job references a task that would create a duplicate qualified key (`job_name.task_name`)

```python
from databricks_bundle_decorators import DuplicateResourceError
```

### CLI

#### `pydabs init`

Scaffolds a pipeline project in the current directory. Reads `pyproject.toml` to detect the project name and layout, then creates the required files.

```bash
uv run pydabs init
```

Skips any files that already exist.

## Packaging Model

databricks-bundle-decorators is a **standalone library package**. Pipeline repositories depend on it:

```
┌──────────────────────────┐     ┌────────────────────────┐
│   databricks-bundle-decorators      │     │   my-pipeline (repo)   │
│   (library, PyPI)        │◄────│                        │
│                          │     │  pyproject.toml         │
│  @task, @job, ...        │     │    → depends on         │
│  IoManager ABC           │     │      databricks-bundle-decorators  │
│  codegen                 │     │  src/my_pipeline/       │
│  runtime                 │     │    pipelines/           │
│  discovery               │     │      etl.py             │
│  pydabs CLI              │     │  resources/__init__.py  │
│  pydabs_run script       │     │  databricks.yaml        │
└──────────────────────────┘     └────────────────────────┘
```

**Why separate packages?**
- The framework is reusable across many pipeline repositories.
- Pipeline repos only contain business logic — no framework code to maintain.
- Upgrading databricks-bundle-decorators is a single dependency bump.
- Each pipeline repo builds its own wheel for deployment. The `databricks-bundle-decorators` dependency is resolved and installed alongside it on the Databricks cluster.

**This repository** also serves as a working example: it includes a sample pipeline under `examples/example_pipeline.py` along with a `examples/databricks.yaml` and `examples/resources/` loader ready for deployment.

## Development

```bash
# Clone and install
git clone https://github.com/<org>/databricks-bundle-decorators.git
cd databricks-bundle-decorators
uv sync

# Run tests
uv run pytest tests/ -v

# Deploy the example pipeline
databricks bundle deploy --target develop
```

## Project Structure

```
databricks-bundle-decorators/
├── pyproject.toml                    # Package metadata, deps, entry points
├── examples/
│   ├── databricks.yaml               # Example Databricks Asset Bundle configuration
│   ├── resources/
│   │   └── __init__.py               # Example load_resources() for databricks bundle deploy
│   └── example_pipeline.py           # Example GitHub events pipeline
├── src/databricks_bundle_decorators/
│   ├── __init__.py                   # Public API exports
│   ├── cli.py                        # pydabs init CLI command
│   ├── codegen.py                    # Registry → databricks.bundles.jobs.Job
│   ├── context.py                    # Global params dict
│   ├── decorators.py                 # @task, @job decorators, job_cluster() + TaskProxy
│   ├── discovery.py                  # Entry-point based pipeline discovery
│   ├── io_manager.py                 # IoManager ABC, OutputContext, InputContext
│   ├── registry.py                   # Global registries + DuplicateResourceError
│   ├── runtime.py                    # pydabs_run CLI entry point
│   ├── sdk_types.py                  # JobConfig, TaskConfig, ClusterConfig TypedDicts
│   └── task_values.py                # set_task_value / get_task_value helpers
└── tests/
    ├── test_codegen.py
    ├── test_decorators.py
    └── test_runtime.py
```
