# How It Works

## Deploy and run

1. **You write Python** — define `@task` functions inside a `@job` body, wire them by passing return values as arguments.
2. **`databricks bundle deploy`** — the framework reads your decorators and generates Databricks Job definitions. No task code runs at this stage.
3. **Databricks runs your job** — each task executes on a cluster. The framework loads upstream data, calls your function, and persists the result for downstream tasks.

```
You write Python
  @job / @task / job_cluster()
       ▼
databricks bundle deploy
  → Job definitions created in workspace
       ▼
Job runs on Databricks
  → Each task: load upstream data → call your function → save output
```

## Task dependencies

Inside a `@job` body, calling a `@task` function doesn't execute it immediately — it records it. Passing the return value of one task call to another captures the dependency:

```python
@job
def my_job():
    @task
    def a(): ...
    @task
    def b(data): ...

    x = a()     # records task "a"
    b(x)        # records task "b", depends on "a"
```

At deploy time this produces a two-task job where `b` runs after `a`. At runtime, the framework passes the output of `a` as the `data` argument to `b`.

## Passing data between tasks

There are two mechanisms, suited to different data sizes:

| Mechanism | Use case | Size limit |
|-----------|----------|------------|
| `IoManager` | DataFrames, datasets, large objects | Unlimited (external storage) |
| `set_task_value` / `get_task_value` | Row counts, status flags, small strings | < 48 KB |

### IoManager (large data)

Attach an `IoManager` to a task to persist its return value to external storage. Downstream tasks receive the data as a plain function argument:

```python
from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

io = PolarsParquetIoManager(base_path="abfss://lake@account.dfs.core.windows.net/staging")

@job(cluster=my_cluster)
def pipeline():
    @task(io_manager=io)
    def extract() -> pl.DataFrame:
        return pl.DataFrame({"x": [1, 2, 3]})

    @task
    def transform(df: pl.DataFrame):
        print(df.head())

    data = extract()    # output saved by IoManager
    transform(data)     # input loaded by upstream's IoManager
```

### Task values (small scalars)

For lightweight metadata (row counts, status flags), use task values:

```python
from databricks_bundle_decorators import set_task_value, get_task_value

@task
def produce():
    set_task_value("row_count", 42)

@task
def consume():
    count = get_task_value("produce", "row_count")
```

## Packaging model

```
┌──────────────────────────────┐     ┌────────────────────────┐
│  databricks-bundle-decorators│     │  my-pipeline (repo)    │
│  (library, PyPI)             │◄────│                        │
│                              │     │  pyproject.toml        │
│  @task, @job, job_cluster()  │     │  src/my_pipeline/      │
│  IoManager ABC               │     │    pipelines/          │
│  dbxdec CLI                  │     │  resources/__init__.py │
│                              │     │  databricks.yaml       │
└──────────────────────────────┘     └────────────────────────┘
```

!!! info "Under the hood"

    For details on codegen, runtime dispatch, the registry, and
    pipeline discovery, see [Internals](internals/index.md).
