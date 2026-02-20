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

## Delta Write Modes & Merge

All Delta IoManagers accept a `mode` parameter that controls how data
is written.  The default is `"error"`, which raises if the table
already exists — this prevents accidental overwrites.  Set it
explicitly when you want a different behaviour:

```python
from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

io_append = PolarsDeltaIoManager(
    base_path="abfss://lake@account.dfs.core.windows.net/staging",
    mode="overwrite",   # or "append", "ignore"
)
```

For **merge / upsert** operations, return a merge builder from your
task instead of a DataFrame.  The IoManager detects the type and
calls `.execute()` automatically:

=== "Polars (deltalake)"

    ```python
    from deltalake import DeltaTable

    @task(io_manager=io)
    def upsert(new_data: pl.DataFrame):
        dt = DeltaTable(io._uri("upsert"))
        return (
            dt.merge(
                source=new_data,
                predicate="t.id = s.id",
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
        )  # returns TableMerger — IoManager calls .execute()
    ```

=== "PySpark (delta-spark)"

    ```python
    from delta.tables import DeltaTable

    @task(io_manager=io)
    def upsert(new_data):
        spark = SparkSession.getActiveSession()
        dt = DeltaTable.forPath(spark, io._uri("upsert"))
        return (
            dt.alias("t")
            .merge(new_data.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
        )  # returns DeltaMergeBuilder — IoManager calls .execute()
    ```

| Mode | Behaviour |
|------|-----------|
| `"error"` (default) | Raise if the target already exists |
| `"overwrite"` | Replace the target completely |
| `"append"` | Add rows to the existing target |
| `"ignore"` | Silently skip if the target already exists |
| *(merge builder)* | Return a `TableMerger` / `DeltaMergeBuilder` — `mode` is ignored |

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

## Limitations

This framework is designed for teams deploying jobs with [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/). It generates bundle resources from decorated Python code — it is not a general-purpose orchestrator.

Key rules to keep in mind:

- **The `@job` body is for wiring only.** It runs once at import time (during `databricks bundle deploy`), not on a cluster. Do not put data-fetching, transformations, or any real logic in it — only `@task` definitions and calls. The framework warns if a task call receives a non-`TaskProxy` argument.
- **No conditional or dynamic DAGs.** `if`/`else` or loops in the `@job` body are evaluated at import time, not at runtime. Put conditional logic inside a `@task` function.
- **Task arguments are symbolic.** Inside a `@job` body, `@task` calls return `TaskProxy` placeholders, not real data. Passing a literal value to a task call has no effect at runtime.
- **IoManager belongs to the producer.** Attach `io_manager=` to the task that *produces* data. Downstream tasks receive the data as plain function arguments — they don't declare an IoManager.
- **Names must be unique.** Job names are unique across the project; task names are unique within a job. Duplicates raise `DuplicateResourceError` at import time.

!!! info "Under the hood"

    For details on codegen, runtime dispatch, the registry, and
    pipeline discovery, see [Internals](internals/index.md).
