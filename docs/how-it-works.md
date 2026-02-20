# How It Works

## Deploy time (`databricks bundle deploy`)

When you run `databricks bundle deploy`, the Databricks CLI imports your Python pipeline files. This triggers the `@job` and `@task` decorators, which **register** your tasks and their dependencies into an internal DAG — no task code actually runs yet. The framework then generates Databricks Job definitions from this DAG and uploads them to your workspace.

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

## Runtime (when the job runs on Databricks)

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

## DAG Construction (TaskFlow Pattern)

Inside a `@job` body, `@task` calls return `TaskProxy` objects (not real data). When a proxy is passed as an argument to another `@task` call, the framework records the dependency edge. The DAG is built by normal Python execution — no AST parsing.

```python
@job
def my_job():
    @task
    def a(): ...
    @task
    def b(data): ...

    x = a()       # Returns TaskProxy("a")
    b(x)          # Records: b depends on a, param "data" maps to task "a"
```

## IoManager vs Task Values

| Mechanism | Use case | Size limit |
|-----------|----------|------------|
| `IoManager` | DataFrames, datasets, large objects | Unlimited (external storage) |
| `set_task_value` / `get_task_value` | Row counts, status flags, small strings | < 48 KB |

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
