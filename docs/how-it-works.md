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
