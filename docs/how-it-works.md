# How It Works

## Deploy and run

1. **You write Python** — define `@task` functions inside a `@job` body, wire them by passing return values as arguments.
2. **`databricks bundle deploy`** — the framework imports your pipeline modules and generates Databricks Job definitions. The `@job` body runs at import time to build the DAG, but `@task` business logic does not run at this stage.
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

There are two ways to declare that one task depends on another:

### Data dependencies (pass return values as arguments)

Inside a `@job` body, calling a `@task` function doesn't execute it immediately — it records it. Passing the return value of one task call to another captures the dependency **and** wires up data transfer via `IoManager`:

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

### Control-flow dependencies (`depends_on`)

Sometimes a task must wait for another to finish but doesn't need its output — for example, a cleanup task that runs after ingestion, or a notification task that fires after all transforms complete. Use `depends_on` for these ordering-only constraints:

```python
@job
def my_job():
    @task
    def ingest(): ...
    @task
    def build_index(): ...
    @task(depends_on=ingest_result)
    def notify(): ...

    ingest_result = ingest()
    build_index(ingest_result)      # data dependency — receives output
    notify()                        # control-flow dependency — no data transferred
```

`depends_on` accepts a single `TaskProxy` or a list of them:

```python
@task(depends_on=[step_a_result, step_b_result])
def final_step():
    ...  # runs after both step_a and step_b, but receives no data from them
```

You can combine both styles on the same task — pass some upstream outputs as arguments (data dependencies) and list others in `depends_on` (control-flow dependencies). Duplicate edges are automatically deduplicated.

## Passing data between tasks

There are two mechanisms, suited to different data sizes:

| Mechanism | Use case | Notes |
|-----------|----------|------------|
| `IoManager` | DataFrames, datasets, large objects | Unlimited size - external storage) |
| `set_task_value` / `get_task_value` | Row counts, status flags, iteration lists, small JSON data | JSON-serializable small values |

### IoManager (large data)

Attach an `IoManager` to a task to persist its return value to external storage. Downstream tasks receive the data as a plain function argument:

```python
from databricks_bundle_decorators.io_managers import SparkDeltaIoManager

io = SparkDeltaIoManager(
    base_path="abfss://lake@account.dfs.core.windows.net/staging",
    mode="overwrite",
)

@job(cluster=my_cluster)
def pipeline():
    @task(io_manager=io)
    def extract():
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        return spark.table("raw.events").limit(100)

    @task
    def transform(df):
        print(f"Rows: {df.count()}")

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

All Delta IoManagers accept a `mode` parameter (`"error"`, `"overwrite"`,
`"append"`, `"ignore"`).  For **merge / upsert** operations, return a
merge builder from your task instead of a DataFrame — the IoManager
detects the type and calls `.execute()` automatically.

See [Built-in IoManagers](api/io-managers/index.md#delta-write-modes--merge)
for full details, examples, and the mode reference table.

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

- **The `@job` body is for wiring only.** It runs once at import time (during `databricks bundle deploy`), not on a cluster. Keep all business logic inside `@task` functions.
- **No conditional or dynamic DAGs.** `if`/`else` or loops in the `@job` body are evaluated at import time. Put conditional logic inside a `@task` function.
- **Task arguments are symbolic.** `@task` calls return `TaskProxy` placeholders, not real data. Passing a literal value to a task call has no effect at runtime.
- **Dependencies must be direct arguments.** A `TaskProxy` hidden inside a list, dict, or other container will **not** register a dependency edge — use a separate parameter per upstream dependency.
- **IoManager belongs to the producer.** Attach `io_manager=` to the task that *produces* data. Downstream tasks receive data as plain function arguments.
- **Names must be unique.** Job names are unique across the project; task names are unique within a job. Duplicates raise `DuplicateResourceError` at import time.

??? example "Examples of common mistakes"

    **Nested proxies — edge NOT captured:**

    ```python
    @job
    def my_job():
        @task
        def a(): ...
        @task
        def b(inputs): ...

        result = a()
        b(inputs=[result])   # ✗ — result is nested in a list
    ```

    **Correct — direct keyword argument:**

    ```python
    @job
    def my_job():
        @task
        def a(): ...
        @task
        def b(a_data): ...

        result = a()
        b(a_data=result)     # ✓ — edge captured
    ```

    **Side effects in `@job` body — runs at deploy time:**

    ```python
    @job
    def my_job():
        print("deploying!")  # ✗ — runs every import
        connect_to_db()      # ✗ — network call at deploy time

        @task
        def extract(): ...
        extract()
    ```

!!! info "Under the hood"

    For details on codegen, runtime dispatch, the registry, and
    pipeline discovery, see [Internals](internals/index.md).