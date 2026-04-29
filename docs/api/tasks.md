# Tasks

::: databricks_bundle_decorators.decorators.task

::: databricks_bundle_decorators.decorators.for_each_task

::: databricks_bundle_decorators.decorators.task_value

## Control-Flow Dependencies

By default, passing a `TaskProxy` as a function argument creates a
**data dependency** — the upstream task's output is loaded and passed to
the downstream task at runtime.  When you only need ordering ("run A
before B") without data transfer, use `depends_on`:

```python
@job
def my_job():
    @task
    def setup():
        ...  # e.g. create a table, warm a cache

    setup_proxy = setup()

    @task(depends_on=setup_proxy)
    def work():
        ...  # runs after setup, but receives no data from it

    work()
```

Since `depends_on` is a `@task` decorator parameter, the upstream
`TaskProxy` must be assigned before the `@task(depends_on=...)` line.

You can pass a list to wait on multiple tasks:

```python
@job
def my_job():
    @task
    def step_a(): ...

    @task
    def step_b(): ...

    a_proxy = step_a()
    b_proxy = step_b()

    @task(depends_on=[a_proxy, b_proxy])
    def final():
        ...

    final()
```

`depends_on` and data arguments can be mixed on the same task:

```python
@job
def my_job():
    @task
    def init(): ...

    @task
    def produce(): ...

    i = init()
    p = produce()

    @task(depends_on=i)       # control-flow dep on init
    def consume(data): ...

    consume(p)                # data dep on produce
```

See [How It Works — Control-flow dependencies](../how-it-works.md#control-flow-dependencies-depends_on)
for more details.

## Per-Task Compute Override

By default every task inherits the shared job cluster defined via
`@job(cluster=...)`.  If a specific task needs different compute you
can pass any of the compute-related `TaskConfig` fields directly to
the decorator:

```python
@task(existing_cluster_id="0123-456789-abcdef01")
def special_task():
    ...

@task(environment_key="my-serverless-env")
def serverless_task():
    ...
```

These fields are forwarded to the Databricks SDK `Task` constructor
and take precedence over the job-level cluster.  See
`TaskConfig` for all available fields.
