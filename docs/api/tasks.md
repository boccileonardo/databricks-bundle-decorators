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

    @task(depends_on=setup_proxy)
    def work():
        ...  # runs after setup, but receives no data from it

    setup_proxy = setup()
    work()
```

You can pass a list to wait on multiple tasks:

```python
@task(depends_on=[a_proxy, b_proxy])
def final():
    ...
```

`depends_on` and data arguments can be mixed on the same task.
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
