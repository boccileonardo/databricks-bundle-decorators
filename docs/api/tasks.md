# Tasks

::: databricks_bundle_decorators.decorators.task

::: databricks_bundle_decorators.decorators.for_each_task

::: databricks_bundle_decorators.decorators.task_value

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
