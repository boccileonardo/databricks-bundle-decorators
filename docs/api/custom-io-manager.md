# Custom IoManagers

Subclass `IoManager` to implement your own storage backend when the
[built-in IoManagers](io-managers/index.md) don't cover your use case
(e.g. writing to a REST API, a message queue, or a custom file format).

## Partitioning support

When building a custom IoManager, use `context.logical_date` to scope
storage to the current partition, and `context.all_partitions` to
support cross-partition reads (triggered by the `all_partitions()`
wrapper or `@task(all_partitions=True)`):

```python
from databricks_bundle_decorators import IoManager, OutputContext, InputContext

class MyIoManager(IoManager):
    def write(self, context: OutputContext, obj):
        path = f"/data/{context.task_key}"
        if context.logical_date:
            date_str = context.logical_date.strftime("%Y-%m-%d")
            path = f"{path}/logical_date={date_str}"
        save(path, obj)

    def read(self, context: InputContext):
        path = f"/data/{context.upstream_task_key}"
        if context.all_partitions:
            return load_all(path)  # Read all partition directories
        if context.logical_date:
            date_str = context.logical_date.strftime("%Y-%m-%d")
            path = f"{path}/logical_date={date_str}"
        return load(path)
```

### Delta replaceWhere example

To write partitioned data into a **single Delta table** using
`replaceWhere` (partition-scoped overwrite):

```python
from databricks_bundle_decorators import IoManager, OutputContext, InputContext
from pyspark.sql import SparkSession, functions as F


class DeltaReplaceWhereIoManager(IoManager):
    """Write to one Delta table, overwriting only the current partition."""

    def __init__(self, base_path: str) -> None:
        self.base_path = base_path

    def write(self, context: OutputContext, obj) -> None:
        uri = f"{self.base_path}/{context.task_key}"
        date_str = (
            context.logical_date.strftime("%Y-%m-%d")
            if context.logical_date
            else "unknown"
        )
        obj = obj.withColumn("logical_date", F.lit(date_str))
        (
            obj.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"logical_date = '{date_str}'")
            .partitionBy("logical_date")
            .save(uri)
        )

    def read(self, context: InputContext):
        spark = SparkSession.getActiveSession()
        uri = f"{self.base_path}/{context.upstream_task_key}"
        return spark.read.format("delta").load(uri)
```

## API Reference

::: databricks_bundle_decorators.io_manager.IoManager

::: databricks_bundle_decorators.io_manager.OutputContext

::: databricks_bundle_decorators.io_manager.InputContext
