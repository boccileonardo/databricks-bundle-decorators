# Custom IoManagers

Subclass `IoManager` to implement your own storage backend when the
[built-in IoManagers](io-managers/index.md) don't cover your use case
(e.g. writing to a REST API, a message queue, or a custom file format).

## Partitioning support

When building a custom IoManager, use `context.logical_date` to scope
storage to the current partition, and `context.all_partitions` to
support cross-partition reads (triggered by the `all_partitions()`
wrapper or `@task(all_partitions=True)`).

### Auto-filtering via task values

By default (`auto_filter=True`), the runtime pushes the distinct
partition values written by the producer to downstream consumers via
Databricks task values.  On the read side, these values are available
in `context.partition_filter` — a `dict[str, list[str]]` mapping
column names to the values that were written.

To opt into auto-filtering in a custom IoManager:

1. Accept `auto_filter` in your `__init__` and set `self.auto_filter`.
2. Override `_extract_partition_values` to return the distinct values
   for each partition column after a write.
3. In `read()`, check `context.partition_filter` and apply it.

```python
from databricks_bundle_decorators import IoManager, OutputContext, InputContext

class MyIoManager(IoManager):
    def __init__(self, base_path: str, *, auto_filter: bool = True) -> None:
        self.base_path = base_path
        self.auto_filter = auto_filter

    def write(self, context: OutputContext, obj):
        path = f"/data/{context.task_key}"
        if context.logical_date:
            date_str = context.logical_date.strftime("%Y-%m-%d")
            path = f"{path}/logical_date={date_str}"
        save(path, obj)

    def _extract_partition_values(
        self, context: OutputContext
    ) -> dict[str, list[str]]:
        path = f"/data/{context.task_key}"
        return extract_distinct_values(path, context.partition_by)

    def read(self, context: InputContext):
        path = f"/data/{context.upstream_task_key}"
        if context.all_partitions:
            return load_all(path)  # Read all partition directories
        if context.partition_filter:
            return load_filtered(path, context.partition_filter)
        if context.logical_date:
            date_str = context.logical_date.strftime("%Y-%m-%d")
            path = f"{path}/logical_date={date_str}"
        return load(path)
```

If you set `auto_filter=False`, `context.partition_filter` will
always be `None` and only the `logical_date` fallback applies.

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
