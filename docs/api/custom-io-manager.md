# Custom IoManagers

Subclass `IoManager` to implement your own storage backend when the
[built-in IoManagers](io-managers/index.md) don't cover your use case
(e.g. writing to a REST API, a message queue, or a custom file format).

## Asset naming

Use `context.asset_name` (write) and `context.upstream_asset_name`
(read) to derive storage paths.  These return `output_name` when set
via `@task(output_name="...")`, falling back to the task key otherwise.

```python
def write(self, context: OutputContext, obj) -> None:
    path = f"{self.base_path}/{context.asset_name}"
    save(path, obj)

def read(self, context: InputContext):
    path = f"{self.base_path}/{context.upstream_asset_name}"
    return load(path)
```

!!! tip
    Prefer `asset_name` / `upstream_asset_name` over `task_key` /
    `upstream_task_key` when building storage paths.

## Partitioning support

When building a custom IoManager, use `context.backfill_key` to scope
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
        if context.backfill_key:
            path = f"{path}/backfill_key={context.backfill_key}"
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
        if context.backfill_key:
            path = f"{path}/backfill_key={context.backfill_key}"
        return load(path)
```

If you set `auto_filter=False`, `context.partition_filter` will
always be `None` and only the `backfill_key` fallback applies.

### Delta replaceWhere example

All built-in Delta IoManagers automatically apply `replaceWhere` (Spark)
or `predicate` (delta-rs / Polars) when `mode="overwrite"` is combined
with `partition_by` — see [Partition-scoped overwrite](../guides/partitioning.md#partition-scoped-overwrite).

If you are writing a **custom** Delta IoManager, you should apply the
same pattern to avoid destroying data in other partitions during
backfill runs:

```python
from databricks_bundle_decorators import IoManager, OutputContext, InputContext
from pyspark.sql import SparkSession, functions as F


class DeltaReplaceWhereIoManager(IoManager):
    """Write to one Delta table, overwriting only the current partition."""

    def __init__(self, base_path: str) -> None:
        self.base_path = base_path

    def write(self, context: OutputContext, obj) -> None:
        uri = f"{self.base_path}/{context.task_key}"
        bk = context.backfill_key or "unknown"
        obj = obj.withColumn("backfill_key", F.lit(bk))
        (
            obj.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"backfill_key = '{bk}'")
            .partitionBy("backfill_key")
            .save(uri)
        )

    def read(self, context: InputContext):
        spark = SparkSession.getActiveSession()
        uri = f"{self.base_path}/{context.upstream_task_key}"
        return spark.read.format("delta").load(uri)
```

## Write retries

To handle transient or concurrent-write errors, configure `RetryConfig` on your IoManager.  The framework will retry failed writes with exponential backoff (powered
by [tenacity](https://tenacity.readthedocs.io/)):

```python
from databricks_bundle_decorators import IoManager, RetryConfig, OutputContext, InputContext


class MyIoManager(IoManager):
    def __init__(self, base_path: str) -> None:
        self.base_path = base_path
        self.retry = RetryConfig(max_attempts=5, delay=1.0, backoff_factor=2.0)

    def write(self, context: OutputContext, obj) -> None:
        ...

    def read(self, context: InputContext):
        ...
```

`RetryConfig` parameters:

| Parameter | Default | Description |
|---|---|---|
| `max_attempts` | `3` | Total number of attempts (including the first try) |
| `delay` | `1.0` | Initial delay in seconds between retries |
| `backoff_factor` | `2.0` | Multiplier applied to delay after each failure (1s → 2s → 4s …) |

## API Reference

::: databricks_bundle_decorators.io_manager.IoManager

::: databricks_bundle_decorators.io_manager.OutputContext

::: databricks_bundle_decorators.io_manager.InputContext

::: databricks_bundle_decorators.io_manager.RetryConfig