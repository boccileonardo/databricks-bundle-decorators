<!-- markdownlint-disable MD046 -->
# Built-in IoManagers

Ready-to-use `IoManager` implementations for common data formats and
compute types. Pick one, pass it to `@task(io_manager=...)`, and the
framework handles reading and writing data between tasks automatically.

Choose an IoManager based on your compute type and preferred data format.

| Compute | Format | IoManager |
|---|---|---|
| **Polars** | Parquet | `PolarsParquetIoManager` |
| | Delta | `PolarsDeltaIoManager` |
| | JSON (NDJSON) | `PolarsJsonIoManager` |
| | CSV | `PolarsCsvIoManager` |
| **Spark – Classic** | Delta | `SparkDeltaIoManager` |
| | Parquet | `SparkParquetIoManager` |
| **Spark – Serverless** | Delta | `SparkServerlessDeltaIoManager` |
| | Parquet | `SparkServerlessParquetIoManager` |
| **Spark – Unity Catalog** | Managed Tables | `SparkUCTableIoManager` |
| | Volume – Delta | `SparkUCVolumeDeltaIoManager` |
| | Volume – Parquet | `SparkUCVolumeParquetIoManager` |

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

| Mode | Behaviour |
|------|-----------|
| `"error"` (default) | Raise if the target already exists |
| `"overwrite"` | Replace the target completely |
| `"append"` | Add rows to the existing target |
| `"ignore"` | Silently skip if the target already exists |

!!! warning "mode='merge' is not a valid write mode"

    Do **not** pass `mode="merge"` — it will raise a `ValueError`.
    Delta merge/upsert is not a write mode; it requires a merge
    definition that describes the match predicate and update/insert
    actions.  See the next section for the correct pattern.

### Merge / Upsert

For **merge / upsert** operations, return a merge definition from your
task instead of a DataFrame:

=== "Polars (deltalake)"

    Return a `DeltaMerge` — a declarative definition of the merge
    predicate and actions.  The **target table** is the task's own
    output table (resolved automatically by the IoManager from
    `base_path/task_key`) — you only provide the source data and
    merge logic:

    ```python
    from databricks_bundle_decorators import DeltaMerge

    @task(io_manager=io)
    def upsert(new_data: pl.LazyFrame) -> DeltaMerge:
        df = new_data.collect()
        return (
            DeltaMerge(source=df, predicate="s.id = t.id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
        )
    ```

    `DeltaMerge` supports the same actions as `deltalake.TableMerger`:

    | Method | Description |
    |--------|-------------|
    | `.when_matched_update_all()` | Update all columns on match |
    | `.when_matched_update(updates)` | Update specific columns on match |
    | `.when_matched_delete()` | Delete matched rows |
    | `.when_not_matched_insert_all()` | Insert all columns for new rows |
    | `.when_not_matched_insert(updates)` | Insert specific columns for new rows |
    | `.when_not_matched_by_source_update(updates)` | Update unmatched target rows |
    | `.when_not_matched_by_source_delete()` | Delete unmatched target rows |

    Each method accepts an optional `predicate` for conditional actions.

=== "PySpark (delta-spark)"

    Return a `DeltaMerge` with a PySpark DataFrame as the source:

    ```python
    from databricks_bundle_decorators import DeltaMerge

    @task(io_manager=io)
    def upsert(new_data) -> DeltaMerge:
        return (
            DeltaMerge(source=new_data, predicate="s.id = t.id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
        )
    ```

When returning a merge definition, the `mode` parameter on the IoManager is
ignored — the merge predicate fully controls how data is written.

!!! note "First run (table doesn't exist yet)"

    If the target Delta table does not exist when a `DeltaMerge` is
    written, the IoManager automatically creates it by writing the
    source data directly.  On subsequent runs, the merge is performed
    as normal.  No special handling is needed in your task code.

## Write Retries

All built-in IoManagers accept an optional `retry` keyword argument of
type `RetryConfig` to handle transient write failures (e.g. Delta commit
conflicts during concurrent backfill runs on unpartitioned tables).

```python
from databricks_bundle_decorators import RetryConfig
from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

io = PolarsDeltaIoManager(
    base_path="abfss://lake@account.dfs.core.windows.net/dims",
    mode="overwrite",
    retry=RetryConfig(max_attempts=5, delay=1.0, backoff_factor=2.0),
)
```

| Parameter | Default | Description |
|---|---|---|
| `max_attempts` | `3` | Total number of attempts (including the first try) |
| `delay` | `1.0` | Initial delay in seconds between retries |
| `backoff_factor` | `2.0` | Multiplier applied to delay after each failure (1s → 2s → 4s …) |

Retries use exponential backoff powered by
[tenacity](https://tenacity.readthedocs.io/).  See
[Custom IoManagers – Write retries](../custom-io-manager.md#write-retries)
for details on using retries in custom IoManager implementations.
