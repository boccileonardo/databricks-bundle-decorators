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
    Delta merge/upsert is not a write mode; it requires a merge builder
    object that describes the match predicate and update/insert actions.
    See the next section for the correct pattern.

### Merge / Upsert

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

When returning a merge builder, the `mode` parameter on the IoManager is
ignored — the merge builder fully controls how data is written.
