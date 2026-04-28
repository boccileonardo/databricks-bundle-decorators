# Polars

## Partitioning

All Polars IoManagers support Hive-style partitioning via the
`partition_by` parameter on the `@task` decorator.  When `partition_by`
includes `"backfill_key"`, the column is auto-injected before writing
and auto-filtered on read.

```python
io = PolarsParquetIoManager(
    base_path="abfss://lake@acct.dfs.core.windows.net/data",
)

@task(io_manager=io, partition_by=["backfill_key", "region"])
def extract(): ...
```

Parquet, CSV, and NDJSON use `pl.PartitionBy` for LazyFrame sinks.
DataFrame writes use native `partition_by` (Parquet) or
`.lazy().sink_*` with `PartitionBy` (CSV/NDJSON).
Delta uses `delta_write_options={"partition_by": ...}`.

Reads use `hive_partitioning=True` (Parquet, CSV, NDJSON) or
Delta's native partition pruning.

## Parquet

::: databricks_bundle_decorators.io_managers.PolarsParquetIoManager

## Delta

!!! tip "Merge / Upsert"

    `mode="merge"` is **not** a valid write mode and will raise a `ValueError`.
    To perform merge/upsert operations, return a `deltalake.table.TableMerger`
    from your task function — the IoManager calls `.execute()` automatically.
    See [Delta Write Modes & Merge](index.md#delta-write-modes-merge) for
    full examples.

::: databricks_bundle_decorators.io_managers.PolarsDeltaIoManager

## JSON (NDJSON)

::: databricks_bundle_decorators.io_managers.PolarsJsonIoManager

## CSV

::: databricks_bundle_decorators.io_managers.PolarsCsvIoManager
