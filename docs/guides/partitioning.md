# Backfilling & Partitioning

Jobs that use `@job(backfill=...)` get a **backfill key** — a
string job parameter that represents *which* data slice the run
belongs to (similar to Airflow's `logical_date` or Dagster's partition
key).  The backfill CLI enumerates these keys for bulk run submission.

On the storage side, IoManagers can write data in a **Hive-style
partitioned layout** (`column=value/` directories) using the
`partition_by` parameter on the `@task` decorator — see
[Built-in IoManagers](../api/io-managers/index.md) for format-specific
details.

## Quick start

```python
from databricks_bundle_decorators import job, task
from databricks_bundle_decorators.backfill import DailyBackfill, get_backfill_key
from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

io = PolarsParquetIoManager(
    base_path="abfss://lake@acct.dfs.core.windows.net/data",
)

@job(backfill=DailyBackfill(start_date="2024-01-01"))
def daily_pipeline():
    @task(io_manager=io, partition_by="backfill_key")
    def extract():
        key = get_backfill_key()
        return fetch_data(key)  # key is e.g. "2024-01-15"

    @task
    def transform(df):
        print(df.head())

    data = extract()
    transform(data)
```

All `partition_by` columns produce Hive-style partitioned output
(`column=value/` directory layout).

### Partition-scoped overwrite

When `partition_by` is set, all built-in IoManagers automatically
scope their writes to **only the partitions present in the data
being written**. This means an `overwrite` write for `region=eu`
will never touch `region=us` — existing partitions are preserved.

This is critical for **backfill safety**: running a backfill for
`2024-01-15` will not destroy data for any other date.

The mechanism varies by format:

| Format | Mechanism |
|--------|-----------|
| **Delta** (Polars & Spark) | `replaceWhere` predicate scoped to written partition values |
| **Parquet** (Spark) | `partitionOverwriteMode=dynamic` — only written partitions are replaced |
| **Parquet / CSV / JSON** (Polars) | Hive-style `PartitionBy` directories — each partition is a separate directory, naturally isolated |

!!! note "Merge operations are partition-safe by design"
    When a task returns a `DeltaMergeBuilder` (PySpark) or
    `TableMerger` (Polars / deltalake), the IoManager calls
    `.execute()` directly — it bypasses the overwrite path
    entirely.  The merge predicate (e.g. `t.id = s.id`) controls
    which rows are touched, so other partitions are never at risk.
    You can safely combine `partition_by` with merge; the
    `replaceWhere` / `partitionOverwriteMode` mechanisms above
    only apply to regular DataFrame writes, not merge builders.

    See [Delta Write Modes & Merge](../api/io-managers/index.md#delta-write-modes--merge)
    for full merge examples.

!!! warning "Without `partition_by`"
    When `partition_by` is **not** set, `mode="overwrite"` replaces
    the **entire** table/file as usual. Partition-scoped overwrite
    only applies when `partition_by` is configured on the `@task`.

### Auto-filtering (default)

By default (`auto_filter=True`), all built-in IoManagers automatically
push the distinct partition column values written by the producing task
to downstream consumers via Databricks task values.  Downstream reads
are then filtered to exactly the partition values that were written —
regardless of column name.  This works for `backfill_key`, custom date
columns, categorical columns, and multi-column partitioning alike.

The special column name `"backfill_key"` has one extra convenience on
top of auto-filtering: the IoManager **auto-injects** a `backfill_key`
column on write (your DataFrame doesn't need to contain it).  Any other
column name must already exist in the DataFrame.

### Disabling auto-filtering

Pass `auto_filter=False` when constructing the IoManager to disable
all automatic partition filtering.
In this mode, a warning is logged for all partition columns reminding you to
filter manually.

This is useful when the list of distinct partition values is large
enough to exceed the **48 KB limit** that Databricks imposes on task
values passed between tasks.  Disabling auto-filtering avoids that
limit entirely — at the cost of requiring manual filtering in
downstream task code.

```python
io = PolarsParquetIoManager(
    base_path="...",
    auto_filter=False,   # no automatic filtering
)
```

### Using an existing date column

If your dataset already has a date column that maps to the logical
date, use `partition_by` with that column name directly.  With the
default `auto_filter=True`, downstream reads are automatically
filtered to the written values — no manual filtering needed:

```python
io = PolarsParquetIoManager(
    base_path="abfss://lake@acct.dfs.core.windows.net/data",
)

@job(backfill=DailyBackfill(start_date="2024-01-01"))
def daily_pipeline():
    @task(io_manager=io, partition_by="event_date")
    def extract() -> pl.LazyFrame:
        key = get_backfill_key()
        # The data already contains 'event_date' — no injection needed
        return pl.scan_ndjson(f"s3://raw/{key}/*.jsonl")

    @task
    def transform(df: pl.LazyFrame):
        # df is automatically filtered to the written event_date values
        ...

    data = extract()
    transform(data)
```

## How it works

When `@job(backfill=...)` is specified, the decorator auto-injects a
`backfill_key` job parameter with an empty default value. At runtime,
a non-empty key is passed as a string to all IoManager contexts and is
available via `get_backfill_key()`.

### Automatic key derivation

When the `backfill_key` parameter is missing or empty — for example
when a job is triggered by a **cron schedule**, a **file-arrival
trigger**, or a **manual Run Now** — and the job has a time-based
backfill definition (`DailyBackfill`, `WeeklyBackfill`,
`MonthlyBackfill`, or `HourlyBackfill`), the key is automatically
derived from the current time in the backfill definition's timezone.
A warning is logged when this happens.

| Backfill type | Auto-derived key |
|---|---|
| `DailyBackfill` | Today's date (e.g. `2024-06-15`) |
| `WeeklyBackfill` | Current ISO week (e.g. `2024-W24`) |
| `MonthlyBackfill` | First of the current month (e.g. `2024-06-01`) |
| `HourlyBackfill` | Current hour (e.g. `2024-06-15T14`) |

This means scheduled runs "just work" without requiring the
trigger to supply the key explicitly.

`StaticBackfill` has no sensible default, so runs without an
explicit key will still raise `RuntimeError`.

## Reading the backfill key

Two helpers are available inside task functions:

- **`get_backfill_key()`** — returns the raw key string (e.g. `"2024-01-15"`, `"us"`). Works with all backfill types.
- **`get_run_logical_date()`** — parses the key as an ISO-8601 `datetime`. Use for time-based backfills only.

```python
from databricks_bundle_decorators.backfill import get_backfill_key, get_run_logical_date

key = get_backfill_key()          # "2024-01-15"
dt  = get_run_logical_date()      # datetime(2024, 1, 15, tzinfo=UTC)
```

For time-based backfill definitions, the key is
[auto-derived](#automatic-key-derivation) from the current time when
not explicitly provided. A `RuntimeError` is raised only when no
automatic derivation is possible (e.g. `StaticBackfill`, or no
backfill definition at all).
See the [Backfill Definitions API](../api/backfill.md) for full parameter details.

## Backfill definitions

Attach a `BackfillDef` to `@job(backfill=...)` to enable the
backfill CLI:

```python
from databricks_bundle_decorators.backfill import DailyBackfill

@job(backfill=DailyBackfill(start_date="2024-01-01"))
def my_pipeline():
    ...
```

The backfill definition only affects **key enumeration** — it
does not change runtime behavior beyond injecting the `backfill_key`
parameter.

| Class | Keys | Example |
|-------|------|---------|
| `DailyBackfill` | One per day (`YYYY-MM-DD`) | `2024-01-01` … `2024-12-31` |
| `WeeklyBackfill` | One per ISO week (`YYYY-WNN`) | `2024-W01` … `2024-W52` |
| `MonthlyBackfill` | One per month (`YYYY-MM-01`) | `2024-01-01` … `2024-12-01` |
| `HourlyBackfill` | One per hour (`YYYY-MM-DDTHH`) | `2024-01-01T00` … `2024-01-01T23` |
| `StaticBackfill` | Fixed list of strings | `["us", "eu", "jp"]` |

All time-based definitions accept `start_date`, `end_date` (optional,
defaults to "most recent complete period"), and `tz` (IANA timezone).
Key formats are fixed to ISO-8601-compatible strings.

### Timezone-aware defaults

All time-based definitions default to `tz="UTC"`.  The `tz` parameter
determines which timezone is used to compute the default `end_date`
("yesterday", "last complete week/month").  Override it when your
pipeline is tied to a specific region:

```python
# "yesterday" in Berlin time
DailyBackfill(start_date="2024-01-01", tz="Europe/Berlin")
```

`HourlyBackfill` additionally uses `tz` to handle daylight-saving
transitions safely.

!!! note "`StaticBackfill` and `--start`/`--end`"
    `StaticBackfill.keys()` returns all keys regardless of
    `start`/`end` arguments.  Using `--start`/`--end` with a static
    backfill definition in the backfill CLI has no effect.

## Backfill CLI

The `dbxdec backfill` command submits one Databricks run per
backfill key:

```bash
# Backfill all daily keys from start to yesterday
uv run dbxdec backfill my_pipeline --start 2024-01-01 --end 2024-03-31

# Dry run — show keys without submitting
uv run dbxdec backfill my_pipeline --dry-run

# Explicit keys (works even without a job-level backfill definition)
uv run dbxdec backfill my_pipeline --keys "2024-01-01,2024-01-02,2024-01-03"

# Limit concurrency
uv run dbxdec backfill my_pipeline --max-concurrent 5

# Wait for all runs to complete and report success/failure
uv run dbxdec backfill my_pipeline --start 2024-01-01 --end 2024-01-07 --wait
```

### Options

| Flag | Description |
|------|-------------|
| `--start` | Start of range (inclusive) |
| `--end` | End of range (inclusive) |
| `--keys` | Comma-separated explicit keys |
| `--max-concurrent` | Limit parallel run submissions |
| `--dry-run` | Print keys without submitting |
| `--wait` | Wait for all runs to complete and exit non-zero on failure |
| `--target`, `-t` | Databricks bundle target (e.g. `dev`, `staging`, `prod`) |
| `--profile` | Databricks CLI profile name |

Under the hood the command calls `databricks bundle run`, which
automatically resolves the deployed job name — including any
dev-mode prefix (e.g. `[dev user] my_pipeline`).  The Databricks CLI
must be installed and on your `PATH`.

When `--wait` is used, the CLI polls each run until completion,
printing `SUCCESS` or the failure status for each key.  This is
useful in CI/CD pipelines where you need to gate on backfill success.

### Catchup

The `dbxdec catchup` subcommand automatically determines which
backfill keys are missing and submits only those.  It is useful for:

- **First deploy** — trigger all backfill keys after deploying a job
  for the first time.
- **Partial recovery** — after a partial backfill, submit only the
  remaining keys.

```bash
# Preview missing keys without submitting
uv run dbxdec catchup my_pipeline --dry-run

# Submit all missing keys
uv run dbxdec catchup my_pipeline
```

The `catchup` command counts a key as **already launched** if it has a successful or
still-active run.  Terminally failed runs (`FAILED`, `TIMED_OUT`,
`CANCELED`) will be relaunched if they exist.

!!! tip "Catchup requires a deployed bundle"
    `catchup` resolves the job ID from the bundle state via
    `databricks bundle summary`, so the bundle must have been
    deployed at least once before running `catchup`.

## Cross-partition reads

By default, each downstream task reads only the current partition from
its upstream dependencies.  To read **all** partitions instead, use
either approach:

### Per-edge: `all_partitions()` wrapper

Wrap a single `TaskProxy` to mark that specific edge:

```python
from databricks_bundle_decorators import job, task, all_partitions
from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

io = PolarsParquetIoManager(
    base_path="abfss://lake@acct.dfs.core.windows.net/data",
)

@job(backfill=DailyBackfill(start_date="2024-01-01"))
def daily_pipeline():
    @task(io_manager=io, partition_by="backfill_key")
    def extract():
        ...

    @task
    def aggregate(data):
        # data contains ALL partitions
        return data.group_by("region").agg(pl.sum("revenue"))

    data = extract()
    aggregate(all_partitions(data))
```

### Per-task: `@task(all_partitions=True)`

Mark the consuming task so that **all** upstream data dependencies
read all partitions:

```python
@task(all_partitions=True)
def aggregate(data):
    # every upstream IoManager read gets all partitions
    ...
```

Both approaches set `context.all_partitions = True` on the
`InputContext` passed to `IoManager.read()`.  See
[Custom IoManagers](../api/custom-io-manager.md) for how to handle this
in your own implementations.

## Limitations

- **No automatic scheduling.** Backfill definitions describe the
  *universe* of valid keys but do not generate Databricks triggers or
  schedules.  Use a Databricks cron trigger on the job and compute the
  current date in your task code, or use `dbxdec backfill` for
  ad-hoc runs.
