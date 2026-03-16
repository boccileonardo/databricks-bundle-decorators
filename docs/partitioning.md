# Partitioning

Every job run has a **logical date** — a `datetime` that represents
*when* the data slice belongs (similar to Airflow's `logical_date` or
Dagster's partition key).  By default it's the current UTC time; for
backfills it's a specific date set via job parameters.

Data partitioning is handled via the `partition_by` parameter on the
`@task` decorator of the producing task — see
[Built-in IoManagers](api/io-managers/index.md) for format-specific
details.

## Quick start

```python
from databricks_bundle_decorators import job, task
from databricks_bundle_decorators.backfill import DailyBackfill, current_logical_date
from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

io = PolarsParquetIoManager(
    base_path="abfss://lake@acct.dfs.core.windows.net/data",
)

@job(backfill=DailyBackfill(start_date="2024-01-01"))
def daily_pipeline():
    @task(io_manager=io, partition_by="logical_date")
    def extract():
        date = current_logical_date()
        return fetch_data(date.strftime("%Y-%m-%d"))

    @task
    def transform(df):
        print(df.head())

    data = extract()
    transform(data)
```

All `partition_by` columns produce Hive-style partitioned output
(`column=value/` directory layout).

### Auto-filtering (default)

By default (`auto_filter=True`), all built-in IoManagers automatically
push the distinct partition column values written by the producing task
to downstream consumers via Databricks task values.  Downstream reads
are then filtered to exactly the partition values that were written —
regardless of column name.  This works for `logical_date`, custom date
columns, categorical columns, and multi-column partitioning alike.

The special column name `"logical_date"` has one extra convenience on
top of auto-filtering: the IoManager **auto-injects** a `logical_date`
column on write (your DataFrame doesn't need to contain it).  Any other
column name must already exist in the DataFrame.

### Disabling auto-filtering

Pass `auto_filter=False` when constructing the IoManager to disable
partition value pushdown.  In this mode, only `logical_date` is
auto-filtered (via the runtime context), and a warning is logged for
any non-`logical_date` partition columns reminding you to filter
manually.

```python
io = PolarsParquetIoManager(
    base_path="...",
    auto_filter=False,   # only logical_date will be auto-filtered
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
        date = current_logical_date()
        # The data already contains 'event_date' — no injection needed
        return pl.scan_ndjson(f"s3://raw/{date:%Y-%m-%d}/*.jsonl")

    @task
    def transform(df: pl.LazyFrame):
        # df is automatically filtered to the written event_date values
        ...

    data = extract()
    transform(data)
```

## How it works

### Deploy time

When `@job(backfill=...)` is specified, the decorator auto-injects a
`logical_date` job parameter with an empty default value and stores
the backfill definition for the CLI.  Jobs without `backfill=` do not
get a `logical_date` parameter unless you add one explicitly via
`params={"logical_date": ""}`.

### Runtime

When `logical_date` is present as a job parameter:

- If non-empty, it is parsed as a `datetime` via
  `datetime.fromisoformat()`.
- If empty (e.g. a manual run without specifying a date), it defaults
  to `datetime.now(tz=timezone.utc)`.

This value is passed to all IoManager contexts and is available via
`current_logical_date()`.

When `logical_date` is **not** a job parameter (no `backfill=` and
not added manually), the IoManager contexts receive `logical_date=None`.

## Reading the logical date

Inside a task, use the convenience helper:

```python
from databricks_bundle_decorators.backfill import current_logical_date

date = current_logical_date()  # returns datetime
```

`current_logical_date()` raises `RuntimeError` if `logical_date` is
empty or missing — i.e. when the job has no backfill definition and
was not invoked with a `logical_date` parameter.  This strict behaviour
prevents silent bugs where tasks assume a logical date that was never
set.

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
does not change runtime behavior beyond injecting the `logical_date`
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
    @task(io_manager=io, partition_by="logical_date")
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
[Custom IoManagers](api/custom-io-manager.md) for how to handle this
in your own implementations.

## Limitations

- **No automatic scheduling.** Backfill definitions describe the
  *universe* of valid keys but do not generate Databricks triggers or
  schedules.  Use a Databricks cron trigger on the job and compute the
  current date in your task code, or use `dbxdec backfill` for
  ad-hoc runs.
