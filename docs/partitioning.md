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
from databricks_bundle_decorators.partitions import DailyPartition, current_logical_date
from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

io = PolarsParquetIoManager(
    base_path="abfss://lake@acct.dfs.core.windows.net/data",
)

@job(partition=DailyPartition(start_date="2024-01-01"))
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
(`column=value/` directory layout).  On read, every column listed in
`partition_by` is used to filter the data to the current partition
automatically.

The `partition_by` parameter lives on the **producing** `@task`
decorator, not on the IoManager.  This means you can reuse the same
IoManager instance across datasets partitioned by different columns.

The special column name `"logical_date"` adds one extra convenience:
the IoManager **auto-injects** a `logical_date` column on write (so
your DataFrame doesn't need to contain it).  Any other column name
must already exist in the DataFrame.

### Using an existing date column

If your dataset already has a date column that maps to the logical
date, use `partition_by` with that column name directly — no column
injection occurs, but filtering on read still applies:

```python
io = PolarsParquetIoManager(
    base_path="abfss://lake@acct.dfs.core.windows.net/data",
)

@job(partition=DailyPartition(start_date="2024-01-01"))
def daily_pipeline():
    @task(io_manager=io, partition_by="event_date")
    def extract() -> pl.LazyFrame:
        date = current_logical_date()
        # The data already contains 'event_date' — no injection needed
        return pl.scan_ndjson(f"s3://raw/{date:%Y-%m-%d}/*.jsonl")

    @task
    def transform(df: pl.LazyFrame):
        # df is already filtered to the current event_date partition
        ...

    data = extract()
    transform(data)
```

## How it works

### Deploy time

The `@job` decorator auto-injects a `logical_date` job parameter with
an empty default value on **every** job (not just partitioned ones).
When `@job(partition=...)` is also specified, the partition definition
is stored for the backfill CLI.

### Runtime

The `logical_date` parameter is parsed as a `datetime` via
`datetime.fromisoformat()`.  If it's empty or missing, it defaults to
`datetime.now(tz=timezone.utc)`.  This value is passed to all IoManager
contexts and is available via `current_logical_date()`.

## Reading the logical date

Inside a task, use the convenience helper:

```python
from databricks_bundle_decorators.partitions import current_logical_date

date = current_logical_date()  # returns datetime
```

`current_logical_date()` raises `RuntimeError` if `logical_date` is
empty or missing — i.e. when the job is not partitioned or was not
invoked with a `logical_date` parameter.  This strict behaviour
prevents silent bugs where tasks assume partitioned data on a
non-partitioned job.

## Job-level partitioning

Attach a `PartitionDef` to `@job(partition=...)` to enable the
backfill CLI:

```python
from databricks_bundle_decorators.partitions import DailyPartition

@job(partition=DailyPartition(start_date="2024-01-01"))
def my_pipeline():
    ...
```

The partition definition only affects **backfill enumeration** — it
does not change runtime behavior.  `logical_date` is always available
on every job, partitioned or not.

## Partition definitions

| Class | Keys | Example |
|-------|------|---------|
| `DailyPartition` | One per day (`YYYY-MM-DD`) | `2024-01-01` … `2024-12-31` |
| `WeeklyPartition` | One per ISO week (`YYYY-WNN`) | `2024-W01` … `2024-W52` |
| `MonthlyPartition` | One per month (`YYYY-MM`) | `2024-01` … `2024-12` |
| `HourlyPartition` | One per hour (`YYYY-MM-DDTHH`) | `2024-01-01T00` … `2024-01-01T23` |
| `StaticPartition` | Fixed list of strings | `["us", "eu", "jp"]` |

All time-based partitions accept `start_date`, `end_date` (optional,
defaults to "most recent complete period"), and `fmt` (strftime format).

### Timezone-aware defaults

All time-based partitions default to `tz="UTC"`.  The `tz` parameter
determines which timezone is used to compute the default `end_date`
("yesterday", "last complete week/month").  Override it when your
pipeline is tied to a specific region:

```python
# "yesterday" in Berlin time
DailyPartition(start_date="2024-01-01", tz="Europe/Berlin")
```

`HourlyPartition` additionally uses `tz` to handle daylight-saving
transitions safely.

!!! note "`StaticPartition` and `--start`/`--end`"
    `StaticPartition.partition_keys()` returns all keys regardless of
    `start`/`end` arguments.  Using `--start`/`--end` with a static
    partition in the backfill CLI has no effect.

## Backfill CLI

The `dbxdec backfill` command submits one Databricks run per
partition key:

```bash
# Backfill all daily partitions from start to yesterday
uv run dbxdec backfill my_pipeline --start 2024-01-01 --end 2024-03-31

# Dry run — show keys without submitting
uv run dbxdec backfill my_pipeline --dry-run

# Explicit keys (works even without a job-level partition definition)
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

@job(partition=DailyPartition(start_date="2024-01-01"))
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

- **No automatic scheduling.** Partition definitions describe the
  *universe* of valid keys but do not generate Databricks triggers or
  schedules.  Use a Databricks cron trigger on the job and compute the
  current date in your task code, or use `dbxdec backfill` for
  ad-hoc runs.
