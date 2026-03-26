# Observability Dashboard

A Dash-based dashboard that shows job execution status, run history,
task performance, and backfill coverage for all jobs deployed from the
current bundle.

The dashboard is **bundle-scoped** — only jobs from *this* bundle are
shown.  It uses the Databricks CLI for data access, inheriting the same
credentials used for `databricks bundle deploy`.

## Quick start

Install the observability extras:

```bash
uv add databricks-bundle-decorators[observability]
```

Launch the dashboard from your project root:

```bash
dbxdec dashboard
```

On first run this scaffolds `observability/app.py` (a lightweight entry
point that imports your pipeline package) and then launches the Dash
server at `http://127.0.0.1:8050`.

## Dashboard pages

### Overview

The landing page ("factory floor") provides:

- **KPI cards** — registered jobs, deployed count, total runs, success
  rate, failures, average duration.
- **Job status grid** — one card per job showing latest state, run
  counts, pass rate, deployment status, and backfill indicator.
- **Backfill summary table** — quick coverage overview for all
  backfill-enabled jobs.

### Jobs

A sortable, filterable table of all registered jobs with columns for
deployment status, run count, pass/fail, success rate, last run time,
status, average duration, and backfill flag.

### Runs

All runs across all jobs in a single filterable table with run ID, job
name, status, start time, duration, and backfill key.

### Job Detail (`/jobs/<name>`)

Drill-down view for a specific job:

- Run history table with per-run details.
- Error alerts for recent failures.
- **Task DAG** visualisation from the latest run (topological layout
  with colour-coded nodes by result state).
- Task breakdown table.
- Backfill coverage chart (if the job has a `BackfillDef`).

### Backfills

Visual comparison of expected backfill keys (from `BackfillDef`)
against successful runs.  Each backfill type gets a dedicated
visualization:

| Backfill type | Visualization |
|---------------|---------------|
| `DailyBackfill` | GitHub-style calendar heatmap (weekday rows × week columns) |
| `WeeklyBackfill` | Year × week grid (W01–W53) |
| `MonthlyBackfill` | Year × month grid (Jan–Dec) |
| `HourlyBackfill` | Date × hour grid (00–23) |
| `StaticBackfill` | Single-row partition grid |

Green = completed, amber = not launched, gray = not in range.

## Navigation

The top navigation bar provides direct links to **Overview**, **Jobs**,
**Runs**, and **Backfills** pages.  Target and CLI profile can be set
inline in the navbar.  Click **Refresh** to re-fetch data.

## Programmatic usage

The data functions can be used independently of the Dash UI:

```python
from databricks_bundle_decorators.dashboard import (
    fetch_job_runs,
    resolve_job_ids,
    compute_backfill_coverage,
    build_job_overview,
)

# Resolve bundle job IDs
job_ids = resolve_job_ids(target="dev")

# Fetch runs for a specific job
runs = fetch_job_runs(job_ids["my_etl"], profile="work")

# Compute backfill coverage
coverage = compute_backfill_coverage(
    "my_etl", runs, expected_keys=["2024-01-01", "2024-01-02"]
)
```
