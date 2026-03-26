# Observability Dashboard

A Streamlit-based dashboard that shows job execution status, run history,
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
point that imports your pipeline package) and then launches Streamlit.

## Dashboard tabs

### Overview

A summary table of all registered jobs showing run counts, success rate,
last run time, and whether the job has a backfill definition.

### Run History

Detailed run-by-run listing for a selected job, including start time,
duration, and backfill key (if applicable).

### Task Performance

Task-level breakdown of the most recent run for a selected job —
useful for spotting slow or failing tasks.

### Backfill Coverage

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

Green = completed, red = missing, gray = not in range.

## Programmatic usage

The data functions can be used independently of the Streamlit UI:

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
