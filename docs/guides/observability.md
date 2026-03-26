# Observability Dashboard

A Dash-based dashboard that shows job KPIs and backfill coverage for
all jobs deployed from the current bundle.

For run history, task DAGs, and task-level details, the dashboard links
directly to the Databricks workspace UI — avoiding duplicating native
observability features.

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

The landing page provides:

- **KPI cards** — registered jobs, deployed count, total runs, success
  rate, failures, average duration.
- **Job table** — all registered jobs with status, run counts, pass
  rate, deployment status, and backfill indicator.  When the Databricks
  workspace URL is available, job names link directly to the workspace
  job page.
- **Backfill summary table** — quick coverage overview for all
  backfill-enabled jobs, with links to per-job backfill detail.

### Backfills

Summary table of backfill coverage across all jobs.  Click a job name
to see the per-job coverage heatmap.

### Backfill Detail (`/backfills/<name>`)

Drill-down view for a specific job's backfill coverage:

- Coverage percentage and key counts.
- Link to the Databricks workspace job page (when available).
- Backfill coverage chart with date-range picker for time-based
  backfills.
- Expandable list of missing (not-launched) keys.

Each backfill type gets a dedicated visualization:

| Backfill type | Visualization |
|---------------|---------------|
| `DailyBackfill` | GitHub-style calendar heatmap (weekday rows × week columns) |
| `WeeklyBackfill` | Year × week grid (W01–W53) |
| `MonthlyBackfill` | Year × month grid (Jan–Dec) |
| `HourlyBackfill` | Date × hour grid (00–23) |
| `StaticBackfill` | Single-row partition grid |

Green = completed, amber = not launched, gray = not in range.

## Navigation

The top navigation bar provides direct links to **Overview** and
**Backfills** pages.  Target and CLI profile can be set inline in
the navbar.  Click **Refresh** to re-fetch data.

## Workspace links

When the Databricks CLI credentials are configured, job names in the
overview table link directly to the workspace job page (e.g.
`https://<workspace>/jobs/<job_id>`).  This lets you access run
history, task DAGs, and task-level details in the native Databricks UI.

## Programmatic usage

The data functions can be used independently of the Dash UI:

```python
from databricks_bundle_decorators.dashboard import (
    fetch_job_runs,
    resolve_job_ids,
    resolve_workspace_url,
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
