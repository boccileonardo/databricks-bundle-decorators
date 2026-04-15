# Observability Dashboard

A Dash-based dashboard deployed as a native
[Databricks App](https://docs.databricks.com/aws/en/dev-tools/databricks-apps)
— accessible to your team via a browser, with no local setup required.

The dashboard shows job KPIs and backfill completeness for all jobs
deployed from the current bundle.  For run history, task DAGs, and
task-level details, the dashboard links directly to the Databricks
workspace UI.

Authentication is handled automatically via the service principal that
Databricks provisions for each app.

## Quick start

```bash
uv add databricks-bundle-decorators[app]
uv run dbxdec init --dashboard
databricks bundle deploy
databricks bundle run <app_resource_key>
```

The resource key is printed during `init` and lives in
`resources/app.yml` (e.g. `my_project_observability`).

!!! warning
    Both `deploy` **and** `run` are required.  `deploy` alone does not
    start the app — `run` pushes the source code to compute.

## Generated files

| File | Purpose |
|---|---|
| `app/app.py` | Dash entry point |
| `app/app.yaml` | App runtime configuration |
| `app/pyproject.toml` | Dependencies (`>=3.12`) |
| `app/uv.lock` | Lock file (tells Databricks Apps to use `uv`) |
| `app/registry.json` | Serialised job list and backfill definitions |
| `resources/app.yml` | Bundle app resource (resource bindings, permissions) |

## Updating after job changes

When you **add or remove** `@job` definitions:

```bash
uv run dbxdec app-config
databricks bundle deploy
databricks bundle run <app_resource_key>
```

Changing backfill parameters (e.g. `start_date`, `tz`) on existing
jobs does **not** require `dbxdec app-config` — `registry.json` is
updated automatically on every `databricks bundle deploy`.

## Adding the dashboard to an existing project

??? example "Step-by-step for existing projects"

    **1. Install the app extra**

    ```bash
    uv add databricks-bundle-decorators[app]
    ```

    **2. Scaffold the app files**

    ```bash
    uv run dbxdec init --dashboard
    ```

    **3. Add `include` to `databricks.yaml`** (if not already present)

    ```yaml
    include:
      - resources/*.yml
    ```

    **4. Deploy and start**

    ```bash
    databricks bundle deploy
    databricks bundle run <app_resource_key>
    ```

## Local testing

Run the app locally from the `app/` directory:

```bash
cd app
DATABRICKS_APP_NAME=<app-name> databricks apps run-local --prepare-environment
```

The app resolves job IDs via `WorkspaceClient().apps.get()`, so
`DATABRICKS_APP_NAME` must be set to your deployed app name.

## Customization

### Permission level

The app's service principal gets `CAN_VIEW` by default.  Use
`--permission` to change it:

```bash
uv run dbxdec app-config --permission CAN_MANAGE_RUN
```

This also works with `dbxdec init --dashboard --permission CAN_MANAGE_RUN`.

### Source path

If your app lives somewhere other than `./app`:

```yaml
source_code_path: ./observability
```

## Dashboard pages

- **Overview** — KPI cards (registered jobs, deployed count, total runs,
  success rate, failures, average duration) and a job table with status,
  run counts, and backfill completeness.  Job names link to the
  Databricks workspace when available.
- **Backfills** — summary table of backfill completeness across all jobs.
  Click a job name to drill down.
- **Backfill Detail** (`/backfills/<name>`) — per-job completeness
  percentage, heatmap with date-range picker, and list of missing keys.

Each backfill type gets a dedicated visualization:

| Backfill type | Visualization |
|---------------|---------------|
| `DailyBackfill` | GitHub-style calendar heatmap (weekday rows × week columns) |
| `WeeklyBackfill` | Year × week grid (W01–W53) |
| `MonthlyBackfill` | Year × month grid (Jan–Dec) |
| `HourlyBackfill` | Date × hour grid (00–23) |
| `StaticBackfill` | Single-row partition grid |

| Color | Meaning |
|-------|----------|
| 🟩 Green | Completed |
| 🟦 Blue | In progress |
| 🟥 Red | Failed |
| 🟨 Amber | Missing |
| Light blue | Not started (future) |
| Gray | Not in range |

## Known limitations

The `databricks-bundles` Python SDK does not support `App` as a resource
type — only `Job` is supported via `Resources.add_resource()`.  This is
why the app resource must be declared in a separate YAML file
(`resources/app.yml`) and why `dbxdec app-config` is needed when jobs
are added or removed.  If the SDK adds `App` support, this complexity
goes away — the app resource would be generated alongside jobs in
`load_resources()` with no manual step.
