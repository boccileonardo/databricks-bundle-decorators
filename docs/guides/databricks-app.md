# Databricks App Dashboard

A native [Databricks App](https://docs.databricks.com/aws/en/dev-tools/databricks-apps)
that provides an observability dashboard for your pipelines, running
directly inside your Databricks workspace — accessible to your team via
a browser, with no local setup required.

The app uses the Databricks Python SDK and authenticates via the
**service principal** that Databricks automatically provisions for each
app.  Job IDs are injected at deploy time through bundle resource
bindings — no hardcoding required.

## Installation

The app extras are opt-in.  Install them in your project:

```bash
uv add databricks-bundle-decorators[app]
```

This pulls in `dash`, `plotly`, `dash-bootstrap-components`,
`dash-ag-grid`, and `databricks-sdk`.

## Setup

### 1. Scaffold the app files

Run `dbxdec init` with the `--dashboard` flag:

```bash
uv run dbxdec init --dashboard
```

This creates:

| File | Purpose |
|---|---|
| `app/app.py` | Dash entry point that imports your pipelines and calls `run_app()` |
| `app/app.yaml` | Databricks App runtime configuration |
| `app/requirements.txt` | Python dependencies for the app runtime |
| `resources/app.yml` | Bundle resource definition for the app (auto-generated from registry) |

If you already ran `dbxdec init` previously, existing files are
preserved — the command only creates files that don't exist yet.
You can add the `--dashboard` flag to a subsequent run to scaffold
only the missing app files.

!!! note
    `resources/app.yml` is always regenerated (not skipped) because it
    is derived from the job registry.  After adding or removing `@job`
    definitions, run `dbxdec app-config` to update it.

??? example "Adding the dashboard to an existing project"

    If you already ran `dbxdec init` before, `databricks.yaml` already
    exists and will be skipped.  Follow these steps:

    **1. Install the app extra**

    ```bash
    uv add databricks-bundle-decorators[app]
    ```

    **2. Scaffold the app files**

    ```bash
    uv run dbxdec init --dashboard
    ```

    This creates `app/app.py`, `app/app.yaml`,
    `app/requirements.txt`, and `resources/app.yml`.

    **3. Add `include` to `databricks.yaml`**

    The command will print a reminder if your existing `databricks.yaml`
    doesn't include the generated YAML.  Add it near the top:

    ```yaml
    bundle:
      name: my-project

    include:
      - resources/*.yml
    ```

    **4. Deploy and start**

    ```bash
    databricks bundle deploy
    databricks bundle run <app_resource_key>
    ```

### 2. Deploy and start

Deploy the bundle, then start the app:

```bash
databricks bundle deploy
databricks bundle run <app_resource_key>
```

Replace `<app_resource_key>` with the app's resource key from
`resources/app.yml` (e.g. `my_project_observability`).

The **deploy** step:

1. Creates the Databricks App with a dedicated service principal.
2. Grants the service principal `CAN_VIEW` on each registered job.
3. Injects `DBXDEC_JOB_<NAME>=<job_id>` environment variables into the
   app runtime so it can discover your jobs.
4. Uploads the `app/` directory to the workspace.

The **run** step deploys the source code to the app's compute and
starts the Dash server.

!!! warning
    `databricks bundle deploy` alone does **not** start the app.
    You must also run `databricks bundle run` to deploy the source
    code to compute.  Without this step the app will show
    "No source code" in the UI, and manually deploying through the
    UI requires navigating the workspace file browser — use
    `bundle run` to avoid that entirely.

After the run completes, find the app in the **Apps** tab of your
workspace sidebar.

## How it works

### Job discovery

At scaffold time, `dbxdec init --dashboard` reads the job registry and
generates `resources/app.yml` — a bundle
[app resource](https://docs.databricks.com/aws/en/dev-tools/bundles/resources#app-resources)
definition.  Each registered job becomes:

- An **app resource binding** with `${resources.jobs.<name>.id}` — the
  bundle resolves this to the actual job ID.
- An **environment variable** (`DBXDEC_JOB_<NAME>`) mapped via
  `valueFrom` to that resource binding.

At app startup, `resolve_job_ids_from_env()` reads all `DBXDEC_JOB_*`
environment variables to build the `{name: job_id}` mapping.

### Authentication

The app uses **app authorization** (service principal).  Databricks
automatically provisions a service principal for each app and injects
`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` into the runtime.
The Databricks Python SDK auto-detects these credentials — no tokens or
profiles to configure.

Because the bundle resource declaration includes `permission: CAN_VIEW`
for each job, the service principal is automatically granted access.
All app users see the same data (no per-user permissions).

### Data fetching

The app calls `WorkspaceClient().jobs.list_runs()` to fetch run history.
The workspace URL is read from `DATABRICKS_HOST` (set automatically by
the Databricks Apps runtime).

## Customization

### Custom permission level

By default, the app's service principal gets `CAN_VIEW` on each job.  To
allow triggering runs from the app in the future, edit the `permission`
field in `resources/app.yml`:

```yaml
resources:
  apps:
    my_project_observability:
      resources:
        - name: my-job
          job:
            id: "${resources.jobs.my_job.id}"
            permission: CAN_MANAGE_RUN
```

### Custom source path

If your app files live somewhere other than `./app`, update the
`source_code_path` in `resources/app.yml`:

```yaml
resources:
  apps:
    my_project_observability:
      source_code_path: ./observability
```

### Updating after job changes

When you add or remove `@job` definitions, regenerate the app resource:

```bash
uv run dbxdec app-config
```

This overwrites `resources/app.yml` with the current registry contents.

## Dashboard pages

The dashboard renders these pages:

- **Overview** — KPI cards, job table with workspace links and backfill
  completeness.
- **Backfills** — summary grid with completeness percentages and status
  squares.
- **Backfill Detail** — per-job completeness heatmap with date-range
  picker.
