"""CLI for databricks-bundle-decorators.

Provides scaffolding commands for pipeline repositories.

Usage::

    uv run dbxdec init
"""

from __future__ import annotations

import importlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Annotated

import typer

from databricks_bundle_decorators.backfill import (
    BACKFILL_KEY_PARAM,
    EXACT_BACKFILL_PARAM,
    BackfillDef,
)
from databricks_bundle_decorators.discovery import discover_pipelines
from databricks_bundle_decorators.registry import _JOB_REGISTRY

try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11
    import tomli as tomllib  # ty: ignore[unresolved-import]

app = typer.Typer(
    name="dbxdec",
    help="databricks-bundle-decorators CLI",
    no_args_is_help=True,
)


def _read_pyproject(cwd: Path) -> dict:
    """Read and parse pyproject.toml from *cwd*."""
    path = cwd / "pyproject.toml"
    if not path.exists():
        print(
            "Error: No pyproject.toml found in the current directory.", file=sys.stderr
        )
        print(
            "Run this command from the root of your pipeline project.", file=sys.stderr
        )
        sys.exit(1)
    return tomllib.loads(path.read_text())


def _detect_package_name(pyproject: dict) -> str:
    """Derive the Python import name from the project name in pyproject.toml."""
    name = pyproject.get("project", {}).get("name")
    if not name:
        print("Error: No [project].name found in pyproject.toml.", file=sys.stderr)
        sys.exit(1)
    return name.replace("-", "_")


def _detect_src_layout(cwd: Path, package_name: str) -> Path:
    """Return the package directory, detecting flat or src layout."""
    src_path = cwd / "src" / package_name
    flat_path = cwd / package_name
    if src_path.exists():
        return src_path
    if flat_path.exists():
        return flat_path
    # Default to src layout (will be created)
    return src_path


def _read_app_name_from_yml(cwd: Path) -> str | None:
    """Extract the app name from an existing ``resources/app.yml``.

    Parses the generated YAML structure to find the resource key under
    ``resources.apps`` and converts it back to the app name (replacing
    underscores with hyphens).

    Returns ``None`` if the file does not exist or cannot be parsed.
    """
    app_yml = cwd / "resources" / "app.yml"
    if not app_yml.exists():
        return None
    # The generated YAML has a known structure:
    #   resources:
    #     apps:
    #       <resource_key>:
    #         name: <app_name>
    # We look for the "name:" field at the 4th indentation level.
    for line in app_yml.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("name:") and line.startswith("      "):
            return stripped.removeprefix("name:").strip()
    return None


# --- File templates -------------------------------------------------------

_RESOURCES_INIT = '''\
"""Resource loader for ``databricks bundle deploy``.

Referenced from ``python.resources`` in ``databricks.yaml``::

    python:
      venv_path: .venv
      resources:
        - 'resources:load_resources'
"""

from databricks.bundles.core import Bundle, Resources


def load_resources(bundle: Bundle) -> Resources:
    """Entry-point called by ``databricks bundle deploy``."""
    import {package_name}.pipelines  # noqa: F401 - triggers decorator registration
    from databricks_bundle_decorators.codegen import generate_resources

    resources = Resources()
    for key, job_resource in generate_resources({generate_kwargs}).items():
        resources.add_resource(key, job_resource)

    # Keep app/registry.json in sync with backfill definitions
    from databricks_bundle_decorators.app._codegen import sync_registry_json
    sync_registry_json()

    return resources
'''

_PIPELINES_INIT = '''\
"""Pipeline auto-discovery.

Every .py module in this package is imported automatically, triggering
@task / @job / @job_cluster decorator registration.
"""



import importlib
import pkgutil

for _loader, _module_name, _is_pkg in pkgutil.walk_packages(__path__):
    importlib.import_module(f"{__name__}.{_module_name}")
'''

_DATABRICKS_YAML = """\
bundle:
  name: {project_name}

artifacts:
  {package_name}:
    type: whl
    build: uv build --wheel
    path: .

python:
  venv_path: .venv
  resources:
    - 'resources:load_resources'

targets:
  dev:
    mode: development
    workspace:
      host: https://<your-workspace>.azuredatabricks.net/
"""

_DATABRICKS_YAML_WITH_APP = """\
bundle:
  name: {project_name}

include:
  - resources/*.yml

artifacts:
  {package_name}:
    type: whl
    build: uv build --wheel
    path: .

python:
  venv_path: .venv
  resources:
    - 'resources:load_resources'

targets:
  dev:
    mode: development
    workspace:
      host: https://<your-workspace>.azuredatabricks.net/
"""

_EXAMPLE_PIPELINE = '''\
"""Example pipeline - demonstrates task dependencies, IoManager, and parameters.

Shows the TaskFlow pattern:
- ``@job_cluster`` for shared cluster configuration
- ``@task`` with dependencies (pass a task result to another task)
- Built-in ``PolarsParquetIoManager`` for DataFrame persistence between tasks
- ``get_dbutils`` for accessing secrets at runtime
- ``params`` for job-level parameter access
Requires the cloud extra, e.g.::

    uv add databricks-bundle-decorators[azure]   # adlfs + fsspec + polars
    uv add databricks-bundle-decorators[aws]     # s3fs + fsspec + polars
    uv add databricks-bundle-decorators[gcp]     # gcsfs + fsspec + polars"""

import polars as pl

from databricks_bundle_decorators import (
    get_dbutils,
    job,
    job_cluster,
    params,
    task,
)
from databricks_bundle_decorators.io_managers import PolarsParquetIoManager


# ---------------------------------------------------------------------------
# IoManager - persist DataFrames as Parquet (works with any cloud or local path)
# ---------------------------------------------------------------------------

def _storage_options() -> dict[str, str]:
    """Resolve cloud storage credentials lazily at runtime.

    This callable is invoked by the IoManager only when reading/writing
    data on a Databricks cluster \u2013 never during local ``bundle deploy``.
    """
    dbutils = get_dbutils()
    key = dbutils.secrets.get(scope="my_scope", key="storage-access-key")
    return {"account_name": "mystorageaccount", "account_key": key}


staging_io = PolarsParquetIoManager(
    base_path="abfss://datalake@mystorageaccount.dfs.core.windows.net/staging",
    storage_options=_storage_options,
)


# ---------------------------------------------------------------------------
# Shared job cluster
# ---------------------------------------------------------------------------

default_cluster = job_cluster(
    name="default_cluster",
    spark_version="16.4.x-scala2.12",
    node_type_id="Standard_DS3_v2",
    num_workers=2,
)


# ---------------------------------------------------------------------------
# Job - inline TaskFlow pattern
# ---------------------------------------------------------------------------


@job(
    params={"source_url": "https://api.github.com/events", "limit": "10"},
    cluster=default_cluster,
)
def example_job():
    @task(io_manager=staging_io)
    def extract() -> pl.DataFrame:
        """Fetch data from a remote API and return a DataFrame."""
        import requests

        url = params["source_url"]
        response = requests.get(url)
        response.raise_for_status()
        return pl.DataFrame(response.json())

    @task(io_manager=staging_io)
    def transform(raw_df: pl.DataFrame) -> pl.DataFrame:
        """Apply filtering/transformations to the raw data."""
        limit = int(params["limit"])
        return raw_df.head(limit)

    @task
    def summarize(clean_df: pl.DataFrame) -> None:
        """Final consumer - print the result (replace with your own logic)."""
        print(f"Loaded {len(clean_df)} rows:")
        print(clean_df)

    raw = extract()
    clean = transform(raw)
    summarize(clean)
'''

_DOCKER_EXAMPLE_PIPELINE = '''\
"""Example pipeline using a pre-built Docker image.

Demonstrates how to deploy when your package and dependencies are
baked into a custom Docker image:

- ``libraries=[]`` tells the framework **not** to attach a wheel,
  because the package is already installed in the image.
- ``docker_image`` on the cluster specifies the container image.
- Everything else (TaskFlow DAG, IoManager, params) works the same.

Build and push your image with the package pre-installed::

    docker build -t my-registry.io/my-pipeline:latest .
    docker push my-registry.io/my-pipeline:latest
"""

from databricks_bundle_decorators import (
    job,
    job_cluster,
    params,
    task,
)


# ---------------------------------------------------------------------------
# Shared job cluster (with Docker image)
# ---------------------------------------------------------------------------

default_cluster = job_cluster(
    name="docker_cluster",
    spark_version="16.4.x-scala2.12",
    node_type_id="Standard_DS3_v2",
    num_workers=2,
    docker_image={
        "url": "my-registry.io/my-pipeline:latest",
    },
)


# ---------------------------------------------------------------------------
# Job - libraries=[] because the package is pre-installed in the image
# ---------------------------------------------------------------------------


@job(
    params={"greeting": "hello"},
    cluster=default_cluster,
    libraries=[],
)
def example_docker_job():
    @task
    def produce() -> None:
        """Produce a message (use set_task_value for small data)."""
        from databricks_bundle_decorators import set_task_value

        message = f"{params[\'greeting\']} from Docker!"
        set_task_value("message", message)
        print(message)

    @task
    def consume() -> None:
        """Consume the message via task values."""
        from databricks_bundle_decorators import get_task_value

        message = get_task_value("produce", "message")
        print(f"Received: {message}")

    produce()
    consume()
'''

_DOCKER_DATABRICKS_YAML = """\
bundle:
  name: {project_name}

# No artifacts section needed - the package is pre-installed
# in the Docker image rather than uploaded as a wheel.

python:
  venv_path: .venv
  resources:
    - 'resources:load_resources'

targets:
  dev:
    mode: development
    workspace:
      host: https://<your-workspace>.azuredatabricks.net/
"""


def _add_entry_point_to_pyproject(cwd: Path, package_name: str) -> bool:
    """Append the pipeline entry-point section to *pyproject.toml*.

    Returns ``True`` if the section was added, ``False`` if it already
    existed.
    """
    pyproject_path = cwd / "pyproject.toml"
    content = pyproject_path.read_text()
    if "databricks_bundle_decorators.pipelines" in content:
        return False
    entry_point_block = (
        '\n[project.entry-points."databricks_bundle_decorators.pipelines"]\n'
        f'{package_name} = "{package_name}.pipelines"\n'
    )
    pyproject_path.write_text(content.rstrip() + "\n" + entry_point_block)
    return True


def _generate_app_yml(
    *,
    cwd: Path,
    app_name: str,
    permission: str = "CAN_VIEW",
    created: list[str] | None = None,
) -> Path:
    """Discover pipelines and write ``resources/app.yml``.

    This file is always overwritten because it is generated code.
    Also writes ``app/registry.json`` with serialised backfill
    definitions so the app can display backfill data without
    importing the pipeline package.

    Returns the path to the written YAML file.
    """
    from databricks_bundle_decorators.app._codegen import (  # noqa: PLC0415
        generate_app_config_yaml,
        generate_registry_json,
    )

    discover_pipelines()
    yaml_content = generate_app_config_yaml(app_name, permission=permission)
    app_yml = cwd / "resources" / "app.yml"
    app_yml.parent.mkdir(parents=True, exist_ok=True)
    app_yml.write_text(yaml_content)
    if created is not None:
        created.append(str(app_yml.relative_to(cwd)))

    # Write backfill registry for the app
    registry_json = cwd / "app" / "registry.json"
    registry_json.parent.mkdir(parents=True, exist_ok=True)
    registry_json.write_text(generate_registry_json())
    if created is not None:
        created.append(str(registry_json.relative_to(cwd)))

    return app_yml


# --- Init command ----------------------------------------------------------


def _cmd_init(
    *, docker: bool = False, dashboard: bool = False, permission: str = "CAN_VIEW"
) -> None:
    """Scaffold a new databricks-bundle-decorators pipeline project."""
    if dashboard:
        try:
            importlib.import_module("dash")
        except ImportError:
            print(
                "Error: dash is not installed. "
                "Install the app extras first:\n\n"
                "    uv add databricks-bundle-decorators[app]",
                file=sys.stderr,
            )
            sys.exit(1)

    cwd = Path.cwd()
    pyproject = _read_pyproject(cwd)
    package_name = _detect_package_name(pyproject)
    project_name = pyproject["project"]["name"]
    pkg_dir = _detect_src_layout(cwd, package_name)

    created: list[str] = []
    skipped: list[str] = []

    def _write(path: Path, content: str) -> None:
        if path.exists():
            skipped.append(str(path.relative_to(cwd)))
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        created.append(str(path.relative_to(cwd)))

    # 1. resources/__init__.py
    if dashboard:
        app_resource_key = f"{project_name}-observability".replace("-", "_")
        generate_kwargs = f'app_resource_key="{app_resource_key}"'
    else:
        generate_kwargs = ""
    _write(
        cwd / "resources" / "__init__.py",
        _RESOURCES_INIT.format(
            package_name=package_name,
            generate_kwargs=generate_kwargs,
        ),
    )

    # 2. pipelines/__init__.py  (auto-discovery)
    _write(pkg_dir / "pipelines" / "__init__.py", _PIPELINES_INIT)

    # 3. Example pipeline
    _write(
        pkg_dir / "pipelines" / "example.py",
        _DOCKER_EXAMPLE_PIPELINE if docker else _EXAMPLE_PIPELINE,
    )

    # 4. databricks.yaml
    if docker:
        yaml_template = _DOCKER_DATABRICKS_YAML
    elif dashboard:
        yaml_template = _DATABRICKS_YAML_WITH_APP
    else:
        yaml_template = _DATABRICKS_YAML
    _write(
        cwd / "databricks.yaml",
        yaml_template.format(
            project_name=project_name,
            package_name=package_name,
        ),
    )

    # 5. Ensure package __init__.py exists
    _write(pkg_dir / "__init__.py", "")

    # 6. Dashboard app files (when --dashboard is used)
    if dashboard:
        from databricks_bundle_decorators.app._template import (  # noqa: PLC0415
            APP_PY_TEMPLATE,
            APP_PYPROJECT_TEMPLATE,
            APP_YAML_TEMPLATE,
        )

        _write(
            cwd / "app" / "app.py",
            APP_PY_TEMPLATE,
        )
        _write(
            cwd / "app" / "app.yaml",
            APP_YAML_TEMPLATE,
        )
        _write(
            cwd / "app" / "pyproject.toml",
            APP_PYPROJECT_TEMPLATE,
        )

        # Generate uv.lock so Databricks Apps uses uv (not pip)
        app_dir = cwd / "app"
        if not (app_dir / "uv.lock").exists():
            uv_bin = shutil.which("uv")
            if uv_bin is None:
                print(
                    "Warning: uv not found on PATH. "
                    "Run 'uv lock' inside app/ manually to generate uv.lock.",
                    file=sys.stderr,
                )
            else:
                subprocess.run(  # noqa: S603
                    [uv_bin, "lock"],
                    cwd=app_dir,
                    check=True,
                    capture_output=True,
                )
                created.append("app/uv.lock")

        # Generate resources/app.yml from registry (always overwrite)
        _generate_app_yml(
            cwd=cwd,
            app_name=f"{project_name}-observability",
            permission=permission,
            created=created,
        )

    # --- Summary -----------------------------------------------------------
    print()
    print("databricks-bundle-decorators project initialized!")
    print()

    if created:
        print("Created:")
        for f in created:
            print(f"  {f}")

    if skipped:
        print("Skipped (already exist):")
        for f in skipped:
            print(f"  {f}")

    # --- Add entry point to pyproject.toml --------------------------------
    entry_point_added = _add_entry_point_to_pyproject(cwd, package_name)
    if entry_point_added:
        print("Modified:")
        print("  pyproject.toml (added pipeline entry point)")

    # --- Dashboard: check databricks.yaml includes app.yml ----------------
    if dashboard:
        databricks_yaml = cwd / "databricks.yaml"
        if databricks_yaml.exists():
            yaml_text = databricks_yaml.read_text()
            if "resources/*.yml" not in yaml_text and "app.yml" not in yaml_text:
                print()
                print("NOTE: Add the following to your databricks.yaml to")
                print("include the generated app resource:")
                print()
                print("  include:")
                print("    - resources/*.yml")

    print()
    print("Done! Define your @task and @job functions in the pipelines/ directory.")

    if dashboard:
        print()
        print("To deploy the app:")
        print()
        print("  databricks bundle deploy")
        print("  databricks bundle run <app_resource_key>")
        print()
        print(
            "The resource key is in resources/app.yml (e.g. my_project_observability)."
        )


# --- Backfill command ------------------------------------------------------


def _cmd_backfill(
    *,
    job_name: str,
    start: str | None = None,
    end: str | None = None,
    keys: str | None = None,
    dry_run: bool = False,
    wait: bool = False,
    exact: bool = False,
    reverse: bool = False,
    target: str | None = None,
    profile: str | None = None,
) -> None:
    """Trigger one Databricks job run per backfill key.

    Uses ``databricks bundle run`` under the hood, which automatically
    resolves the deployed job name (including any dev-mode prefix).
    """
    # 1. Populate registries
    discover_pipelines()

    job_meta = _JOB_REGISTRY.get(job_name)
    if job_meta is None:
        available = sorted(_JOB_REGISTRY.keys())
        print(f"Error: Job '{job_name}' not found.", file=sys.stderr)
        if available:
            print(f"Available jobs: {', '.join(available)}", file=sys.stderr)
        else:
            print(
                "No jobs were discovered. Ensure your package is installed "
                "(e.g. 'uv pip install -e .') and has an entry point "
                "under [project.entry-points.'databricks_bundle_decorators."
                "pipelines'] in pyproject.toml.",
                file=sys.stderr,
            )
        sys.exit(1)

    backfill_def: BackfillDef | None = job_meta.backfill
    if backfill_def is None and keys is None:
        print(
            f"Error: Job '{job_name}' has no backfill definition. "
            f"Use --keys to specify keys explicitly.",
            file=sys.stderr,
        )
        sys.exit(1)

    # 2. Enumerate keys
    if keys is not None:
        key_list = [k.strip() for k in keys.split(",") if k.strip()]
    elif backfill_def is not None:
        key_list = backfill_def.keys(start=start, end=end)
    else:
        key_list = []

    # 3. Sort keys (ascending by default, descending with --reverse)
    key_list = sorted(key_list, reverse=reverse)

    if not key_list:
        print("No backfill keys to process.", file=sys.stderr)
        sys.exit(1)

    print(f"Job: {job_name}")
    print(f"Backfill keys ({len(key_list)}): {', '.join(key_list[:10])}", end="")
    if len(key_list) > 10:
        print(f" ... and {len(key_list) - 10} more")
    else:
        print()

    if dry_run:
        print("\n[DRY RUN] No runs submitted.")
        return

    _submit_backfill_runs(
        job_name=job_name,
        key_list=key_list,
        wait=wait,
        exact=exact,
        target=target,
        profile=profile,
    )


def _submit_backfill_runs(
    *,
    job_name: str,
    key_list: list[str],
    wait: bool = False,
    exact: bool = False,
    target: str | None = None,
    profile: str | None = None,
) -> None:
    """Submit one ``databricks bundle run`` per backfill key.

    Runs are submitted sequentially in the order of *key_list*.
    Databricks handles concurrency via its job-level
    ``max_concurrent_runs`` setting and run queue.

    Shared by ``backfill`` and ``catchup`` commands.
    """
    if shutil.which("databricks") is None:
        print(
            "Error: 'databricks' CLI not found on PATH. "
            "Install it: https://docs.databricks.com/dev-tools/cli/install.html",
            file=sys.stderr,
        )
        sys.exit(1)

    base_cmd: list[str] = ["databricks", "bundle", "run", job_name]
    if target:
        base_cmd += ["--target", target]
    if profile:
        base_cmd += ["--profile", profile]

    submitted: list[str] = []
    failed: list[str] = []

    for key in key_list:
        params_val = f"{BACKFILL_KEY_PARAM}={key}"
        if exact:
            params_val += f",{EXACT_BACKFILL_PARAM}=1"
        cmd = [*base_cmd, "--params", params_val]
        if not wait:
            cmd.append("--no-wait")
        try:
            result = subprocess.run(  # noqa: S603
                cmd, capture_output=True, text=True, check=False
            )
            if result.returncode == 0:
                output = result.stdout.strip()
                label = "OK" if wait else "submitted"
                msg = f"  {key} -> {label}"
                if output:
                    last_line = output.splitlines()[-1]
                    msg = f"  {key} -> {last_line}"
                submitted.append(key)
                print(msg)
            else:
                failed.append(key)
                err = result.stderr.strip() or result.stdout.strip()
                print(f"  {key} -> FAILED: {err}", file=sys.stderr)
        except KeyboardInterrupt:
            print("\nBackfill interrupted.", file=sys.stderr)
            sys.exit(130)
        except Exception as exc:  # noqa: BLE001
            failed.append(key)
            print(f"  {key} -> FAILED: {exc}", file=sys.stderr)

    action = "Completed" if wait else "Submitted"
    print(f"\n{action} {len(submitted)}/{len(key_list)} runs.")
    if failed:
        print(
            f"Failed keys ({len(failed)}): {', '.join(failed)}",
            file=sys.stderr,
        )
        sys.exit(1)


def _get_job_id_from_bundle(
    job_name: str,
    target: str | None,
    profile: str | None,
) -> str:
    """Resolve the deployed Databricks job ID from the bundle state.

    Shells out to ``databricks bundle summary --output json`` and
    extracts the numeric job ID for *job_name*.
    """
    if shutil.which("databricks") is None:
        print(
            "Error: 'databricks' CLI not found on PATH. "
            "Install it: https://docs.databricks.com/dev-tools/cli/install.html",
            file=sys.stderr,
        )
        sys.exit(1)

    cmd: list[str] = ["databricks", "bundle", "summary", "--output", "json"]
    if target:
        cmd += ["--target", target]
    if profile:
        cmd += ["--profile", profile]

    result = subprocess.run(cmd, capture_output=True, text=True)  # noqa: PLW1510, S603
    if result.returncode != 0:
        err = result.stderr.strip() or result.stdout.strip()
        print(
            f"Error: 'databricks bundle summary' failed: {err}",
            file=sys.stderr,
        )
        sys.exit(1)

    summary = json.loads(result.stdout)
    jobs = summary.get("resources", {}).get("jobs", {})
    job_resource = jobs.get(job_name)
    if job_resource is None:
        available = sorted(jobs.keys())
        print(
            f"Error: Job '{job_name}' not found in bundle summary.",
            file=sys.stderr,
        )
        if available:
            print(f"Available jobs: {', '.join(available)}", file=sys.stderr)
        sys.exit(1)

    job_id: str | None = job_resource.get("id")
    if not job_id:
        print(
            f"Error: Job '{job_name}' has no ID in the bundle state. "
            f"Has the bundle been deployed?",
            file=sys.stderr,
        )
        sys.exit(1)
    return job_id


def _get_launched_backfill_keys(
    job_id: str,
    _target: str | None,
    profile: str | None,
) -> set[str]:
    """Return the set of ``backfill_key`` values already launched.

    Includes both active and completed (successful) runs so that
    in-flight runs are not relaunched.  Terminally failed runs
    (``FAILED``, ``TIMED_OUT``, ``CANCELED``) are **excluded** so
    they will be retried on the next catchup.

    Shells out to ``databricks jobs list-runs`` with JSON output.
    The CLI handles pagination internally and returns all runs as
    a JSON array.
    """
    #: Terminal result states that should NOT block a re-run.
    _failed_states = {"FAILED", "TIMEDOUT", "TIMED_OUT", "CANCELED", "CANCELLED"}

    cmd: list[str] = [
        "databricks",
        "jobs",
        "list-runs",
        "--job-id",
        job_id,
        "--output",
        "json",
    ]
    if profile:
        cmd += ["--profile", profile]

    result = subprocess.run(cmd, capture_output=True, text=True)  # noqa: PLW1510, S603
    if result.returncode != 0:
        err = result.stderr.strip() or result.stdout.strip()
        print(
            f"Error: 'databricks jobs list-runs' failed: {err}",
            file=sys.stderr,
        )
        sys.exit(1)

    runs: list[dict] = json.loads(result.stdout)
    launched: set[str] = set()
    for run in runs:
        state = run.get("state", {})
        result_state = state.get("result_state")
        # Skip terminally-failed runs so they get retried.
        # Active runs have result_state=None — we keep those.
        if result_state in _failed_states:
            continue
        for param in run.get("job_parameters", []):
            if param.get("name") == "backfill_key":
                launched.add(param["value"])

    return launched


def _cmd_backfill_catchup(
    *,
    job_name: str,
    dry_run: bool = False,
    wait: bool = False,
    reverse: bool = False,
    target: str | None = None,
    profile: str | None = None,
) -> None:
    """Determine and submit missing backfill runs for a job.

    Enumerates all keys from the job's backfill definition, queries
    the Databricks API for successful past runs, computes the
    difference, and submits the missing keys.
    """
    # 1. Populate registries
    discover_pipelines()

    job_meta = _JOB_REGISTRY.get(job_name)
    if job_meta is None:
        available = sorted(_JOB_REGISTRY.keys())
        print(f"Error: Job '{job_name}' not found.", file=sys.stderr)
        if available:
            print(f"Available jobs: {', '.join(available)}", file=sys.stderr)
        else:
            print(
                "No jobs were discovered. Ensure your package is installed "
                "(e.g. 'uv pip install -e .') and has an entry point "
                "under [project.entry-points.'databricks_bundle_decorators."
                "pipelines'] in pyproject.toml.",
                file=sys.stderr,
            )
        sys.exit(1)

    backfill_def: BackfillDef | None = job_meta.backfill
    if backfill_def is None:
        print(
            f"Error: Job '{job_name}' has no backfill definition. "
            f"catchup requires a backfill= on the @job decorator.",
            file=sys.stderr,
        )
        sys.exit(1)

    # 2. Enumerate all keys
    all_keys = backfill_def.keys()

    # 3. Get the deployed job ID from the bundle state
    job_id = _get_job_id_from_bundle(job_name, target, profile)

    # 4. Get already-launched keys (active + successful) from Databricks
    launched_keys = _get_launched_backfill_keys(job_id, target, profile)

    # 5. Compute missing (sorted ascending; reversed if --reverse)
    all_keys_set = set(all_keys)
    missing_keys = sorted(
        [k for k in all_keys if k not in launched_keys], reverse=reverse
    )

    print(f"Job: {job_name}")
    print(f"All backfill keys: {len(all_keys)}")
    print(f"Already launched: {len(all_keys_set & launched_keys)}")
    print(f"Missing: {len(missing_keys)}")

    if not missing_keys:
        print("\nAll backfill keys have been completed!")
        return

    # Show a preview of the missing keys
    preview = ", ".join(missing_keys[:10])
    if len(missing_keys) > 10:
        preview += f" ... and {len(missing_keys) - 10} more"
    print(f"Missing keys: {preview}")

    if dry_run:
        print("\n[DRY RUN] No runs submitted.")
        return

    _submit_backfill_runs(
        job_name=job_name,
        key_list=missing_keys,
        wait=wait,
        target=target,
        profile=profile,
    )


# --- Typer commands --------------------------------------------------------


@app.command("init")
def init(
    docker: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            help="Generate a Docker-based example pipeline where the "
            "package is pre-installed in a custom container image "
            "instead of uploaded as a wheel.",
        ),
    ] = False,
    dashboard: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            help="Scaffold a Databricks App observability dashboard "
            "under app/. Requires the [app] extra "
            "(uv add databricks-bundle-decorators[app]).",
        ),
    ] = False,
    permission: Annotated[
        str,
        typer.Option(
            help="Permission level granted to the app's service "
            "principal on each job (e.g. CAN_VIEW, CAN_MANAGE_RUN). "
            "Only used with --dashboard.",
        ),
    ] = "CAN_VIEW",
) -> None:
    """Scaffold a new databricks-bundle-decorators pipeline project."""
    _cmd_init(docker=docker, dashboard=dashboard, permission=permission)


@app.command("backfill")
def backfill(
    job_name: Annotated[
        str,
        typer.Argument(help="Name of the @job to backfill"),
    ],
    start: Annotated[
        str | None,
        typer.Option(help="Start of backfill range (inclusive), e.g. 2024-01-01"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option(help="End of backfill range (inclusive), e.g. 2024-01-31"),
    ] = None,
    keys: Annotated[
        str | None,
        typer.Option(help="Comma-separated list of explicit backfill keys"),
    ] = None,
    dry_run: Annotated[  # noqa: FBT002
        bool,
        typer.Option("--dry-run", help="Print backfill keys without submitting runs"),
    ] = False,
    wait: Annotated[  # noqa: FBT002
        bool,
        typer.Option(help="Wait for all runs to complete and report success/failure"),
    ] = False,
    exact: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            "--exact",
            help="Bypass lookback and schedule-gap expansion; each key "
            "processes only its own partition.",
        ),
    ] = False,
    reverse: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            "--reverse",
            help="Submit keys in descending order (most recent first).",
        ),
    ] = False,
    target: Annotated[
        str | None,
        typer.Option(
            "--target",
            "-t",
            help="Databricks bundle target (e.g. dev, staging, prod)",
        ),
    ] = None,
    profile: Annotated[
        str | None,
        typer.Option(help="Databricks CLI profile name"),
    ] = None,
) -> None:
    """Submit one Databricks job run per backfill key via ``databricks bundle run``."""
    _cmd_backfill(
        job_name=job_name,
        start=start,
        end=end,
        keys=keys,
        dry_run=dry_run,
        wait=wait,
        exact=exact,
        reverse=reverse,
        target=target,
        profile=profile,
    )


@app.command("catchup")
def catchup(
    job_name: Annotated[
        str,
        typer.Argument(help="Name of the @job to catch up"),
    ],
    dry_run: Annotated[  # noqa: FBT002
        bool,
        typer.Option("--dry-run", help="Print missing keys without submitting runs"),
    ] = False,
    wait: Annotated[  # noqa: FBT002
        bool,
        typer.Option(help="Wait for all runs to complete and report success/failure"),
    ] = False,
    reverse: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            "--reverse",
            help="Submit keys in descending order (most recent first).",
        ),
    ] = False,
    target: Annotated[
        str | None,
        typer.Option(
            "--target",
            "-t",
            help="Databricks bundle target (e.g. dev, staging, prod)",
        ),
    ] = None,
    profile: Annotated[
        str | None,
        typer.Option(help="Databricks CLI profile name"),
    ] = None,
) -> None:
    """Submit missing backfill runs for a job.

    Enumerates all keys from the job's backfill definition, checks
    which runs are already active or completed, and submits only
    the missing ones.
    """
    _cmd_backfill_catchup(
        job_name=job_name,
        dry_run=dry_run,
        wait=wait,
        reverse=reverse,
        target=target,
        profile=profile,
    )


@app.command("app-config")
def app_config(
    permission: Annotated[
        str,
        typer.Option(
            help="Permission level granted to the app's service "
            "principal on each job (e.g. CAN_VIEW, CAN_MANAGE_RUN).",
        ),
    ] = "CAN_VIEW",
    name: Annotated[
        str | None,
        typer.Option(
            help="Override the Databricks App name. "
            "If not provided, the name is read from the existing "
            "resources/app.yml; if that file doesn't exist, it is "
            "derived from the project name in pyproject.toml.",
        ),
    ] = None,
) -> None:
    """Regenerate ``resources/app.yml`` and ``app/registry.json``.

    Run this after adding or removing ``@job`` definitions to keep the
    Databricks App resource and backfill metadata in sync.  Both files
    are always overwritten.

    Requires the ``[app]`` extra::

        uv add databricks-bundle-decorators[app]
    """
    try:
        importlib.import_module("dash")
    except ImportError:
        print(
            "Error: dash is not installed. "
            "Install the app extras first:\n\n"
            "    uv add databricks-bundle-decorators[app]",
            file=sys.stderr,
        )
        sys.exit(1)

    cwd = Path.cwd()

    if name is not None:
        app_name = name
    else:
        # Preserve existing name from resources/app.yml if present
        app_name = _read_app_name_from_yml(cwd)
        if app_name is None:
            pyproject = _read_pyproject(cwd)
            project_name = pyproject["project"]["name"]
            app_name = f"{project_name}-observability"

    created: list[str] = []
    _generate_app_yml(
        cwd=cwd, app_name=app_name, permission=permission, created=created
    )
    for f in created:
        print(f"Generated: {f}")


# --- Main ------------------------------------------------------------------


def main() -> None:
    try:
        app(standalone_mode=False)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        # standalone_mode=False doesn't convert click errors to SystemExit.
        # Print the error so the user can diagnose the problem.
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
