"""CLI for databricks-bundle-decorators.

Provides scaffolding commands for pipeline repositories.

Usage::

    uv run dbxdec init
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated, Any

import typer

try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef]

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
    import {package_name}.pipelines  # noqa: F401 – triggers decorator registration
    from databricks_bundle_decorators.codegen import generate_resources

    resources = Resources()
    for key, job_resource in generate_resources().items():
        resources.add_resource(key, job_resource)
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

_EXAMPLE_PIPELINE = '''\
"""Example pipeline – demonstrates task dependencies, IoManager, and parameters.

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
# IoManager – persist DataFrames as Parquet (works with any cloud or local path)
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
# Job – inline TaskFlow pattern
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
        """Final consumer – print the result (replace with your own logic)."""
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
# Job – libraries=[] because the package is pre-installed in the image
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

# No artifacts section needed – the package is pre-installed
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


# --- Init command ----------------------------------------------------------


def _cmd_init(*, docker: bool = False) -> None:
    """Scaffold a new databricks-bundle-decorators pipeline project."""
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
    _write(
        cwd / "resources" / "__init__.py",
        _RESOURCES_INIT.format(package_name=package_name),
    )

    # 2. pipelines/__init__.py  (auto-discovery)
    _write(pkg_dir / "pipelines" / "__init__.py", _PIPELINES_INIT)

    # 3. Example pipeline
    _write(
        pkg_dir / "pipelines" / "example.py",
        _DOCKER_EXAMPLE_PIPELINE if docker else _EXAMPLE_PIPELINE,
    )

    # 4. databricks.yaml
    _write(
        cwd / "databricks.yaml",
        (_DOCKER_DATABRICKS_YAML if docker else _DATABRICKS_YAML).format(
            project_name=project_name,
            package_name=package_name,
        ),
    )

    # 5. Ensure package __init__.py exists
    _write(pkg_dir / "__init__.py", "")

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

    print()
    print("Done! Define your @task and @job functions in the pipelines/ directory.")


# --- Backfill command ------------------------------------------------------


def _cmd_backfill(
    *,
    job_name: str,
    start: str | None = None,
    end: str | None = None,
    keys: str | None = None,
    max_concurrent: int | None = None,
    dry_run: bool = False,
    wait: bool = False,
    profile: str | None = None,
    host: str | None = None,
) -> None:
    """Trigger one Databricks job run per partition key."""
    import asyncio

    from databricks_bundle_decorators.discovery import discover_pipelines
    from databricks_bundle_decorators.partitions import (
        LOGICAL_DATE_PARAM,
        PartitionDef,
    )
    from databricks_bundle_decorators.registry import _JOB_REGISTRY

    # 1. Populate registries
    discover_pipelines()

    job_meta = _JOB_REGISTRY.get(job_name)
    if job_meta is None:
        available = sorted(_JOB_REGISTRY.keys())
        print(f"Error: Job '{job_name}' not found.", file=sys.stderr)
        if available:
            print(f"Available jobs: {', '.join(available)}", file=sys.stderr)
        sys.exit(1)

    partition: PartitionDef | None = job_meta.partition
    if partition is None and keys is None:
        print(
            f"Error: Job '{job_name}' has no partition definition. "
            f"Use --keys to specify partition keys explicitly.",
            file=sys.stderr,
        )
        sys.exit(1)

    # 2. Enumerate keys
    if keys is not None:
        key_list = [k.strip() for k in keys.split(",") if k.strip()]
    elif partition is not None:
        key_list = partition.partition_keys(start=start, end=end)
    else:
        key_list = []

    if not key_list:
        print("No partition keys to process.", file=sys.stderr)
        sys.exit(1)

    print(f"Job: {job_name}")
    print(f"Partition keys ({len(key_list)}): {', '.join(key_list[:10])}", end="")
    if len(key_list) > 10:
        print(f" ... and {len(key_list) - 10} more")
    else:
        print()

    if dry_run:
        print("\n[DRY RUN] No runs submitted.")
        return

    # 3. Submit runs via Databricks SDK
    try:
        from databricks.sdk import WorkspaceClient  # type: ignore[import-untyped]
    except ImportError:
        print(
            "Error: databricks-sdk is required for backfill. "
            "Install it with: uv add databricks-sdk",
            file=sys.stderr,
        )
        sys.exit(1)

    sdk_kwargs: dict[str, str] = {}
    if profile:
        sdk_kwargs["profile"] = profile
    if host:
        sdk_kwargs["host"] = host

    w = WorkspaceClient(**sdk_kwargs)

    # Find the job by name (exact match)
    matching_jobs = [
        j
        for j in w.jobs.list(name=job_name)
        if j.settings and j.settings.name == job_name
    ]
    if not matching_jobs:
        print(
            f"Error: No deployed job named '{job_name}' found in the workspace.",
            file=sys.stderr,
        )
        sys.exit(1)

    if len(matching_jobs) > 1:
        job_ids = [str(j.job_id) for j in matching_jobs]
        print(
            f"Error: Multiple jobs named '{job_name}' found "
            f"(job_ids: {', '.join(job_ids)}). "
            f"Rename the jobs to be unique.",
            file=sys.stderr,
        )
        sys.exit(1)

    db_job_id = matching_jobs[0].job_id

    concurrency: int = max_concurrent or len(key_list)
    submitted: list[str] = []
    failed: list[str] = []

    async def _submit_all() -> None:
        sem = asyncio.Semaphore(concurrency)
        waiters: list[tuple[str, Any]] = []  # (key, Wait[Run])

        async def _submit_one(key: str) -> None:
            async with sem:
                try:
                    waiter = await asyncio.to_thread(
                        w.jobs.run_now,
                        job_id=db_job_id,
                        job_parameters={LOGICAL_DATE_PARAM: key},
                    )
                    msg = f"  {key} -> run_id={waiter.run_id}"
                    submitted.append(msg)
                    waiters.append((key, waiter))
                    print(msg)
                except Exception as exc:  # noqa: BLE001
                    failed.append(key)
                    print(f"  {key} -> FAILED: {exc}", file=sys.stderr)

        async with asyncio.TaskGroup() as tg:
            for key in key_list:
                tg.create_task(_submit_one(key))

        if wait and waiters:
            print(f"\nWaiting for {len(waiters)} runs to complete...")

            async def _wait_one(key: str, waiter: Any) -> None:
                try:
                    result = await asyncio.to_thread(waiter.result)
                    state = result.state
                    result_state = (
                        state.result_state.value
                        if state and state.result_state
                        else "UNKNOWN"
                    )
                    if result_state == "SUCCESS":
                        print(f"  {key} -> SUCCESS")
                    else:
                        failed.append(key)
                        print(f"  {key} -> {result_state}", file=sys.stderr)
                except Exception as exc:  # noqa: BLE001
                    failed.append(key)
                    print(f"  {key} -> ERROR: {exc}", file=sys.stderr)

            async with asyncio.TaskGroup() as tg:
                for key, waiter in waiters:
                    tg.create_task(_wait_one(key, waiter))

    try:
        asyncio.run(_submit_all())
    except KeyboardInterrupt:
        print("\nBackfill interrupted.", file=sys.stderr)
        sys.exit(130)

    print(f"\nSubmitted {len(submitted)}/{len(key_list)} runs.")
    if failed:
        print(
            f"Failed keys ({len(failed)}): {', '.join(failed)}",
            file=sys.stderr,
        )
        sys.exit(1)


# --- Typer commands --------------------------------------------------------


@app.command("init")
def init(
    docker: Annotated[
        bool,
        typer.Option(
            help="Generate a Docker-based example pipeline where the "
            "package is pre-installed in a custom container image "
            "instead of uploaded as a wheel.",
        ),
    ] = False,
) -> None:
    """Scaffold a new databricks-bundle-decorators pipeline project."""
    _cmd_init(docker=docker)


@app.command("backfill")
def backfill(
    job_name: Annotated[
        str,
        typer.Argument(help="Name of the @job to backfill"),
    ],
    start: Annotated[
        str | None,
        typer.Option(help="Start of partition range (inclusive), e.g. 2024-01-01"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option(help="End of partition range (inclusive), e.g. 2024-01-31"),
    ] = None,
    keys: Annotated[
        str | None,
        typer.Option(help="Comma-separated list of explicit partition keys"),
    ] = None,
    max_concurrent: Annotated[
        int | None,
        typer.Option(help="Maximum number of concurrent run submissions"),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Print partition keys without submitting runs"),
    ] = False,
    wait: Annotated[
        bool,
        typer.Option(help="Wait for all runs to complete and report success/failure"),
    ] = False,
    profile: Annotated[
        str | None,
        typer.Option(help="Databricks CLI profile name"),
    ] = None,
    host: Annotated[
        str | None,
        typer.Option(help="Databricks workspace URL"),
    ] = None,
) -> None:
    """Submit one Databricks job run per partition key."""
    _cmd_backfill(
        job_name=job_name,
        start=start,
        end=end,
        keys=keys,
        max_concurrent=max_concurrent,
        dry_run=dry_run,
        wait=wait,
        profile=profile,
        host=host,
    )


# --- Main ------------------------------------------------------------------


def main() -> None:
    try:
        app(standalone_mode=False)
    except SystemExit:
        raise
    except Exception:
        # standalone_mode=False doesn't convert click errors to SystemExit.
        # Re-raise as SystemExit so the CLI behaves correctly for end users
        # (e.g. no-args-is-help, missing required argument, etc.).
        sys.exit(1)
