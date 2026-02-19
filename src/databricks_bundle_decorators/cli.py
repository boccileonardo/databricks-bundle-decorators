"""CLI for databricks-bundle-decorators.

Provides scaffolding commands for pipeline repositories.

Usage::

    uv run dbxdec init
"""

import argparse
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef]


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
- ``IoManager`` for DataFrame persistence between tasks
- ``params`` for job-level parameter access
"""

from typing import Any

import polars as pl

from databricks_bundle_decorators import (
    IoManager,
    InputContext,
    OutputContext,
    job,
    job_cluster,
    params,
    task,
)


# ---------------------------------------------------------------------------
# IoManager -- persist DataFrames as Parquet on Azure Data Lake Storage Gen2
# ---------------------------------------------------------------------------


class AdlsParquetIoManager(IoManager):
    """Read/write Polars DataFrames as Parquet files on ADLS Gen2.

    Polars natively supports ``abfss://`` paths via ``storage_options``.
    Use ``dbutils.secrets.get()`` to retrieve the storage access key
    at runtime on Databricks.

    Parameters
    ----------
    storage_account:
        Azure storage account name.
    container:
        Blob container / filesystem name.
    base_path:
        Folder prefix inside the container (e.g. ``"staging"``).
    storage_options:
        Dict forwarded to ``polars.write_parquet`` /
        ``polars.read_parquet`` -- must contain ``account_name``
        and ``account_key``.
    """

    def __init__(
        self,
        storage_account: str,
        container: str,
        base_path: str = "data",
        *,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        self.root = (
            f"abfss://{container}@{storage_account}.dfs.core.windows.net/{base_path}"
        )
        self.storage_options = storage_options or {
            "account_name": storage_account,
            "account_key": self._get_access_key(storage_account),
        }

    @staticmethod
    def _get_access_key(storage_account: str) -> str:
        """Retrieve the ADLS access key from Databricks secrets."""
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(
            scope="KeyVault_Scope", key=f"{storage_account}-access-key"
        )

    def _uri(self, task_key: str) -> str:
        return f"{self.root}/{task_key}.parquet"

    def store(self, context: OutputContext, obj: Any) -> None:
        uri = self._uri(context.task_key)
        obj.write_parquet(uri, storage_options=self.storage_options)
        print(f"[IoManager] Wrote {len(obj)} rows -> {uri}")

    def load(self, context: InputContext) -> Any:
        uri = self._uri(context.upstream_task_key)
        print(f"[IoManager] Reading from {uri}")
        return pl.read_parquet(uri, storage_options=self.storage_options)


staging_io = AdlsParquetIoManager(
    storage_account="mystorageaccount",
    container="datalake",
    base_path="staging",
    # Optionally pass storage_options explicitly:
    # storage_options={"account_name": "...", "account_key": "..."},
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
    cluster="default_cluster",
)
def example_job():
    @task(io_manager=staging_io)
    def extract():
        """Fetch data from a remote API and return a DataFrame."""
        import requests

        url = params["source_url"]
        response = requests.get(url)
        response.raise_for_status()
        return pl.DataFrame(response.json())

    @task(io_manager=staging_io)
    def transform(raw_df):
        """Apply filtering/transformations to the raw data."""
        limit = int(params["limit"])
        return raw_df.head(limit)

    @task
    def load(clean_df):
        """Final consumer – print the result (replace with your own logic)."""
        print(f"Loaded {len(clean_df)} rows:")
        print(clean_df)

    raw = extract()
    clean = transform(raw)
    load(clean)
'''


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


def _cmd_init(args: argparse.Namespace) -> None:
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
        _EXAMPLE_PIPELINE,
    )

    # 4. databricks.yaml
    _write(
        cwd / "databricks.yaml",
        _DATABRICKS_YAML.format(
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


# --- Main ------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="dbxdec",
        description="databricks-bundle-decorators CLI",
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser(
        "init",
        help="Scaffold a new databricks-bundle-decorators pipeline project",
    )

    args = parser.parse_args()

    if args.command == "init":
        _cmd_init(args)
    else:
        parser.print_help()
        sys.exit(1)
