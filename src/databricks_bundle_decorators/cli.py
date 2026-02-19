"""CLI for databricks-bundle-decorators.

Provides scaffolding commands for pipeline repositories.

Usage::

    uv run pydabs init
"""

from __future__ import annotations

import argparse
import sys
import textwrap
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
    from databricks_bundle_decorators.discovery import discover_pipelines
    from databricks_bundle_decorators.codegen import generate_resources

    discover_pipelines()

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

from __future__ import annotations

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
"""Example pipeline – replace with your own tasks and jobs."""

from databricks_bundle_decorators import job, job_cluster, task, params


default_cluster = job_cluster(
    name="default_cluster",
    spark_version="14.3.x-scala2.12",
    node_type_id="Standard_DS3_v2",
    num_workers=2,
)


@task
def hello():
    print(f"Hello from databricks-bundle-decorators! url={{params.get('url', 'N/A')}}")


@job(
    params={{"url": "https://example.com"}},
    cluster="default_cluster",
)
def example_job():
    hello()
'''


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
    _write(cwd / "resources" / "__init__.py", _RESOURCES_INIT)

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

    # --- Check for entry point in pyproject.toml ---------------------------
    entry_points = (
        pyproject.get("project", {})
        .get("entry-points", {})
        .get("databricks_bundle_decorators.pipelines", {})
    )
    if not entry_points:
        print()
        print("Next step: add the pipeline entry point to your pyproject.toml:")
        print()
        print(
            textwrap.dedent(f"""\
            [project.entry-points."databricks_bundle_decorators.pipelines"]
            {package_name} = "{package_name}.pipelines"
            """)
        )

    print("Done! Define your @task and @job functions in the pipelines/ directory.")


# --- Main ------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="pydabs",
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
