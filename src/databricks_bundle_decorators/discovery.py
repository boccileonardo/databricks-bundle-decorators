"""Pipeline discovery via Python entry points.

Pipeline packages register themselves under the ``databricks_bundle_decorators.pipelines``
entry-point group in their ``pyproject.toml``::

    [project.entry-points."databricks_bundle_decorators.pipelines"]
    my_pipeline = "my_pipeline.pipelines"

At deploy time and runtime, pydabs discovers and imports all registered
modules, triggering ``@task`` / ``@job`` / ``@job_cluster`` decorator
registration.

This is the standard Python plugin-discovery pattern (used by pytest,
Flask, Dagster, etc.) and decouples the framework package from concrete
pipeline implementations.
"""

from __future__ import annotations

import importlib.metadata


def discover_pipelines() -> None:
    """Import every module registered under the ``databricks_bundle_decorators.pipelines`` entry-point group."""
    eps = importlib.metadata.entry_points(
        group="databricks_bundle_decorators.pipelines"
    )
    for ep in eps:
        ep.load()  # imports the module, triggering decorator registration
