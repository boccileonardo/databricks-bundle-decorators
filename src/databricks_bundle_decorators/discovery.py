"""Pipeline discovery via Python entry points.

Pipeline packages register themselves under the ``databricks_bundle_decorators.pipelines``
entry-point group in their ``pyproject.toml``::

    [project.entry-points."databricks_bundle_decorators.pipelines"]
    my_pipeline = "my_pipeline.pipelines"

At deploy time and runtime, databricks-bundle-decorators discovers and
imports all registered modules, triggering ``@task`` / ``@job`` /
``@job_cluster`` decorator registration.

This is the standard Python plugin-discovery pattern (used by pytest,
Flask, Dagster, etc.) and decouples the framework package from concrete
pipeline implementations.
"""

from __future__ import annotations

import importlib.metadata
import logging
import sys

_logger = logging.getLogger(__name__)


def discover_pipelines() -> None:
    """Import every module registered under the ``databricks_bundle_decorators.pipelines`` entry-point group."""
    eps = importlib.metadata.entry_points(
        group="databricks_bundle_decorators.pipelines"
    )
    loaded: list[str] = []
    for ep in eps:
        _logger.debug("Loading pipeline entry point: %s (%s)", ep.name, ep.value)
        try:
            ep.load()  # imports the module, triggering decorator registration
        except Exception as exc:
            print(
                f"Error: Failed to load pipeline entry point '{ep.name}' "
                f"({ep.value}): {exc}",
                file=sys.stderr,
            )
            raise
        loaded.append(ep.name)
    if not loaded:
        _logger.debug("No pipeline entry points found.")
