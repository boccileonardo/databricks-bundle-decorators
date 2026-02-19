"""Resource loader for ``databricks bundle deploy``.

Referenced from the ``python.resources`` list in ``databricks.yaml``::

    python:
      venv_path: .venv
      resources:
        - 'resources:load_resources'

The Databricks CLI calls :func:`load_resources` during deployment.  It
imports all pipeline definitions (populating the decorator registries)
and then generates ``Job`` resources via :mod:`databricks_bundle_decorators.codegen`.
"""

from databricks.bundles.core import Bundle, Resources


def load_resources(bundle: Bundle) -> Resources:
    """Entry-point called by ``databricks bundle deploy``."""
    # Discover and import pipeline definitions via entry-point group.
    from databricks_bundle_decorators.discovery import discover_pipelines
    from databricks_bundle_decorators.codegen import generate_resources

    discover_pipelines()

    resources = Resources()
    for resource_key, job in generate_resources().items():
        resources.add_resource(resource_key, job)
    return resources
