"""App template scaffolding for ``dbxdec init --dashboard``.

Provides the ``app.py`` and ``pyproject.toml`` templates that are
written to the ``./app`` directory.  The ``app.yaml`` is generated
dynamically by `generate_app_yaml` in ``_codegen.py`` so that it
includes ``valueFrom`` env vars for each registered job.
"""

from __future__ import annotations

#: Template for ``app.py`` — the app entry point.
APP_PY_TEMPLATE = """\
\"\"\"Pipeline observability Databricks App.

Deploy with ``databricks bundle deploy``.
Run locally with ``python app.py`` (requires DBXDEC_JOB_* env vars).

Requires::

    uv add databricks-bundle-decorators[app]
\"\"\"

from databricks_bundle_decorators.app import run_app

run_app()
"""

#: Template for ``pyproject.toml`` inside the ``app/`` directory.
#: Databricks Apps uses ``uv`` when ``pyproject.toml`` + ``uv.lock``
#: are present (and ``requirements.txt`` is absent), which allows
#: specifying a Python version >= 3.12.
APP_PYPROJECT_TEMPLATE = """\
[project]
name = "dbxdec-app"
requires-python = ">=3.12"
version = "0.0.0"
dependencies = [
    "databricks-bundle-decorators[app]",
    "databricks-sdk",
]
"""
