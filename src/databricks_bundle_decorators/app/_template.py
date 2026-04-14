"""App template scaffolding for ``dbxdec init-app``.

Provides the ``app.yaml`` and ``app.py`` templates that users
copy into their bundle project's ``./app`` directory.
"""

from __future__ import annotations

#: Template for ``app.yaml`` — the Databricks App runtime config.
#: The ``env`` section and ``command`` are populated by
#: ``generate_app_resource`` in the ``databricks.yml``, so this
#: file just needs the basics.
APP_YAML_TEMPLATE = """\
# Databricks App configuration for the observability dashboard.
# The command and env vars are managed by the bundle resource
# definition (see databricks.yml).  This file provides defaults
# for local development.

command:
  - python
  - app.py
"""

#: Template for ``app.py`` — the app entry point.
APP_PY_TEMPLATE = """\
\"\"\"Pipeline observability Databricks App.

Deploy with ``databricks bundle deploy``.
Run locally with ``python app.py`` (requires DBXDEC_JOB_* env vars).

Requires::

    uv add databricks-bundle-decorators[app]
\"\"\"

import {package_name}.pipelines  # noqa: F401 — populate the job registry

from databricks_bundle_decorators.app import run_app

run_app()
"""

#: Template for ``requirements.txt`` — app dependencies.
REQUIREMENTS_TXT_TEMPLATE = """\
databricks-bundle-decorators[app]
databricks-sdk
{package_name}
"""
