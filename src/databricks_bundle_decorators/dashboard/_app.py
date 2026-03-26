"""Dash application entry point and callbacks.

Contains `run_app`, `APP_TEMPLATE`, the navbar layout, and the
URL-routing callback.
"""

from __future__ import annotations

import sys
from typing import Any

from databricks_bundle_decorators.dashboard._compute import (
    _backfill_kind,
    build_job_overview,
    compute_backfill_coverage,
)
from databricks_bundle_decorators.dashboard._data import (
    BackfillCoverage,
    JobOverview,
    RunInfo,
)
from databricks_bundle_decorators.dashboard._fetch import (
    fetch_job_runs,
    resolve_job_ids,
)
from databricks_bundle_decorators.dashboard._pages import (
    _page_backfills,
    _page_job_detail,
    _page_jobs,
    _page_overview,
    _page_run_detail,
    _page_runs,
)

APP_TEMPLATE = '''\
"""Pipeline observability dashboard.

Launch with::

    python {app_path}

Requires::

    uv add databricks-bundle-decorators[observability]
"""

import {package_name}.pipelines  # noqa: F401 — populate the job registry

from databricks_bundle_decorators.dashboard import run_app

run_app()
'''


def run_app(
    *,
    host: str = "127.0.0.1",
    port: int = 8050,
    debug: bool = False,
) -> None:
    """Launch the Dash observability dashboard.

    Import your pipeline package **before** calling this so the
    job registry is populated.  Requires the ``[observability]``
    optional dependency (``dash``).

    The dashboard is **bundle-scoped** — only jobs deployed from
    the current bundle are shown.  It uses the Databricks CLI for
    data access, inheriting the same unified credentials used for
    ``databricks bundle deploy``.

    Parameters
    ----------
    host:
        Host to bind the server to.
    port:
        Port number.
    debug:
        Enable Dash debug mode with hot-reloading.
    """
    try:
        import dash
        import dash_bootstrap_components as dbc
    except ImportError as exc:
        raise ImportError(
            "dash and dash-bootstrap-components are required for the "
            "observability dashboard. "
            "Install with: uv add databricks-bundle-decorators[observability]"
        ) from exc

    from dash import Input, Output, dcc, html

    from databricks_bundle_decorators.registry import _JOB_REGISTRY

    job_names = sorted(_JOB_REGISTRY.keys())
    if not job_names:
        print(
            "Error: No jobs found in registry. "
            "Ensure your pipeline package is imported before run_app().",
            file=sys.stderr,
        )
        sys.exit(1)

    # --- Mutable data store ---
    _data: dict[str, Any] = {
        "job_names": job_names,
        "job_id_map": {},
        "all_runs": {},
        "overviews": [],
        "coverages": {},
    }

    def _refresh_data(target: str | None, profile: str | None) -> None:
        job_id_map = resolve_job_ids(target=target, profile=profile)
        _data["job_id_map"] = job_id_map

        all_runs: dict[str, list[RunInfo]] = {}
        overviews: list[JobOverview] = []
        coverages: dict[str, BackfillCoverage] = {}

        for name in job_names:
            meta = _JOB_REGISTRY[name]
            job_id = job_id_map.get(name)
            runs = fetch_job_runs(job_id, profile=profile) if job_id else []
            all_runs[name] = runs

            has_bf = meta.backfill is not None
            overviews.append(
                build_job_overview(name, job_id, runs, has_backfill=has_bf)
            )

            if has_bf and meta.backfill is not None:
                expected = meta.backfill.keys()
                kind = _backfill_kind(meta.backfill)
                coverages[name] = compute_backfill_coverage(
                    name, runs, expected, kind=kind
                )

        _data["all_runs"] = all_runs
        _data["overviews"] = overviews
        _data["coverages"] = coverages

    # --- Build Dash app ---
    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.COSMO],
        suppress_callback_exceptions=True,
    )
    app.title = "Pipeline Observability"

    navbar = dbc.Navbar(
        dbc.Container(
            [
                dbc.NavbarBrand(
                    [
                        html.Span(
                            "\u26a1",
                            className="me-2",
                        ),
                        "Pipeline Observability",
                    ],
                    href="/",
                    className="fw-bold",
                ),
                dbc.Nav(
                    [
                        dbc.NavItem(dbc.NavLink("Overview", href="/")),
                        dbc.NavItem(dbc.NavLink("Jobs", href="/jobs")),
                        dbc.NavItem(dbc.NavLink("Runs", href="/runs")),
                        dbc.NavItem(dbc.NavLink("Backfills", href="/backfills")),
                    ],
                    navbar=True,
                    className="me-auto",
                ),
                dbc.Nav(
                    [
                        dbc.NavItem(
                            dbc.Input(
                                id="input-target",
                                placeholder="Target (e.g. dev)",
                                size="sm",
                                className="me-2",
                                style={"width": "140px"},
                            )
                        ),
                        dbc.NavItem(
                            dbc.Input(
                                id="input-profile",
                                placeholder="CLI profile",
                                size="sm",
                                className="me-2",
                                style={"width": "140px"},
                            )
                        ),
                        dbc.NavItem(
                            dbc.Button(
                                "\u21bb Refresh",
                                id="btn-refresh",
                                color="outline-light",
                                size="sm",
                            )
                        ),
                    ],
                    navbar=True,
                ),
            ],
            fluid=True,
        ),
        color="dark",
        dark=True,
        className="mb-4",
    )

    app.layout = html.Div(
        [
            dcc.Location(id="url", refresh=False),
            navbar,
            dbc.Container(
                [
                    dcc.Loading(
                        id="page-loading",
                        children=html.Div(id="page-content"),
                        type="default",
                    ),
                ],
                fluid=True,
                className="pb-4",
            ),
        ]
    )

    # --- URL routing callback ---

    @app.callback(
        Output("page-content", "children"),
        [
            Input("url", "pathname"),
            Input("btn-refresh", "n_clicks"),
        ],
        [
            dash.State("input-target", "value"),
            dash.State("input-profile", "value"),
        ],
    )
    def _display_page(
        pathname: str | None,
        n_clicks: int | None,
        target: str | None,
        profile: str | None,
    ) -> Any:
        target_val = target if target else None
        profile_val = profile if profile else None

        _refresh_data(target_val, profile_val)

        overviews = _data["overviews"]
        all_runs = _data["all_runs"]
        coverages = _data["coverages"]

        if pathname is None or pathname == "/":
            return _page_overview(overviews, coverages)

        if pathname == "/jobs":
            return _page_jobs(overviews)

        if pathname.startswith("/jobs/"):
            name = pathname[len("/jobs/") :]
            runs = all_runs.get(name, [])
            return _page_job_detail(name, overviews, runs, coverages, profile_val)

        if pathname == "/runs":
            return _page_runs(all_runs, job_names)

        if pathname.startswith("/runs/"):
            run_id_str = pathname[len("/runs/") :]
            try:
                run_id = int(run_id_str)
            except ValueError:
                return dbc.Alert(
                    f"Invalid run ID: {run_id_str}",
                    color="warning",
                )
            return _page_run_detail(run_id, all_runs, profile_val)

        if pathname == "/backfills":
            return _page_backfills(coverages)

        return dbc.Alert(
            f"Page not found: {pathname}",
            color="warning",
        )

    app.run(host=host, port=port, debug=debug)
