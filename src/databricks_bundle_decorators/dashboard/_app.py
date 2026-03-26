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
    resolve_bundle_targets,
    resolve_job_ids,
    resolve_workspace_url,
)
from databricks_bundle_decorators.dashboard._pages import (
    _FIGURE_BUILDERS,
    _build_coverage_figure,
    _page_backfill_detail,
    _page_backfills,
    _page_overview,
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

    from dash import Input, Output, State, dcc, html

    from databricks_bundle_decorators.registry import _JOB_REGISTRY

    job_names = sorted(_JOB_REGISTRY.keys())
    if not job_names:
        print(
            "Error: No jobs found in registry. "
            "Ensure your pipeline package is imported before run_app().",
            file=sys.stderr,
        )
        sys.exit(1)

    bundle_targets = resolve_bundle_targets()

    # --- Mutable data store ---
    _data: dict[str, Any] = {
        "job_names": job_names,
        "job_id_map": {},
        "all_runs": {},
        "overviews": [],
        "coverages": {},
        "workspace_url": None,
        "active_target": bundle_targets[0] if bundle_targets else None,
    }

    def _refresh_data(target: str | None) -> None:
        job_id_map = resolve_job_ids(target=target)
        _data["job_id_map"] = job_id_map
        _data["workspace_url"] = resolve_workspace_url()
        _data["active_target"] = target

        all_runs: dict[str, list[RunInfo]] = {}
        overviews: list[JobOverview] = []
        coverages: dict[str, BackfillCoverage] = {}

        for name in job_names:
            meta = _JOB_REGISTRY[name]
            job_id = job_id_map.get(name)
            runs = fetch_job_runs(job_id) if job_id else []
            all_runs[name] = runs

            has_bf = meta.backfill is not None
            overviews.append(
                build_job_overview(name, job_id, runs, has_backfill=has_bf)
            )

            if has_bf and meta.backfill is not None:
                expected = meta.backfill.keys()
                kind = _backfill_kind(meta.backfill)
                tz = getattr(meta.backfill, "tz", "UTC")
                coverages[name] = compute_backfill_coverage(
                    name, runs, expected, kind=kind, tz=tz
                )

        _data["all_runs"] = all_runs
        _data["overviews"] = overviews
        _data["coverages"] = coverages

    # --- Build Dash app ---
    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.ZEPHYR],
        suppress_callback_exceptions=True,
    )
    app.title = "Pipeline Observability"

    # Build target dropdown options
    target_options = [{"label": t, "value": t} for t in bundle_targets]
    default_target = bundle_targets[0] if bundle_targets else None

    navbar = dbc.Navbar(
        dbc.Container(
            [
                dbc.NavbarBrand(
                    [
                        html.Span(
                            "\U0001f9f1",
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
                        dbc.NavItem(dbc.NavLink("Backfills", href="/backfills")),
                    ],
                    navbar=True,
                    className="me-auto",
                ),
                dbc.Nav(
                    [
                        dbc.NavItem(
                            dcc.Dropdown(
                                id="input-target",
                                options=target_options,
                                value=default_target,
                                placeholder="Target",
                                clearable=False,
                                searchable=False,
                                style={
                                    "width": "160px",
                                    "color": "#333",
                                },
                            ),
                            className="me-2 d-flex align-items-center",
                        ),
                        dbc.NavItem(
                            html.Span(
                                id="workspace-link",
                            ),
                            className="me-3 d-flex align-items-center",
                        ),
                        dbc.NavItem(
                            dbc.Button(
                                "\u21bb Refresh",
                                id="btn-refresh",
                                color="light",
                                size="sm",
                            )
                        ),
                    ],
                    navbar=True,
                ),
            ],
            fluid=True,
        ),
        color="primary",
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
                        color="#3459e6",
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
        Output("workspace-link", "children"),
        Input("url", "pathname"),
        Input("btn-refresh", "n_clicks"),
        Input("input-target", "value"),
    )
    def _display_page(
        pathname: str | None,
        n_clicks: int | None,
        target: str | None,
    ) -> tuple[Any, Any]:
        target_val = target if target else None

        # Re-fetch when refresh is clicked, target changes, or first load.
        triggered = dash.ctx.triggered_id
        if triggered in ("btn-refresh", "input-target") or not _data["overviews"]:
            _refresh_data(target_val)

        overviews = _data["overviews"]
        coverages = _data["coverages"]
        workspace_url = _data["workspace_url"]
        job_id_map = _data["job_id_map"]

        # Workspace link for the navbar
        if workspace_url:
            # Extract short hostname for display
            ws_label = workspace_url.split("//", 1)[-1].split(".", 1)[0]
            ws_link = html.A(
                f"{ws_label} \u2197",
                href=workspace_url,
                target="_blank",
                rel="noopener noreferrer",
                className="text-light text-decoration-none small",
            )
        else:
            ws_link = html.Span(
                "No workspace",
                className="text-light opacity-50 small",
            )

        if pathname is None or pathname == "/":
            page = _page_overview(overviews, coverages, workspace_url=workspace_url)
        elif pathname == "/backfills":
            page = _page_backfills(coverages)
        elif pathname.startswith("/backfills/"):
            name = pathname[len("/backfills/") :]
            job_id = job_id_map.get(name)
            page = _page_backfill_detail(
                name, coverages, workspace_url=workspace_url, job_id=job_id
            )
        else:
            page = dbc.Alert(
                f"Page not found: {pathname}",
                color="warning",
            )

        return page, ws_link

    # --- Backfill date-range callback ---

    @app.callback(
        Output("bf-graph", "figure"),
        Input("bf-date-range", "start_date"),
        Input("bf-date-range", "end_date"),
        State("bf-job-name", "data"),
        State("bf-kind", "data"),
        prevent_initial_call=True,
    )
    def _update_bf_calendar(
        start_date_str: str | None,
        end_date_str: str | None,
        job_name: str | None,
        kind: str | None,
    ) -> Any:
        from datetime import date as _date

        if not job_name or not kind:
            return dash.no_update
        cov = _data["coverages"].get(job_name)
        if cov is None:
            return dash.no_update
        builder = _FIGURE_BUILDERS.get(kind)
        if builder is None:
            return dash.no_update
        sd = _date.fromisoformat(start_date_str) if start_date_str else None
        ed = _date.fromisoformat(end_date_str) if end_date_str else None
        _, build_fn = builder
        fig = _build_coverage_figure(build_fn, cov, start_date=sd, end_date=ed)
        return fig or {}

    # Prefetch data so the first page load is instant
    _refresh_data(default_target)

    app.run(host=host, port=port, debug=debug)
