"""Dash application entry point for the Databricks App dashboard.

Discovers jobs from ``DBXDEC_JOB_*`` environment variables injected
by the bundle app resource, fetches run data via the Databricks SDK,
and reuses the existing dashboard UI components (pages, figures,
compute logic).
"""

from __future__ import annotations

import sys
from typing import Any

from databricks_bundle_decorators.app._fetch import (
    fetch_job_runs,
    resolve_job_ids_from_env,
    resolve_workspace_url,
)
from databricks_bundle_decorators.dashboard._compute import (
    _backfill_kind,
    build_job_overview,
    compute_backfill_coverage,
)
from databricks_bundle_decorators.dashboard._data import (
    COLOR_IN_PROGRESS,
    BackfillCoverage,
    JobOverview,
    RunInfo,
)
from databricks_bundle_decorators.dashboard._pages import (
    _FIGURE_BUILDERS,
    _build_coverage_figure,
    _page_backfill_detail,
    _page_backfills,
    _page_overview,
)

#: Template for the user's ``app.py`` entry point.
APP_TEMPLATE = '''\
"""Pipeline observability Databricks App.

Deploy with ``databricks bundle deploy``.

Requires::

    uv add databricks-bundle-decorators[app]
"""

try:
    import {package_name}.pipelines  # noqa: F401 — populate the job registry
except ImportError:
    pass  # Job discovery falls back to DBXDEC_JOB_* env vars

from databricks_bundle_decorators.app import run_app

run_app()
'''


def run_app(
    *,
    host: str = "0.0.0.0",  # noqa: S104
    port: int = 8050,
    debug: bool = False,
) -> None:
    """Launch the Dash observability dashboard as a Databricks App.

    Import your pipeline package **before** calling this so the
    job registry is populated.  The app discovers deployed jobs
    from ``DBXDEC_JOB_*`` environment variables set by the bundle
    app resource declarations.

    Parameters
    ----------
    host:
        Host to bind the server to.  Defaults to ``0.0.0.0`` for
        container environments.
    port:
        Port number.
    debug:
        Enable Dash debug mode with hot-reloading.
    """
    try:
        import dash  # noqa: PLC0415
        import dash_bootstrap_components as dbc  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "dash and dash-bootstrap-components are required for the "
            "app dashboard. "
            "Install with: uv add databricks-bundle-decorators[app]"
        ) from exc

    from dash import Input, Output, State, dcc, html  # noqa: PLC0415

    from databricks_bundle_decorators.registry import _JOB_REGISTRY  # noqa: PLC0415

    # Discover jobs from env vars
    job_id_map = resolve_job_ids_from_env()
    workspace_url = resolve_workspace_url()

    # Use registry job names if populated, else fall back to env var names
    if _JOB_REGISTRY:
        job_names = sorted(_JOB_REGISTRY.keys())
    elif job_id_map:
        job_names = sorted(job_id_map.keys())
    else:
        print(
            "Error: No jobs found. "
            "Ensure DBXDEC_JOB_* env vars are set and/or your pipeline "
            "package is imported before run_app().",
            file=sys.stderr,
        )
        sys.exit(1)

    # --- Data cache ---
    _cache: dict[str, Any] = {
        "job_id_map": job_id_map,
        "all_runs": {},
        "overviews": [],
        "coverages": {},
        "workspace_url": workspace_url,
    }

    def _refresh_data() -> None:
        # Re-read env vars in case of redeployment
        _cache["job_id_map"] = resolve_job_ids_from_env()
        _cache["workspace_url"] = resolve_workspace_url()
        jid_map = _cache["job_id_map"]

        all_runs: dict[str, list[RunInfo]] = {}
        overviews: list[JobOverview] = []
        coverages: dict[str, BackfillCoverage] = {}

        for name in job_names:
            meta = _JOB_REGISTRY.get(name)
            job_id = jid_map.get(name)
            runs = fetch_job_runs(job_id) if job_id else []
            all_runs[name] = runs

            has_bf = meta is not None and meta.backfill is not None
            overviews.append(
                build_job_overview(name, job_id, runs, has_backfill=has_bf)
            )

            if has_bf and meta is not None and meta.backfill is not None:
                expected = meta.backfill.keys()
                kind = _backfill_kind(meta.backfill)
                tz = getattr(meta.backfill, "tz", "UTC")
                coverages[name] = compute_backfill_coverage(
                    name, runs, expected, kind=kind, tz=tz
                )

        _cache["all_runs"] = all_runs
        _cache["overviews"] = overviews
        _cache["coverages"] = coverages

    # --- Build Dash app ---
    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.ZEPHYR],
        suppress_callback_exceptions=True,
    )
    app.title = "Pipeline Observability"

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
                        color=COLOR_IN_PROGRESS,
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
    )
    def _display_page(
        pathname: str | None,
        _n_clicks: int | None,
    ) -> tuple[Any, Any]:
        triggered = dash.ctx.triggered_id
        if triggered == "btn-refresh" or not _cache["overviews"]:
            _refresh_data()

        overviews = _cache["overviews"]
        coverages = _cache["coverages"]
        ws_url = _cache["workspace_url"]
        jid_map = _cache["job_id_map"]

        # Workspace link for the navbar
        if ws_url:
            ws_label = ws_url.split("//", 1)[-1].split(".", 1)[0]
            ws_link = html.A(
                f"{ws_label} \u2197",
                href=ws_url,
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
            page = _page_overview(overviews, coverages, workspace_url=ws_url)
        elif pathname == "/backfills":
            page = _page_backfills(coverages)
        elif pathname.startswith("/backfills/"):
            name = pathname[len("/backfills/") :]
            job_id = jid_map.get(name)
            page = _page_backfill_detail(
                name, coverages, workspace_url=ws_url, job_id=job_id
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
        from datetime import date as _date  # noqa: PLC0415

        if not job_name or not kind:
            return dash.no_update
        cov: BackfillCoverage | None = _cache.get("coverages", {}).get(job_name)
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
    _refresh_data()

    app.run(host=host, port=port, debug=debug)
