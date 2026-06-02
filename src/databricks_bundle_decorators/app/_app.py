"""Dash application entry point for the observability dashboard.

Discovers jobs from the app's resource bindings via the Databricks SDK,
fetches run data, and renders the dashboard UI (pages, figures, compute
logic).
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any

from databricks_bundle_decorators.app._compute import (
    _backfill_kind,
    build_job_overview,
    compute_backfill_coverage,
)
from databricks_bundle_decorators.app._data import (
    COLOR_IN_PROGRESS,
    BackfillCoverage,
    JobOverview,
    RunInfo,
)
from databricks_bundle_decorators.app._display import (
    _fmt_duration,
)
from databricks_bundle_decorators.app._fetch import (
    fetch_job_runs,
    resolve_job_ids_from_sdk,
    resolve_workspace_url,
)
from databricks_bundle_decorators.app._pages import (
    _FIGURE_BUILDERS,
    _build_coverage_figure,
    _page_backfill_detail,
    _page_backfills,
    _page_overview,
)
from databricks_bundle_decorators.backfill import BackfillDef, _deserialize_backfill_tag


def _load_registry() -> tuple[set[str], dict[str, BackfillDef], dict[str, str]]:
    """Load job names and backfill definitions from ``registry.json``.

    Returns a set of all job names found in the registry, a dict
    mapping job names to their backfill definitions (only for jobs
    that have one), and a dict mapping job names to their Quartz
    schedule cron expressions (only for jobs that have one).
    """
    all_jobs: set[str] = set()
    backfill_defs: dict[str, BackfillDef] = {}
    schedule_crons: dict[str, str] = {}
    registry_path = Path(__file__).resolve().parent.parent.parent / "registry.json"
    if not registry_path.exists():
        # Also check relative to cwd (deployed app layout)
        registry_path = Path("registry.json")
    if registry_path.exists():
        raw = json.loads(registry_path.read_text())
        all_jobs.update(raw.keys())
        for job_name, defn_dict in raw.items():
            if defn_dict is not None:
                cron = defn_dict.pop("schedule_cron", None)
                backfill_defs[job_name] = _deserialize_backfill_tag(defn_dict)
                if cron is not None:
                    schedule_crons[job_name] = cron
    return all_jobs, backfill_defs, schedule_crons


def run_app(  # noqa: PLR0915
    *,
    host: str = "0.0.0.0",  # noqa: S104
    port: int = 8000,
    debug: bool = False,
) -> None:
    """Launch the Dash observability dashboard as a Databricks App.

    The app discovers deployed jobs from ``DBXDEC_JOB_*`` environment
    variables set by the bundle app resource declarations.  Backfill
    definitions are loaded from ``registry.json`` (generated at deploy
    time), so the pipeline package does not need to be installed.

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

    # Load job names and backfill definitions from registry.json
    all_registry_jobs, backfill_defs, schedule_crons = _load_registry()

    # Merge with live registry (if the pipeline package is imported,
    # the registry takes precedence).
    for name, meta in _JOB_REGISTRY.items():
        if meta.backfill is not None:
            backfill_defs[name] = meta.backfill
        schedule = meta.sdk_config.get("schedule")
        cron_expr: str | None = getattr(schedule, "quartz_cron_expression", None)
        if cron_expr is not None:
            schedule_crons[name] = cron_expr

    # Discover job IDs via SDK (app resource bindings)
    job_id_map = resolve_job_ids_from_sdk()
    workspace_url = resolve_workspace_url()

    # Use registry job names if populated, else fall back to
    # registry.json, or SDK resource names.
    if _JOB_REGISTRY:
        job_names = sorted(_JOB_REGISTRY.keys())
    elif all_registry_jobs:
        job_names = sorted(all_registry_jobs)
    elif job_id_map:
        job_names = sorted(job_id_map.keys())
    else:
        print(
            "Error: No jobs found. "
            "Ensure the app has job resource bindings and/or "
            "registry.json is deployed.",
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
        # Re-read job IDs in case of redeployment
        jid_map = resolve_job_ids_from_sdk()
        _cache["job_id_map"] = jid_map
        _cache["workspace_url"] = resolve_workspace_url()
        print(f"[dbxdec] job_id_map: {jid_map}", flush=True)

        all_runs: dict[str, list[RunInfo]] = {}
        overviews: list[JobOverview] = []
        coverages: dict[str, BackfillCoverage] = {}

        for name in job_names:
            bf = backfill_defs.get(name)
            job_id = jid_map.get(name)
            runs = fetch_job_runs(job_id) if job_id else []
            print(
                f"[dbxdec] {name}: job_id={job_id}, runs={len(runs)}",
                flush=True,
            )
            all_runs[name] = runs

            has_bf = bf is not None
            overviews.append(
                build_job_overview(name, job_id, runs, has_backfill=has_bf)
            )

            if bf is not None:
                expected = bf.keys()
                kind = _backfill_kind(bf)
                tz = getattr(bf, "tz", "UTC")
                coverages[name] = compute_backfill_coverage(
                    name,
                    runs,
                    expected,
                    kind=kind,
                    tz=tz,
                    backfill=bf,
                    schedule_cron=schedule_crons.get(name),
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
            dcc.Interval(
                id="auto-refresh-interval",
                interval=3_600_000,  # 1 hour
                n_intervals=0,
            ),
            dcc.Store(id="bg-refresh-ts", data=0),
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

    # --- Background auto-refresh callback ---

    @app.callback(
        Output("bg-refresh-ts", "data"),
        Input("auto-refresh-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def _background_refresh(_n: int) -> float:
        _refresh_data()
        return time.time()

    # --- URL routing callback ---

    @app.callback(
        Output("page-content", "children"),
        Output("workspace-link", "children"),
        Input("url", "pathname"),
        Input("btn-refresh", "n_clicks"),
        Input("bg-refresh-ts", "data"),
    )
    def _display_page(
        pathname: str | None,
        _n_clicks: int | None,
        _refresh_ts: float | None,
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

    # --- Overview KPI recomputation on grid filter ---

    @app.callback(
        Output("kpi-total-jobs", "children"),
        Output("kpi-deployed", "children"),
        Output("kpi-total-runs", "children"),
        Output("kpi-success-rate", "children"),
        Output("kpi-failures", "children"),
        Output("kpi-avg-duration", "children"),
        Input("overview-jobs-grid", "virtualRowData"),
        prevent_initial_call=True,
    )
    def _update_overview_kpis(
        virtual_rows: list[dict[str, Any]] | None,
    ) -> tuple[str, str, str, str, str, str]:
        if not virtual_rows:
            return "0", "0", "0", "0%", "0", "\u2014"

        total_jobs = len(virtual_rows)
        deployed = sum(r.get("_deployed", 0) for r in virtual_rows)
        total_runs = sum(r.get("_total_runs", 0) for r in virtual_rows)
        failures = sum(r.get("_failures", 0) for r in virtual_rows)
        successes = sum(r.get("_successes", 0) for r in virtual_rows)
        terminal = successes + failures
        success_rate = round(successes / terminal * 100, 1) if terminal else 0
        durations = [
            r["_avg_duration_s"]
            for r in virtual_rows
            if r.get("_avg_duration_s") is not None
        ]
        avg_dur_s = round(sum(durations) / len(durations)) if durations else 0
        avg_dur = _fmt_duration(avg_dur_s)

        return (
            str(total_jobs),
            str(deployed),
            str(total_runs),
            f"{success_rate}%",
            str(failures),
            avg_dur,
        )

    # Prefetch data so the first page load is instant
    _refresh_data()

    app.run(host=host, port=port, debug=debug)
