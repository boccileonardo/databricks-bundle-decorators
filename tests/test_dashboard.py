"""Tests for the observability dashboard (CLI + Dash approach)."""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import whenever

from databricks_bundle_decorators.backfill import (
    DailyBackfill,
    HourlyBackfill,
    MonthlyBackfill,
    StaticBackfill,
    WeeklyBackfill,
)
from databricks_bundle_decorators.dashboard import (
    APP_TEMPLATE,
    BackfillCoverage,
    JobOverview,
    RunInfo,
    build_job_overview,
    compute_backfill_coverage,
    fetch_job_runs,
    resolve_bundle_targets,
    resolve_job_ids,
    resolve_workspace_url,
)
from databricks_bundle_decorators.dashboard._compute import (
    _backfill_kind,
    _effective_state,
    _filter_past_keys,
    _is_active,
    _is_terminal_failure,
)
from databricks_bundle_decorators.dashboard._display import (
    _SQ_COMPLETED,
    _SQ_FAILED,
    _SQ_MISSING,
    _coverages_to_records,
    _fmt_duration,
    _overviews_to_records,
)
from databricks_bundle_decorators.dashboard._figures import (
    _build_daily_calendar,
    _build_hourly_calendar,
    _build_monthly_calendar,
    _build_partition_grid,
    _build_weekly_calendar,
)
from databricks_bundle_decorators.dashboard._pages import _backfill_date_bounds


def _flatten_z(fig: object) -> list[int]:
    """Flatten the z matrix from a Plotly heatmap figure."""
    return [int(v) for row in fig.data[0].z for v in row]  # ty: ignore[unresolved-attribute]


def _flatten_hover(fig: object) -> list[str]:
    """Flatten the hovertext matrix from a Plotly heatmap figure."""
    return [str(v) for row in fig.data[0].hovertext for v in row]  # ty: ignore[unresolved-attribute]


# ---------------------------------------------------------------------------
# Helpers for building mock CLI responses
# ---------------------------------------------------------------------------


def _cli_run(
    *,
    run_id: int = 1,
    result_state: str | None = "SUCCESS",
    life_cycle_state: str | None = None,
    state_message: str | None = None,
    start_time: int = 1_000_000,
    end_time: int = 1_060_000,
    backfill_key: str | None = None,
) -> dict[str, Any]:
    state: dict[str, Any] = {}
    if result_state is not None:
        state["result_state"] = result_state
    if life_cycle_state is not None:
        state["life_cycle_state"] = life_cycle_state
    if state_message is not None:
        state["state_message"] = state_message
    params: list[dict[str, str]] = []
    if backfill_key is not None:
        params.append({"name": "backfill_key", "value": backfill_key})
    return {
        "run_id": run_id,
        "state": state,
        "start_time": start_time,
        "end_time": end_time,
        "job_parameters": params,
    }


def _mock_subprocess(
    stdout: str = "[]",
    returncode: int = 0,
    stderr: str = "",
) -> Any:
    """Return a callable that mocks subprocess.run."""

    def mock_run(cmd: list[str], **kw: Any) -> SimpleNamespace:
        return SimpleNamespace(
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
        )

    return mock_run


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


class TestRunInfo:
    def test_frozen(self) -> None:
        r = RunInfo(
            run_id=1,
            result_state="SUCCESS",
            start_time_ms=1000,
            end_time_ms=2000,
            duration_seconds=1.0,
        )
        with pytest.raises(AttributeError):
            r.run_id = 2  # ty: ignore[invalid-assignment]

    def test_defaults(self) -> None:
        r = RunInfo(
            run_id=1,
            result_state=None,
            start_time_ms=None,
            end_time_ms=None,
            duration_seconds=None,
        )
        assert r.backfill_key is None


class TestBackfillCoverageKind:
    def test_default_kind_is_static(self) -> None:
        cov = BackfillCoverage(
            job_name="j",
            expected_keys=[],
            completed_keys=[],
            missing_keys=[],
            coverage_pct=0.0,
        )
        assert cov.kind == "static"

    def test_kind_preserved(self) -> None:
        cov = BackfillCoverage(
            job_name="j",
            expected_keys=[],
            completed_keys=[],
            missing_keys=[],
            coverage_pct=0.0,
            kind="daily",
        )
        assert cov.kind == "daily"


# ---------------------------------------------------------------------------
# _effective_state / _is_terminal_failure
# ---------------------------------------------------------------------------


class TestEffectiveState:
    def test_prefers_result_state(self) -> None:
        assert _effective_state("SUCCESS", "TERMINATED") == "SUCCESS"

    def test_falls_back_to_life_cycle_state(self) -> None:
        assert _effective_state(None, "INTERNAL_ERROR") == "INTERNAL_ERROR"

    def test_both_none(self) -> None:
        assert _effective_state(None, None) == "UNKNOWN"

    def test_running_lifecycle(self) -> None:
        assert _effective_state(None, "RUNNING") == "RUNNING"


class TestIsTerminalFailure:
    def test_success_is_not_failure(self) -> None:
        assert _is_terminal_failure("SUCCESS", None) is False

    def test_failed_result_state(self) -> None:
        assert _is_terminal_failure("FAILED", None) is True

    def test_timed_out_result_state(self) -> None:
        assert _is_terminal_failure("TIMED_OUT", None) is True

    def test_internal_error_lifecycle(self) -> None:
        assert _is_terminal_failure(None, "INTERNAL_ERROR") is True

    def test_skipped_lifecycle(self) -> None:
        assert _is_terminal_failure(None, "SKIPPED") is True

    def test_running_lifecycle_not_failure(self) -> None:
        assert _is_terminal_failure(None, "RUNNING") is False

    def test_both_none_not_failure(self) -> None:
        assert _is_terminal_failure(None, None) is False


class TestIsActive:
    def test_running_is_active(self) -> None:
        assert _is_active(None, "RUNNING") is True

    def test_pending_is_active(self) -> None:
        assert _is_active(None, "PENDING") is True

    def test_terminating_is_active(self) -> None:
        assert _is_active(None, "TERMINATING") is True

    def test_success_is_not_active(self) -> None:
        assert _is_active("SUCCESS", "TERMINATED") is False

    def test_failed_is_not_active(self) -> None:
        assert _is_active("FAILED", None) is False

    def test_internal_error_is_not_active(self) -> None:
        assert _is_active(None, "INTERNAL_ERROR") is False

    def test_none_none_is_not_active(self) -> None:
        assert _is_active(None, None) is False


# ---------------------------------------------------------------------------
# build_job_overview (pure function)
# ---------------------------------------------------------------------------


class TestBuildJobOverview:
    def test_empty_runs(self) -> None:
        o = build_job_overview("job1", job_id=None, runs=[])
        assert o.job_name == "job1"
        assert o.total_runs == 0
        assert o.successes == 0
        assert o.failures == 0
        assert o.avg_duration_seconds is None

    def test_all_success(self) -> None:
        runs = [
            RunInfo(1, "SUCCESS", 1000, 61000, 60.0),
            RunInfo(2, "SUCCESS", 2000, 32000, 30.0),
        ]
        o = build_job_overview("j", job_id=42, runs=runs)
        assert o.total_runs == 2
        assert o.successes == 2
        assert o.failures == 0
        assert o.avg_duration_seconds == 45.0
        # most_recent is the run with the highest start_time_ms
        assert o.last_run_time_ms == 2000
        assert o.last_run_state == "SUCCESS"
        assert o.job_id == 42

    def test_mixed_states(self) -> None:
        runs = [
            RunInfo(1, "FAILED", 3000, 63000, 60.0),
            RunInfo(2, "SUCCESS", 2000, 32000, 30.0),
            RunInfo(3, "SUCCESS", 1000, 31000, 30.0),
        ]
        o = build_job_overview("j", job_id=1, runs=runs)
        assert o.successes == 2
        assert o.failures == 1
        assert o.last_run_state == "FAILED"

    def test_running_run_excluded_from_failures(self) -> None:
        """A run with result_state=None (still running) is not a failure."""
        runs = [
            RunInfo(1, None, 3000, None, None, life_cycle_state="RUNNING"),
            RunInfo(2, "SUCCESS", 2000, 32000, 30.0),
        ]
        o = build_job_overview("j", job_id=1, runs=runs)
        assert o.total_runs == 2
        assert o.successes == 1
        assert o.failures == 0
        assert o.in_progress == 1

    def test_internal_error_counted_as_failure(self) -> None:
        """An INTERNAL_ERROR run (no result_state) is counted as a failure."""
        runs = [
            RunInfo(
                1,
                None,
                3000,
                4000,
                1.0,
                life_cycle_state="INTERNAL_ERROR",
                state_message="Cluster launch failed",
            ),
            RunInfo(2, "SUCCESS", 2000, 32000, 30.0),
        ]
        o = build_job_overview("j", job_id=1, runs=runs)
        assert o.failures == 1
        assert o.last_run_state == "INTERNAL_ERROR"

    def test_skipped_run_counted_as_failure(self) -> None:
        """A SKIPPED run (no result_state) is counted as a failure."""
        runs = [
            RunInfo(1, None, 3000, 4000, 1.0, life_cycle_state="SKIPPED"),
        ]
        o = build_job_overview("j", job_id=1, runs=runs)
        assert o.failures == 1
        assert o.last_run_state == "SKIPPED"

    def test_has_backfill_flag(self) -> None:
        o = build_job_overview("j", job_id=1, runs=[], has_backfill=True)
        assert o.has_backfill is True

    def test_none_durations_excluded_from_avg(self) -> None:
        runs = [
            RunInfo(1, "SUCCESS", 1000, 31000, 30.0),
            RunInfo(2, "SUCCESS", 2000, None, None),
        ]
        o = build_job_overview("j", job_id=1, runs=runs)
        assert o.avg_duration_seconds == 30.0


# ---------------------------------------------------------------------------
# compute_backfill_coverage (pure function)
# ---------------------------------------------------------------------------


class TestComputeBackfillCoverage:
    def test_full_coverage(self) -> None:
        runs = [
            RunInfo(1, "SUCCESS", 0, 0, 0, backfill_key="2024-01-01"),
            RunInfo(2, "SUCCESS", 0, 0, 0, backfill_key="2024-01-02"),
        ]
        cov = compute_backfill_coverage("j", runs, ["2024-01-01", "2024-01-02"])
        assert cov.coverage_pct == 100.0
        assert cov.missing_keys == []
        assert len(cov.completed_keys) == 2

    def test_partial_coverage(self) -> None:
        runs = [
            RunInfo(1, "SUCCESS", 0, 0, 0, backfill_key="2024-01-01"),
        ]
        cov = compute_backfill_coverage("j", runs, ["2024-01-01", "2024-01-02"])
        assert cov.coverage_pct == 50.0
        assert cov.missing_keys == ["2024-01-02"]
        assert cov.completed_keys == ["2024-01-01"]

    def test_no_coverage(self) -> None:
        cov = compute_backfill_coverage("j", [], ["2024-01-01", "2024-01-02"])
        assert cov.coverage_pct == 0.0
        assert len(cov.missing_keys) == 2

    def test_empty_expected(self) -> None:
        cov = compute_backfill_coverage("j", [], [])
        assert cov.coverage_pct == 0.0
        assert cov.missing_keys == []
        assert cov.completed_keys == []

    def test_ignores_failed_runs(self) -> None:
        runs = [
            RunInfo(1, "FAILED", 0, 0, 0, backfill_key="2024-01-01"),
        ]
        cov = compute_backfill_coverage("j", runs, ["2024-01-01"])
        assert cov.coverage_pct == 0.0
        assert cov.missing_keys == ["2024-01-01"]
        assert cov.errored_keys == ["2024-01-01"]

    def test_errored_keys_not_in_successful(self) -> None:
        """A key with both a failed and successful run is not errored."""
        runs = [
            RunInfo(1, "FAILED", 0, 0, 0, backfill_key="2024-01-01"),
            RunInfo(2, "SUCCESS", 1000, 2000, 1.0, backfill_key="2024-01-01"),
        ]
        cov = compute_backfill_coverage("j", runs, ["2024-01-01"])
        assert cov.coverage_pct == 100.0
        assert cov.errored_keys == []

    def test_ignores_runs_without_key(self) -> None:
        runs = [
            RunInfo(1, "SUCCESS", 0, 0, 0, backfill_key=None),
        ]
        cov = compute_backfill_coverage("j", runs, ["2024-01-01"])
        assert cov.coverage_pct == 0.0

    def test_extra_keys_in_runs_ignored(self) -> None:
        """Runs with keys not in expected_keys don't affect coverage."""
        runs = [
            RunInfo(1, "SUCCESS", 0, 0, 0, backfill_key="2024-01-01"),
            RunInfo(2, "SUCCESS", 0, 0, 0, backfill_key="2024-01-99"),
        ]
        cov = compute_backfill_coverage("j", runs, ["2024-01-01"])
        assert cov.coverage_pct == 100.0
        assert len(cov.completed_keys) == 1

    def test_job_name_propagated(self) -> None:
        cov = compute_backfill_coverage("my_job", [], ["k1"])
        assert cov.job_name == "my_job"

    def test_kind_propagated(self) -> None:
        cov = compute_backfill_coverage("j", [], ["k1"], kind="daily")
        assert cov.kind == "daily"

    def test_default_kind_is_static(self) -> None:
        cov = compute_backfill_coverage("j", [], ["k1"])
        assert cov.kind == "static"

    def test_daily_future_keys_excluded(self) -> None:
        """Daily keys in the future are not counted as missing."""
        runs = [
            RunInfo(1, "SUCCESS", 0, 0, 0, backfill_key="2024-01-01"),
        ]
        cov = compute_backfill_coverage(
            "j", runs, ["2024-01-01", "2099-12-31"], kind="daily"
        )
        assert cov.coverage_pct == 100.0
        assert cov.missing_keys == []
        assert "2099-12-31" not in cov.missing_keys

    def test_completed_key_runs_tracks_run_info(self) -> None:
        """completed_key_runs maps keys to (run_id, start_time_ms)."""
        runs = [
            RunInfo(10, "SUCCESS", 1_700_000_000_000, 0, 0, backfill_key="k1"),
        ]
        cov = compute_backfill_coverage("j", runs, ["k1"])
        assert cov.completed_key_runs is not None
        assert "k1" in cov.completed_key_runs
        run_id, start_ms = cov.completed_key_runs["k1"]
        assert run_id == 10
        assert start_ms == 1_700_000_000_000

    def test_completed_key_runs_keeps_most_recent(self) -> None:
        """When multiple runs target the same key, keep the most recent."""
        runs = [
            RunInfo(1, "SUCCESS", 1_000_000, 0, 0, backfill_key="k1"),
            RunInfo(2, "SUCCESS", 2_000_000, 0, 0, backfill_key="k1"),
            RunInfo(3, "SUCCESS", 1_500_000, 0, 0, backfill_key="k1"),
        ]
        cov = compute_backfill_coverage("j", runs, ["k1"])
        assert cov.completed_key_runs is not None
        assert cov.completed_key_runs["k1"] == (2, 2_000_000)

    def test_completed_key_runs_excludes_out_of_scope_keys(self) -> None:
        """Keys not in expected_keys are excluded from completed_key_runs."""
        runs = [
            RunInfo(1, "SUCCESS", 0, 0, 0, backfill_key="k1"),
            RunInfo(2, "SUCCESS", 0, 0, 0, backfill_key="extra"),
        ]
        cov = compute_backfill_coverage("j", runs, ["k1"])
        assert cov.completed_key_runs is not None
        assert "k1" in cov.completed_key_runs
        assert "extra" not in cov.completed_key_runs

    def test_completed_key_runs_empty_when_no_runs(self) -> None:
        cov = compute_backfill_coverage("j", [], ["k1"])
        assert cov.completed_key_runs == {}


# ---------------------------------------------------------------------------
# _filter_past_keys
# ---------------------------------------------------------------------------


class TestFilterPastKeys:
    def test_static_returns_all(self) -> None:
        keys = ["us", "eu", "jp"]
        assert _filter_past_keys(keys, "static") == keys

    def test_daily_excludes_future(self) -> None:
        past = ["2024-01-01", "2024-06-15"]
        future = ["2099-01-01", "2099-12-31"]
        result = _filter_past_keys(past + future, "daily")
        assert result == past

    def test_daily_includes_today(self) -> None:
        today = whenever.ZonedDateTime.now("UTC").date()
        key = today.to_stdlib().isoformat()
        result = _filter_past_keys([key], "daily")
        assert key in result

    def test_daily_invalid_key_kept(self) -> None:
        result = _filter_past_keys(["not-a-date", "2024-01-01"], "daily")
        assert "not-a-date" in result
        assert "2024-01-01" in result

    def test_weekly_excludes_future(self) -> None:
        result = _filter_past_keys(["2024-W01", "2099-W50"], "weekly")
        assert "2024-W01" in result
        assert "2099-W50" not in result

    def test_weekly_includes_current_week(self) -> None:
        today = whenever.ZonedDateTime.now("UTC").date()
        iwd = today.iso_week_date()
        key = f"{iwd.year}-W{iwd.week:02d}"
        result = _filter_past_keys([key], "weekly")
        assert key in result

    def test_monthly_excludes_future(self) -> None:
        result = _filter_past_keys(["2024-01-01", "2099-12-01"], "monthly")
        assert "2024-01-01" in result
        assert "2099-12-01" not in result

    def test_monthly_includes_current_month(self) -> None:
        today = whenever.ZonedDateTime.now("UTC").date()
        first = today.replace(day=1).to_stdlib().isoformat()
        result = _filter_past_keys([first], "monthly")
        assert first in result

    def test_hourly_excludes_future(self) -> None:
        result = _filter_past_keys(["2024-01-01T10", "2099-01-01T00"], "hourly")
        assert "2024-01-01T10" in result
        assert "2099-01-01T00" not in result

    def test_hourly_includes_current_hour(self) -> None:
        now = whenever.ZonedDateTime.now("UTC")
        key = now.replace(minute=0, second=0, nanosecond=0).format("YYYY-MM-DD'T'hh")
        result = _filter_past_keys([key], "hourly")
        assert key in result

    def test_unknown_kind_returns_all(self) -> None:
        keys = ["a", "b"]
        assert _filter_past_keys(keys, "unknown") == keys


# ---------------------------------------------------------------------------
# fetch_job_runs (mocks subprocess — CLI-based)
# ---------------------------------------------------------------------------


class TestFetchJobRuns:
    def test_basic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runs_json = json.dumps([_cli_run(run_id=10, result_state="SUCCESS")])
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(stdout=runs_json),
        )
        runs = fetch_job_runs(42)
        assert len(runs) == 1
        assert runs[0].run_id == 10
        assert runs[0].result_state == "SUCCESS"

    def test_computes_duration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runs_json = json.dumps([_cli_run(start_time=1_000_000, end_time=1_060_000)])
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(stdout=runs_json),
        )
        runs = fetch_job_runs(1)
        assert runs[0].duration_seconds == 60.0

    def test_extracts_backfill_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runs_json = json.dumps([_cli_run(backfill_key="2024-01-15")])
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(stdout=runs_json),
        )
        runs = fetch_job_runs(1)
        assert runs[0].backfill_key == "2024-01-15"

    def test_handles_no_backfill_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runs_json = json.dumps([_cli_run()])
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(stdout=runs_json),
        )
        runs = fetch_job_runs(1)
        assert runs[0].backfill_key is None

    def test_handles_running_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runs_json = json.dumps([_cli_run(result_state=None, end_time=0)])
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(stdout=runs_json),
        )
        runs = fetch_job_runs(1)
        assert runs[0].result_state is None

    def test_empty_runs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(stdout="[]"),
        )
        runs = fetch_job_runs(1)
        assert runs == []

    def test_returns_empty_on_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(returncode=1, stderr="error"),
        )
        runs = fetch_job_runs(1)
        assert runs == []

    def test_passes_profile(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured_cmd: list[str] = []

        def mock_run(cmd: list[str], **kw: Any) -> SimpleNamespace:
            captured_cmd.extend(cmd)
            return SimpleNamespace(returncode=0, stdout="[]", stderr="")

        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            mock_run,
        )
        fetch_job_runs(42, profile="work")
        assert "--profile" in captured_cmd
        assert "work" in captured_cmd
        assert "--job-id" in captured_cmd
        assert "42" in captured_cmd

    def test_parses_life_cycle_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runs_json = json.dumps(
            [
                _cli_run(
                    result_state=None,
                    life_cycle_state="INTERNAL_ERROR",
                    state_message="Cluster failed",
                ),
            ]
        )
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(stdout=runs_json),
        )
        runs = fetch_job_runs(1)
        assert runs[0].life_cycle_state == "INTERNAL_ERROR"
        assert runs[0].state_message == "Cluster failed"

    def test_empty_state_message_becomes_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runs_json = json.dumps(
            [
                _cli_run(result_state="SUCCESS", state_message=""),
            ]
        )
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(stdout=runs_json),
        )
        runs = fetch_job_runs(1)
        assert runs[0].state_message is None


# ---------------------------------------------------------------------------
# resolve_bundle_targets (reads databricks.yaml)
# ---------------------------------------------------------------------------


class TestResolveBundleTargets:
    def test_parses_targets(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        yaml_content = """bundle:\n  name: my_project\n\ntargets:\n  dev:\n    mode: development\n  staging:\n    mode: development\n  prod:\n    mode: production\n"""
        (tmp_path / "databricks.yaml").write_text(yaml_content)
        monkeypatch.chdir(tmp_path)
        assert resolve_bundle_targets() == ["dev", "staging", "prod"]

    def test_no_yaml_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        assert resolve_bundle_targets() == []

    def test_no_targets_section(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "databricks.yaml").write_text("bundle:\n  name: x\n")
        monkeypatch.chdir(tmp_path)
        assert resolve_bundle_targets() == []

    def test_yml_extension(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        yaml_content = "bundle:\n  name: x\n\ntargets:\n  dev:\n    mode: development\n"
        (tmp_path / "databricks.yml").write_text(yaml_content)
        monkeypatch.chdir(tmp_path)
        assert resolve_bundle_targets() == ["dev"]

    def test_prefers_yaml_over_yml(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "databricks.yaml").write_text(
            "bundle:\n  name: x\ntargets:\n  alpha:\n    mode: dev\n"
        )
        (tmp_path / "databricks.yml").write_text(
            "bundle:\n  name: x\ntargets:\n  beta:\n    mode: dev\n"
        )
        monkeypatch.chdir(tmp_path)
        assert resolve_bundle_targets() == ["alpha"]


# ---------------------------------------------------------------------------
# resolve_job_ids (mocks subprocess)
# ---------------------------------------------------------------------------


class TestResolveJobIds:
    def test_parses_bundle_summary(self, monkeypatch: pytest.MonkeyPatch) -> None:
        summary = {
            "resources": {
                "jobs": {
                    "etl_job": {"id": "12345"},
                    "ml_job": {"id": "67890"},
                }
            }
        }
        monkeypatch.setattr("shutil.which", lambda cmd: "/usr/bin/databricks")
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            lambda cmd, **kw: SimpleNamespace(
                returncode=0, stdout=json.dumps(summary), stderr=""
            ),
        )
        result = resolve_job_ids()
        assert result == {"etl_job": 12345, "ml_job": 67890}

    def test_returns_empty_when_cli_missing(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr("shutil.which", lambda cmd: None)
        result = resolve_job_ids()
        assert result == {}
        assert "not found" in capsys.readouterr().err

    def test_returns_empty_on_command_failure(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr("shutil.which", lambda cmd: "/usr/bin/databricks")
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            lambda cmd, **kw: SimpleNamespace(
                returncode=1, stdout="", stderr="bundle not found"
            ),
        )
        result = resolve_job_ids()
        assert result == {}
        assert "failed" in capsys.readouterr().err

    def test_passes_target_and_profile(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured_cmd: list[str] = []

        def mock_run(cmd: list[str], **kw: Any) -> SimpleNamespace:
            captured_cmd.extend(cmd)
            return SimpleNamespace(
                returncode=0,
                stdout=json.dumps({"resources": {"jobs": {}}}),
                stderr="",
            )

        monkeypatch.setattr("shutil.which", lambda cmd: "/usr/bin/databricks")
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            mock_run,
        )
        resolve_job_ids(target="prod", profile="work")
        assert "--target" in captured_cmd
        assert "prod" in captured_cmd
        assert "--profile" in captured_cmd
        assert "work" in captured_cmd

    def test_skips_jobs_without_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        summary = {
            "resources": {
                "jobs": {
                    "deployed": {"id": "111"},
                    "not_deployed": {},
                }
            }
        }
        monkeypatch.setattr("shutil.which", lambda cmd: "/usr/bin/databricks")
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            lambda cmd, **kw: SimpleNamespace(
                returncode=0, stdout=json.dumps(summary), stderr=""
            ),
        )
        result = resolve_job_ids()
        assert result == {"deployed": 111}


# ---------------------------------------------------------------------------
# resolve_workspace_url (mocks subprocess)
# ---------------------------------------------------------------------------


class TestResolveWorkspaceUrl:
    def test_returns_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        auth_data = {"host": "https://my-workspace.databricks.com/"}
        monkeypatch.setattr("shutil.which", lambda cmd: "/usr/bin/databricks")
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(stdout=json.dumps(auth_data)),
        )
        result = resolve_workspace_url()
        assert result == "https://my-workspace.databricks.com"

    def test_strips_trailing_slash(self, monkeypatch: pytest.MonkeyPatch) -> None:
        auth_data = {"host": "https://example.databricks.com///"}
        monkeypatch.setattr("shutil.which", lambda cmd: "/usr/bin/databricks")
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(stdout=json.dumps(auth_data)),
        )
        result = resolve_workspace_url()
        assert result == "https://example.databricks.com"

    def test_returns_none_when_cli_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("shutil.which", lambda cmd: None)
        assert resolve_workspace_url() is None

    def test_returns_none_on_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("shutil.which", lambda cmd: "/usr/bin/databricks")
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(returncode=1, stderr="error"),
        )
        assert resolve_workspace_url() is None

    def test_returns_none_on_bad_json(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("shutil.which", lambda cmd: "/usr/bin/databricks")
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(stdout="not json"),
        )
        assert resolve_workspace_url() is None

    def test_returns_none_when_no_host_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("shutil.which", lambda cmd: "/usr/bin/databricks")
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(stdout=json.dumps({"user": "test"})),
        )
        assert resolve_workspace_url() is None

    def test_returns_host_from_nested_details(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        auth_data = {
            "status": "success",
            "details": {"host": "https://nested.databricks.com/"},
        }
        monkeypatch.setattr("shutil.which", lambda cmd: "/usr/bin/databricks")
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            _mock_subprocess(stdout=json.dumps(auth_data)),
        )
        result = resolve_workspace_url()
        assert result == "https://nested.databricks.com"

    def test_passes_profile(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured_cmd: list[str] = []

        def mock_run(cmd: list[str], **kw: Any) -> SimpleNamespace:
            captured_cmd.extend(cmd)
            return SimpleNamespace(
                returncode=0,
                stdout=json.dumps({"host": "https://ws.databricks.com"}),
                stderr="",
            )

        monkeypatch.setattr("shutil.which", lambda cmd: "/usr/bin/databricks")
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard._fetch.subprocess.run",
            mock_run,
        )
        resolve_workspace_url(profile="work")
        assert "--profile" in captured_cmd
        assert "work" in captured_cmd


# ---------------------------------------------------------------------------
# _backfill_kind
# ---------------------------------------------------------------------------


class TestBackfillKind:
    def test_daily(self) -> None:
        bf = DailyBackfill(start_date="2024-01-01", end_date="2024-01-31")
        assert _backfill_kind(bf) == "daily"

    def test_weekly(self) -> None:
        bf = WeeklyBackfill(start_date="2024-W01", end_date="2024-W04")
        assert _backfill_kind(bf) == "weekly"

    def test_monthly(self) -> None:
        bf = MonthlyBackfill(start_date="2024-01-01", end_date="2024-03-01")
        assert _backfill_kind(bf) == "monthly"

    def test_hourly(self) -> None:
        bf = HourlyBackfill(start_date="2024-01-01T00", end_date="2024-01-01T23")
        assert _backfill_kind(bf) == "hourly"

    def test_static(self) -> None:
        bf = StaticBackfill(keys=["us", "eu"])
        assert _backfill_kind(bf) == "static"

    def test_unknown_returns_static(self) -> None:
        assert _backfill_kind("unknown") == "static"


# ---------------------------------------------------------------------------
# _build_daily_calendar (Plotly figure)
# ---------------------------------------------------------------------------


class TestBuildDailyCalendar:
    def test_empty_keys(self) -> None:
        assert _build_daily_calendar(set(), set()) is None

    def test_invalid_keys_returns_none(self) -> None:
        assert _build_daily_calendar({"not-a-date"}, set()) is None

    def test_returns_figure(self) -> None:
        fig = _build_daily_calendar({"2024-01-15"}, set())
        assert fig is not None
        assert fig.data[0].type == "heatmap"

    def test_completed_day_in_z(self) -> None:
        fig = _build_daily_calendar({"2024-01-15"}, {"2024-01-15"})
        assert 2 in _flatten_z(fig)  # 2 = completed

    def test_missing_day_in_z(self) -> None:
        fig = _build_daily_calendar({"2024-01-15"}, set())
        assert 1 in _flatten_z(fig)  # 1 = missing

    def test_hover_contains_date(self) -> None:
        fig = _build_daily_calendar({"2024-01-15"}, set())
        hover = _flatten_hover(fig)
        assert any("2024-01-15" in h for h in hover)

    def test_hover_shows_completed_status(self) -> None:
        fig = _build_daily_calendar({"2024-01-15"}, {"2024-01-15"})
        hover = _flatten_hover(fig)
        assert any("Completed" in h for h in hover)

    def test_hover_shows_missing_status(self) -> None:
        fig = _build_daily_calendar({"2024-01-15"}, set())
        hover = _flatten_hover(fig)
        assert any("Missing" in h for h in hover)

    def test_seven_weekday_rows(self) -> None:
        fig = _build_daily_calendar({"2024-01-15"}, set())
        assert len(fig.data[0].z) == 7

    def test_hover_shows_run_info(self) -> None:
        key_run_info = {"2024-01-15": (42, 1_705_312_800_000)}
        fig = _build_daily_calendar({"2024-01-15"}, {"2024-01-15"}, key_run_info)  # ty: ignore[invalid-argument-type]
        hover = _flatten_hover(fig)
        assert any("Run 42" in h for h in hover)
        assert any("2024" in h for h in hover)

    def test_hover_without_run_info_shows_completed(self) -> None:
        fig = _build_daily_calendar({"2024-01-15"}, {"2024-01-15"})
        hover = _flatten_hover(fig)
        assert any(h == "2024-01-15: Completed" for h in hover)

    def test_date_range_filters_days(self) -> None:
        keys = {f"2024-01-{d:02d}" for d in range(1, 31)}
        fig = _build_daily_calendar(
            keys, set(), start_date=date(2024, 1, 10), end_date=date(2024, 1, 20)
        )
        hover = _flatten_hover(fig)
        assert any("2024-01-15" in h for h in hover)
        assert not any("2024-01-05" in h for h in hover)

    def test_auto_clips_large_range(self) -> None:
        # >180 unique dates triggers auto-clip to last 90
        base = date(2023, 1, 1)
        keys = {(base + timedelta(days=i)).isoformat() for i in range(200)}
        fig = _build_daily_calendar(keys, set())
        hover = _flatten_hover(fig)
        # Last date should be present, early dates should be filtered
        assert any("2023-07-19" in h for h in hover)  # day 200
        assert not any("2023-01-01: Missing" in h for h in hover)

    def test_explicit_range_overrides_auto_clip(self) -> None:
        keys = {f"2024-{m:02d}-{d:02d}" for m in range(1, 13) for d in (1, 15)}
        fig = _build_daily_calendar(
            keys, set(), start_date=date(2024, 1, 1), end_date=date(2024, 12, 31)
        )
        hover = _flatten_hover(fig)
        assert any("2024-01-01" in h for h in hover)
        assert any("2024-12-15" in h for h in hover)

    def test_empty_range_returns_none(self) -> None:
        fig = _build_daily_calendar(
            {"2024-01-15"},
            set(),
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
        )
        assert fig is None

    def test_errored_day_in_z(self) -> None:
        fig = _build_daily_calendar({"2024-01-15"}, set(), errored_keys={"2024-01-15"})
        assert 5 in _flatten_z(fig)  # 5 = failed

    def test_errored_hover_shows_failed(self) -> None:
        fig = _build_daily_calendar({"2024-01-15"}, set(), errored_keys={"2024-01-15"})
        hover = _flatten_hover(fig)
        assert any("Failed" in h for h in hover)

    def test_in_progress_day_in_z(self) -> None:
        fig = _build_daily_calendar(
            {"2024-01-15"}, set(), in_progress_keys={"2024-01-15"}
        )
        assert 4 in _flatten_z(fig)  # 4 = in progress

    def test_in_progress_hover(self) -> None:
        fig = _build_daily_calendar(
            {"2024-01-15"}, set(), in_progress_keys={"2024-01-15"}
        )
        hover = _flatten_hover(fig)
        assert any("In progress" in h for h in hover)


# ---------------------------------------------------------------------------
# _build_weekly_calendar (Plotly figure)
# ---------------------------------------------------------------------------


class TestBuildWeeklyCalendar:
    def test_empty_keys(self) -> None:
        assert _build_weekly_calendar(set(), set()) is None

    def test_invalid_keys_returns_none(self) -> None:
        assert _build_weekly_calendar({"not-a-week"}, set()) is None

    def test_returns_figure(self) -> None:
        fig = _build_weekly_calendar({"2024-W03"}, set())
        assert fig is not None
        assert fig.data[0].type == "heatmap"

    def test_completed_week_in_z(self) -> None:
        fig = _build_weekly_calendar({"2024-W03"}, {"2024-W03"})
        assert 2 in _flatten_z(fig)

    def test_missing_week_in_z(self) -> None:
        fig = _build_weekly_calendar({"2024-W03"}, set())
        assert 1 in _flatten_z(fig)

    def test_hover_contains_key(self) -> None:
        fig = _build_weekly_calendar({"2024-W03"}, set())
        hover = _flatten_hover(fig)
        assert any("2024-W03" in h for h in hover)

    def test_spans_multiple_years(self) -> None:
        fig = _build_weekly_calendar({"2023-W50", "2024-W03"}, set())
        y_labels = list(fig.data[0].y)
        assert "2023" in y_labels
        assert "2024" in y_labels

    def test_date_range_filters_weeks(self) -> None:
        keys = {f"2024-W{w:02d}" for w in range(1, 53)}
        fig = _build_weekly_calendar(
            keys, set(), start_date=date(2024, 6, 1), end_date=date(2024, 9, 30)
        )
        hover = _flatten_hover(fig)
        # W26 (late June) should be present as "Missing"
        assert any("2024-W26: Missing" in h for h in hover)
        # W01 (early January) should NOT appear as "Missing" (it was filtered)
        assert not any("2024-W01: Missing" in h for h in hover)

    def test_auto_clips_large_range(self) -> None:
        # >104 weeks triggers auto-clip to last 52
        keys = {f"{y}-W{w:02d}" for y in range(2020, 2024) for w in range(1, 53)}
        fig = _build_weekly_calendar(keys, set())
        hover = _flatten_hover(fig)
        assert any("2023-W52: Missing" in h for h in hover)
        assert not any("2020-W01: Missing" in h for h in hover)

    def test_empty_range_returns_none(self) -> None:
        fig = _build_weekly_calendar(
            {"2024-W03"},
            set(),
            start_date=date(2025, 1, 1),
            end_date=date(2025, 12, 31),
        )
        assert fig is None

    def test_errored_week_in_z(self) -> None:
        fig = _build_weekly_calendar({"2024-W03"}, set(), errored_keys={"2024-W03"})
        assert 5 in _flatten_z(fig)

    def test_errored_hover_shows_failed(self) -> None:
        fig = _build_weekly_calendar({"2024-W03"}, set(), errored_keys={"2024-W03"})
        hover = _flatten_hover(fig)
        assert any("Failed" in h for h in hover)


# ---------------------------------------------------------------------------
# _build_monthly_calendar (Plotly figure)
# ---------------------------------------------------------------------------


class TestBuildMonthlyCalendar:
    def test_empty_keys(self) -> None:
        assert _build_monthly_calendar(set(), set()) is None

    def test_invalid_keys_returns_none(self) -> None:
        assert _build_monthly_calendar({"bad"}, set()) is None

    def test_returns_figure(self) -> None:
        fig = _build_monthly_calendar({"2024-01-01"}, set())
        assert fig is not None
        assert fig.data[0].type == "heatmap"

    def test_has_twelve_month_columns(self) -> None:
        fig = _build_monthly_calendar({"2024-06-01"}, set())
        assert len(fig.data[0].x) == 12

    def test_completed_month_in_z(self) -> None:
        fig = _build_monthly_calendar({"2024-03-01"}, {"2024-03-01"})
        assert 2 in _flatten_z(fig)

    def test_missing_month_in_z(self) -> None:
        fig = _build_monthly_calendar({"2024-03-01"}, set())
        assert 1 in _flatten_z(fig)

    def test_spans_multiple_years(self) -> None:
        fig = _build_monthly_calendar({"2023-12-01", "2024-01-01"}, set())
        y_labels = list(fig.data[0].y)
        assert "2023" in y_labels
        assert "2024" in y_labels

    def test_hover_contains_key(self) -> None:
        fig = _build_monthly_calendar({"2024-03-01"}, set())
        hover = _flatten_hover(fig)
        assert any("2024-03-01" in h for h in hover)

    def test_date_range_filters_months(self) -> None:
        keys = {f"2024-{m:02d}-01" for m in range(1, 13)}
        fig = _build_monthly_calendar(
            keys, set(), start_date=date(2024, 4, 1), end_date=date(2024, 9, 30)
        )
        hover = _flatten_hover(fig)
        assert any("2024-06-01" in h for h in hover)
        assert not any("2024-01-01" in h for h in hover)

    def test_auto_clips_large_range(self) -> None:
        # >48 months triggers auto-clip to last 24
        keys = {f"{y}-{m:02d}-01" for y in range(2018, 2024) for m in range(1, 13)}
        fig = _build_monthly_calendar(keys, set())
        hover = _flatten_hover(fig)
        assert any("2023-12-01" in h for h in hover)
        assert not any("2018-01-01" in h for h in hover)

    def test_empty_range_returns_none(self) -> None:
        fig = _build_monthly_calendar(
            {"2024-03-01"},
            set(),
            start_date=date(2025, 1, 1),
            end_date=date(2025, 12, 31),
        )
        assert fig is None


# ---------------------------------------------------------------------------
# _build_hourly_calendar (Plotly figure)
# ---------------------------------------------------------------------------


class TestBuildHourlyCalendar:
    def test_empty_keys(self) -> None:
        assert _build_hourly_calendar(set(), set()) is None

    def test_invalid_keys_returns_none(self) -> None:
        assert _build_hourly_calendar({"bad"}, set()) is None

    def test_returns_figure(self) -> None:
        fig = _build_hourly_calendar({"2024-01-15T10"}, set())
        assert fig is not None
        assert fig.data[0].type == "heatmap"

    def test_has_24_hour_columns(self) -> None:
        fig = _build_hourly_calendar({"2024-01-15T10"}, set())
        assert len(fig.data[0].x) == 24

    def test_completed_hour_in_z(self) -> None:
        fig = _build_hourly_calendar({"2024-01-15T10"}, {"2024-01-15T10"})
        assert 2 in _flatten_z(fig)

    def test_missing_hour_in_z(self) -> None:
        fig = _build_hourly_calendar({"2024-01-15T10"}, set())
        assert 1 in _flatten_z(fig)

    def test_spans_multiple_days(self) -> None:
        fig = _build_hourly_calendar({"2024-01-15T10", "2024-01-16T05"}, set())
        y_labels = list(fig.data[0].y)
        assert "2024-01-15" in y_labels
        assert "2024-01-16" in y_labels

    def test_hover_contains_key(self) -> None:
        fig = _build_hourly_calendar({"2024-01-15T10"}, set())
        hover = _flatten_hover(fig)
        assert any("2024-01-15T10" in h for h in hover)

    def test_date_range_filters_days(self) -> None:
        keys = {f"2024-01-{d:02d}T10" for d in range(1, 20)}
        fig = _build_hourly_calendar(
            keys, set(), start_date=date(2024, 1, 5), end_date=date(2024, 1, 10)
        )
        y_labels = list(fig.data[0].y)
        assert "2024-01-05" in y_labels
        assert "2024-01-10" in y_labels
        assert "2024-01-04" not in y_labels
        assert "2024-01-11" not in y_labels

    def test_auto_clips_to_last_7_days(self) -> None:
        keys = {f"2024-01-{d:02d}T10" for d in range(1, 31)}
        fig = _build_hourly_calendar(keys, set())
        y_labels = list(fig.data[0].y)
        assert len(y_labels) == 7
        assert "2024-01-30" in y_labels

    def test_no_auto_clip_when_within_threshold(self) -> None:
        keys = {f"2024-01-{d:02d}T10" for d in range(1, 10)}
        fig = _build_hourly_calendar(keys, set())
        y_labels = list(fig.data[0].y)
        assert len(y_labels) == 9

    def test_explicit_range_overrides_auto_clip(self) -> None:
        keys = {f"2024-01-{d:02d}T10" for d in range(1, 31)}
        fig = _build_hourly_calendar(
            keys, set(), start_date=date(2024, 1, 1), end_date=date(2024, 1, 30)
        )
        y_labels = list(fig.data[0].y)
        assert len(y_labels) == 30

    def test_empty_range_returns_none(self) -> None:
        fig = _build_hourly_calendar(
            {"2024-01-15T10"},
            set(),
            start_date=date(2024, 3, 1),
            end_date=date(2024, 3, 31),
        )
        assert fig is None


# ---------------------------------------------------------------------------
# _backfill_date_bounds
# ---------------------------------------------------------------------------


class TestBackfillDateBounds:
    def test_daily_bounds(self) -> None:
        keys = [f"2024-01-{d:02d}" for d in range(1, 10)]
        min_d, max_d, start, end = _backfill_date_bounds("daily", keys)
        assert min_d == date(2024, 1, 1)
        assert max_d == date(2024, 1, 9)
        assert start == min_d
        assert end == max_d

    def test_weekly_bounds(self) -> None:
        keys = ["2024-W01", "2024-W10", "2024-W20"]
        min_d, max_d, start, end = _backfill_date_bounds("weekly", keys)
        assert min_d == date.fromisocalendar(2024, 1, 1)
        assert max_d == date.fromisocalendar(2024, 20, 1)
        assert start == min_d
        assert end == max_d

    def test_monthly_bounds(self) -> None:
        keys = ["2024-01-01", "2024-06-01", "2024-12-01"]
        min_d, max_d, start, _end = _backfill_date_bounds("monthly", keys)
        assert min_d == date(2024, 1, 1)
        assert max_d == date(2024, 12, 1)
        assert start == min_d

    def test_hourly_empty_keys(self) -> None:
        assert _backfill_date_bounds("hourly", []) == (None, None, None, None)

    def test_hourly_invalid_keys(self) -> None:
        assert _backfill_date_bounds("hourly", ["bad"]) == (None, None, None, None)

    def test_hourly_single_day(self) -> None:
        min_d, max_d, start, end = _backfill_date_bounds("hourly", ["2024-01-15T10"])
        assert min_d == date(2024, 1, 15)
        assert max_d == date(2024, 1, 15)
        assert start == min_d
        assert end == max_d

    def test_hourly_small_range_no_clip(self) -> None:
        keys = [f"2024-01-{d:02d}T10" for d in range(1, 10)]
        min_d, max_d, start, _end = _backfill_date_bounds("hourly", keys)
        assert min_d == date(2024, 1, 1)
        assert max_d == date(2024, 1, 9)
        assert start == min_d  # no clipping

    def test_hourly_large_range_clips_to_last_7(self) -> None:
        today = whenever.ZonedDateTime.now("UTC").date().to_stdlib()
        keys = [f"2024-01-{d:02d}T10" for d in range(1, 31)]
        min_d, max_d, start, end = _backfill_date_bounds("hourly", keys)
        assert min_d is not None
        assert max_d is not None
        assert min_d == date(2024, 1, 1)
        assert max_d == date(2024, 1, 30)
        assert start is not None
        assert end is not None
        # end is clamped to min(today, max_d); start is end - 7 days
        expected_end = min(today, max_d)
        assert start == max(min_d, expected_end - timedelta(days=7))
        assert end == expected_end

    def test_hourly_bounds(self) -> None:
        keys = ["2024-01-15T10", "2024-01-16T05"]
        min_d, max_d, _start, _end = _backfill_date_bounds("hourly", keys)
        assert min_d == date(2024, 1, 15)
        assert max_d == date(2024, 1, 16)

    def test_empty_keys_returns_none_tuple(self) -> None:
        assert _backfill_date_bounds("daily", []) == (None, None, None, None)

    def test_unknown_kind_returns_none_tuple(self) -> None:
        assert _backfill_date_bounds("custom", ["a", "b"]) == (None, None, None, None)

    def test_init_end_anchored_to_today(self) -> None:
        today = whenever.ZonedDateTime.now("UTC").date().to_stdlib()
        # Keys span from 1 year ago to 1 year from now
        start = today - timedelta(days=365)
        keys = [(start + timedelta(days=i)).isoformat() for i in range(730)]
        _min_d, _max_d, init_start, init_end = _backfill_date_bounds("daily", keys)
        assert init_end == today
        assert init_start == today - timedelta(days=90)

    def test_init_end_clamps_to_max_d_when_data_in_past(self) -> None:
        keys = [f"2020-01-{d:02d}" for d in range(1, 15)]
        _, max_d, _, init_end = _backfill_date_bounds("daily", keys)
        # Data ends in 2020, today is later — clamp to max_d
        assert init_end == max_d
        assert init_end == date(2020, 1, 14)


# ---------------------------------------------------------------------------
# _build_partition_grid (Plotly figure)
# ---------------------------------------------------------------------------


class TestBuildPartitionGrid:
    def test_empty_keys(self) -> None:
        assert _build_partition_grid([], set()) is None

    def test_returns_figure(self) -> None:
        fig = _build_partition_grid(["us"], set())
        assert fig is not None
        assert fig.data[0].type == "heatmap"

    def test_completed_key_in_z(self) -> None:
        fig = _build_partition_grid(["us"], {"us"})
        assert 2 in _flatten_z(fig)

    def test_missing_key_in_z(self) -> None:
        fig = _build_partition_grid(["us"], set())
        assert 1 in _flatten_z(fig)

    def test_all_keys_on_x_axis(self) -> None:
        fig = _build_partition_grid(["a", "b", "c"], {"a", "c"})
        x_labels = list(fig.data[0].x)
        assert x_labels == ["a", "b", "c"]

    def test_hover_shows_status(self) -> None:
        fig = _build_partition_grid(["us", "eu"], {"us"})
        hover = _flatten_hover(fig)
        assert any("Completed" in h for h in hover)
        assert any("Missing" in h for h in hover)

    def test_hover_shows_run_info(self) -> None:
        key_run_info = {"us": (99, 1_705_312_800_000)}
        fig = _build_partition_grid(["us", "eu"], {"us"}, key_run_info)  # ty: ignore[invalid-argument-type]
        hover = _flatten_hover(fig)
        assert any("Run 99" in h for h in hover)
        assert any("Missing" in h for h in hover)

    def test_errored_key_in_z(self) -> None:
        fig = _build_partition_grid(["us", "eu"], set(), errored_keys={"eu"})
        assert 5 in _flatten_z(fig)

    def test_errored_hover_shows_failed(self) -> None:
        fig = _build_partition_grid(["us", "eu"], {"us"}, errored_keys={"eu"})
        hover = _flatten_hover(fig)
        assert any("Failed" in h for h in hover)
        assert any("Completed" in h for h in hover)


# ---------------------------------------------------------------------------
# _fmt_duration
# ---------------------------------------------------------------------------


class TestFmtDuration:
    def test_seconds_only(self) -> None:
        assert _fmt_duration(45) == "45s"

    def test_zero(self) -> None:
        assert _fmt_duration(0) == "0s"

    def test_minutes_and_seconds(self) -> None:
        assert _fmt_duration(125) == "2m 05s"

    def test_hours_minutes_seconds(self) -> None:
        assert _fmt_duration(3661) == "1h 01m 01s"

    def test_exact_minute(self) -> None:
        assert _fmt_duration(60) == "1m 00s"

    def test_exact_hour(self) -> None:
        assert _fmt_duration(3600) == "1h 00m 00s"


# ---------------------------------------------------------------------------
# APP_TEMPLATE
# ---------------------------------------------------------------------------


class TestAppTemplate:
    def test_renders_package_name(self) -> None:
        result = APP_TEMPLATE.format(
            package_name="my_pipeline", app_path="observability/app.py"
        )
        assert "import my_pipeline.pipelines" in result
        assert "run_app()" in result

    def test_renders_app_path(self) -> None:
        result = APP_TEMPLATE.format(package_name="pkg", app_path="custom/dashboard.py")
        assert "python custom/dashboard.py" in result


# ---------------------------------------------------------------------------
# Polars data helpers
# ---------------------------------------------------------------------------


class TestOverviewsToRecords:
    def test_empty(self) -> None:
        assert _overviews_to_records([]) == []

    def test_single_overview(self) -> None:
        o = JobOverview(
            job_name="etl",
            job_id=42,
            total_runs=10,
            successes=8,
            failures=2,
            last_run_time_ms=1_700_000_000_000,
            last_run_state="SUCCESS",
            avg_duration_seconds=45.0,
            has_backfill=True,
        )
        records = _overviews_to_records([o])
        assert len(records) == 1
        r = records[0]
        assert r["Job"] == "etl"
        assert r["Status"] == "SUCCESS"
        assert r["Runs"] == "10  (8 \u2713 / 2 \u2717)"
        assert r["Success %"] == "80%"
        assert r["Avg Duration"] == "45s"
        assert r["Completeness"] == ""

    def test_no_runs_shows_dash(self) -> None:
        o = JobOverview(job_name="j", job_id=None)
        records = _overviews_to_records([o])
        r = records[0]
        assert r["Success %"] == "\u2014"
        assert r["Status"] == "\u2014"
        assert r["Avg Duration"] == "\u2014"
        assert r["Runs"] == "\u2014"

    def test_workspace_url_adds_links(self) -> None:
        o = JobOverview(job_name="etl", job_id=42)
        records = _overviews_to_records([o], workspace_url="https://ws.databricks.com")
        assert records[0]["Job"] == "[etl](https://ws.databricks.com/jobs/42)"

    def test_workspace_url_skips_undeployed(self) -> None:
        o = JobOverview(job_name="local", job_id=None)
        records = _overviews_to_records([o], workspace_url="https://ws.databricks.com")
        assert records[0]["Job"] == "local"

    def test_no_workspace_url_plain_text(self) -> None:
        o = JobOverview(job_name="etl", job_id=42)
        records = _overviews_to_records([o])
        assert records[0]["Job"] == "etl"

    def test_coverage_with_coverages(self) -> None:
        o = JobOverview(job_name="etl", job_id=42, has_backfill=True)
        cov = BackfillCoverage(
            job_name="etl",
            expected_keys=["a", "b"],
            completed_keys=["a"],
            missing_keys=["b"],
            coverage_pct=50.0,
            kind="static",
        )
        records = _overviews_to_records([o], coverages={"etl": cov})
        assert records[0]["Completeness"] == "[50.0%](/backfills/etl)"

    def test_only_expected_columns(self) -> None:
        o = JobOverview(job_name="etl", job_id=42)
        records = _overviews_to_records([o])
        expected_cols = {
            "Job",
            "Status",
            "Runs",
            "Success %",
            "Avg Duration",
            "Completeness",
        }
        assert set(records[0].keys()) == expected_cols

    def test_success_rate_excludes_in_progress(self) -> None:
        """In-progress runs should not affect the success rate."""
        o = JobOverview(
            job_name="etl",
            job_id=42,
            total_runs=8,
            successes=7,
            failures=0,
            in_progress=1,
        )
        records = _overviews_to_records([o])
        # 7 successes / 7 terminal = 100%, not 87.5% (7/8)
        assert records[0]["Success %"] == "100%"


class TestCoveragesToRecords:
    def test_empty(self) -> None:
        assert _coverages_to_records({}) == []

    def test_basic(self) -> None:
        cov = BackfillCoverage(
            job_name="j",
            expected_keys=["a", "b"],
            completed_keys=["a"],
            missing_keys=["b"],
            coverage_pct=50.0,
            kind="static",
        )
        records = _coverages_to_records({"j": cov})
        assert len(records) == 1
        assert records[0]["Job"] == "j"
        assert "50.0%" in records[0]["Completeness"]
        assert "1 / 2" in records[0]["Completeness"]

    def test_static_squares_failed_first(self) -> None:
        cov = BackfillCoverage(
            job_name="j",
            expected_keys=["a", "b", "c"],
            completed_keys=["a", "c"],
            missing_keys=["b"],
            coverage_pct=66.7,
            kind="static",
            errored_keys=["b"],
        )
        records = _coverages_to_records({"j": cov})
        keys_cell = records[0]["Keys"]
        # Failed (red) should come before success (green)
        red_pos = keys_cell.index(_SQ_FAILED)
        green_pos = keys_cell.index(_SQ_COMPLETED)
        assert red_pos < green_pos

    def test_time_based_last_n_periods(self) -> None:
        cov = BackfillCoverage(
            job_name="j",
            expected_keys=[f"2024-01-{d:02d}" for d in range(1, 11)],
            completed_keys=[f"2024-01-{d:02d}" for d in range(1, 9)],
            missing_keys=["2024-01-09", "2024-01-10"],
            coverage_pct=80.0,
            kind="daily",
        )
        records = _coverages_to_records({"j": cov})
        keys_cell = records[0]["Keys"]
        # Last 5 periods: days 6-10, so 3 completed + 2 missing
        assert keys_cell.count(_SQ_COMPLETED) == 3
        assert keys_cell.count(_SQ_MISSING) == 2
        # Latest date (missing) should be rightmost
        last_completed = keys_cell.rindex(_SQ_COMPLETED)
        first_missing = keys_cell.index(_SQ_MISSING)
        assert last_completed < first_missing

    def test_static_caps_at_max(self) -> None:
        cov = BackfillCoverage(
            job_name="j",
            expected_keys=[f"k{i}" for i in range(20)],
            completed_keys=[f"k{i}" for i in range(20)],
            missing_keys=[],
            coverage_pct=100.0,
            kind="static",
        )
        records = _coverages_to_records({"j": cov})
        assert records[0]["Keys"].count(_SQ_COMPLETED) == 5

    def test_errored_keys_column(self) -> None:
        cov = BackfillCoverage(
            job_name="j",
            expected_keys=["a", "b"],
            completed_keys=["a"],
            missing_keys=["b"],
            coverage_pct=50.0,
            kind="static",
            errored_keys=["b"],
        )
        records = _coverages_to_records({"j": cov})
        assert records[0]["Errors"] == 1
