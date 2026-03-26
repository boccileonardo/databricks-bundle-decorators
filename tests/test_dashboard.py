"""Tests for the observability dashboard (CLI + Streamlit approach)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from databricks_bundle_decorators.dashboard import (
    APP_TEMPLATE,
    BackfillCoverage,
    RunInfo,
    TaskRunInfo,
    _backfill_kind,
    _build_daily_calendar,
    _build_hourly_calendar,
    _build_monthly_calendar,
    _build_partition_grid,
    _build_weekly_calendar,
    build_job_overview,
    compute_backfill_coverage,
    fetch_job_runs,
    fetch_task_runs,
    resolve_job_ids,
)


def _flatten_z(fig: object) -> list[int]:
    """Flatten the z matrix from a Plotly heatmap figure."""
    return [int(v) for row in fig.data[0].z for v in row]  # type: ignore[union-attr]


def _flatten_hover(fig: object) -> list[str]:
    """Flatten the hovertext matrix from a Plotly heatmap figure."""
    return [str(v) for row in fig.data[0].hovertext for v in row]  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Helpers for building mock CLI responses
# ---------------------------------------------------------------------------


def _cli_run(
    *,
    run_id: int = 1,
    result_state: str | None = "SUCCESS",
    start_time: int = 1_000_000,
    end_time: int = 1_060_000,
    backfill_key: str | None = None,
) -> dict[str, Any]:
    state: dict[str, Any] = {}
    if result_state is not None:
        state["result_state"] = result_state
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


def _cli_task(
    *,
    task_key: str = "extract",
    result_state: str | None = "SUCCESS",
    start_time: int = 1_000_000,
    end_time: int = 1_030_000,
) -> dict[str, Any]:
    state: dict[str, Any] = {}
    if result_state is not None:
        state["result_state"] = result_state
    return {
        "task_key": task_key,
        "state": state,
        "start_time": start_time,
        "end_time": end_time,
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
            r.run_id = 2  # type: ignore[misc]

    def test_defaults(self) -> None:
        r = RunInfo(
            run_id=1,
            result_state=None,
            start_time_ms=None,
            end_time_ms=None,
            duration_seconds=None,
        )
        assert r.backfill_key is None


class TestTaskRunInfo:
    def test_frozen(self) -> None:
        t = TaskRunInfo(
            task_key="a",
            result_state="SUCCESS",
            start_time_ms=1000,
            end_time_ms=2000,
            duration_seconds=1.0,
        )
        with pytest.raises(AttributeError):
            t.task_key = "b"  # type: ignore[misc]


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
        assert o.last_run_time_ms == 1000
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
            RunInfo(1, None, 3000, None, None),
            RunInfo(2, "SUCCESS", 2000, 32000, 30.0),
        ]
        o = build_job_overview("j", job_id=1, runs=runs)
        assert o.total_runs == 2
        assert o.successes == 1
        assert o.failures == 0

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


# ---------------------------------------------------------------------------
# fetch_job_runs (mocks subprocess — CLI-based)
# ---------------------------------------------------------------------------


class TestFetchJobRuns:
    def test_basic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runs_json = json.dumps([_cli_run(run_id=10, result_state="SUCCESS")])
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard.subprocess.run",
            _mock_subprocess(stdout=runs_json),
        )
        runs = fetch_job_runs(42)
        assert len(runs) == 1
        assert runs[0].run_id == 10
        assert runs[0].result_state == "SUCCESS"

    def test_computes_duration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runs_json = json.dumps([_cli_run(start_time=1_000_000, end_time=1_060_000)])
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard.subprocess.run",
            _mock_subprocess(stdout=runs_json),
        )
        runs = fetch_job_runs(1)
        assert runs[0].duration_seconds == 60.0

    def test_extracts_backfill_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runs_json = json.dumps([_cli_run(backfill_key="2024-01-15")])
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard.subprocess.run",
            _mock_subprocess(stdout=runs_json),
        )
        runs = fetch_job_runs(1)
        assert runs[0].backfill_key == "2024-01-15"

    def test_handles_no_backfill_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runs_json = json.dumps([_cli_run()])
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard.subprocess.run",
            _mock_subprocess(stdout=runs_json),
        )
        runs = fetch_job_runs(1)
        assert runs[0].backfill_key is None

    def test_handles_running_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runs_json = json.dumps([_cli_run(result_state=None, end_time=0)])
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard.subprocess.run",
            _mock_subprocess(stdout=runs_json),
        )
        runs = fetch_job_runs(1)
        assert runs[0].result_state is None

    def test_empty_runs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard.subprocess.run",
            _mock_subprocess(stdout="[]"),
        )
        runs = fetch_job_runs(1)
        assert runs == []

    def test_returns_empty_on_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard.subprocess.run",
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
            "databricks_bundle_decorators.dashboard.subprocess.run",
            mock_run,
        )
        fetch_job_runs(42, profile="work")
        assert "--profile" in captured_cmd
        assert "work" in captured_cmd
        assert "--job-id" in captured_cmd
        assert "42" in captured_cmd


# ---------------------------------------------------------------------------
# fetch_task_runs (mocks subprocess — CLI-based)
# ---------------------------------------------------------------------------


class TestFetchTaskRuns:
    def test_basic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        run_json = json.dumps(
            {"tasks": [_cli_task(task_key="extract", result_state="SUCCESS")]}
        )
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard.subprocess.run",
            _mock_subprocess(stdout=run_json),
        )
        tasks = fetch_task_runs(1)
        assert len(tasks) == 1
        assert tasks[0].task_key == "extract"
        assert tasks[0].result_state == "SUCCESS"

    def test_computes_duration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        run_json = json.dumps(
            {"tasks": [_cli_task(start_time=1_000_000, end_time=1_030_000)]}
        )
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard.subprocess.run",
            _mock_subprocess(stdout=run_json),
        )
        tasks = fetch_task_runs(1)
        assert tasks[0].duration_seconds == 30.0

    def test_empty_tasks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        run_json = json.dumps({"tasks": []})
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard.subprocess.run",
            _mock_subprocess(stdout=run_json),
        )
        tasks = fetch_task_runs(1)
        assert tasks == []

    def test_no_tasks_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        run_json = json.dumps({})
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard.subprocess.run",
            _mock_subprocess(stdout=run_json),
        )
        tasks = fetch_task_runs(1)
        assert tasks == []

    def test_returns_empty_on_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard.subprocess.run",
            _mock_subprocess(returncode=1, stderr="error"),
        )
        tasks = fetch_task_runs(1)
        assert tasks == []

    def test_passes_profile(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured_cmd: list[str] = []

        def mock_run(cmd: list[str], **kw: Any) -> SimpleNamespace:
            captured_cmd.extend(cmd)
            return SimpleNamespace(
                returncode=0, stdout=json.dumps({"tasks": []}), stderr=""
            )

        monkeypatch.setattr(
            "databricks_bundle_decorators.dashboard.subprocess.run",
            mock_run,
        )
        fetch_task_runs(99, profile="work")
        assert "--profile" in captured_cmd
        assert "work" in captured_cmd
        assert "--run-id" in captured_cmd
        assert "99" in captured_cmd


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
            "databricks_bundle_decorators.dashboard.subprocess.run",
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
            "databricks_bundle_decorators.dashboard.subprocess.run",
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
            "databricks_bundle_decorators.dashboard.subprocess.run",
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
            "databricks_bundle_decorators.dashboard.subprocess.run",
            lambda cmd, **kw: SimpleNamespace(
                returncode=0, stdout=json.dumps(summary), stderr=""
            ),
        )
        result = resolve_job_ids()
        assert result == {"deployed": 111}


# ---------------------------------------------------------------------------
# _backfill_kind
# ---------------------------------------------------------------------------


class TestBackfillKind:
    def test_daily(self) -> None:
        from databricks_bundle_decorators.backfill import DailyBackfill

        bf = DailyBackfill(start_date="2024-01-01", end_date="2024-01-31")
        assert _backfill_kind(bf) == "daily"

    def test_weekly(self) -> None:
        from databricks_bundle_decorators.backfill import WeeklyBackfill

        bf = WeeklyBackfill(start_date="2024-W01", end_date="2024-W04")
        assert _backfill_kind(bf) == "weekly"

    def test_monthly(self) -> None:
        from databricks_bundle_decorators.backfill import MonthlyBackfill

        bf = MonthlyBackfill(start_date="2024-01-01", end_date="2024-03-01")
        assert _backfill_kind(bf) == "monthly"

    def test_hourly(self) -> None:
        from databricks_bundle_decorators.backfill import HourlyBackfill

        bf = HourlyBackfill(start_date="2024-01-01T00", end_date="2024-01-01T23")
        assert _backfill_kind(bf) == "hourly"

    def test_static(self) -> None:
        from databricks_bundle_decorators.backfill import StaticBackfill

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
        assert "streamlit run custom/dashboard.py" in result
