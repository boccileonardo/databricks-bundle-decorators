"""Tests for the Databricks App module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from databricks_bundle_decorators.app._codegen import (
    generate_app_config_yaml,
    generate_app_resource,
    generate_registry_json,
    sync_registry_json,
)
from databricks_bundle_decorators.app._fetch import (
    _compute_exec_duration,
    resolve_job_ids_from_sdk,
    resolve_workspace_url,
)
from databricks_bundle_decorators.backfill import (
    DailyBackfill,
    HourlyBackfill,
    MonthlyBackfill,
    StaticBackfill,
    WeeklyBackfill,
    _deserialize_backfill_tag,
    _serialize_backfill_tag,
)
from databricks_bundle_decorators.registry import (
    _JOB_REGISTRY,
    _TASK_REGISTRY,
    JobMeta,
    TaskMeta,
    reset_registries,
)


class TestResolveJobIdsFromSdk:
    """Tests for resolve_job_ids_from_sdk."""

    def test_returns_empty_when_no_app_name(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)

        result = resolve_job_ids_from_sdk()

        assert result == {}

    def test_returns_empty_on_sdk_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABRICKS_APP_NAME", "my-app")
        # SDK import will fail or WorkspaceClient will fail without
        # proper credentials — the function should catch and return {}
        result = resolve_job_ids_from_sdk()

        assert result == {}


class TestResolveWorkspaceUrl:
    """Tests for resolve_workspace_url."""

    def test_reads_databricks_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(
            "DATABRICKS_HOST", "https://my-workspace.cloud.databricks.com"
        )

        result = resolve_workspace_url()

        assert result == "https://my-workspace.cloud.databricks.com"

    def test_strips_trailing_slash(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(
            "DATABRICKS_HOST", "https://my-workspace.cloud.databricks.com/"
        )

        result = resolve_workspace_url()

        assert result == "https://my-workspace.cloud.databricks.com"

    def test_adds_https_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "my-workspace.cloud.databricks.com")

        result = resolve_workspace_url()

        assert result == "https://my-workspace.cloud.databricks.com"

    def test_returns_none_when_not_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)

        result = resolve_workspace_url()

        assert result is None


class TestComputeExecDuration:
    """Tests for _compute_exec_duration."""

    def test_returns_none_for_no_tasks(self) -> None:
        assert _compute_exec_duration(None) is None
        assert _compute_exec_duration([]) is None

    def test_returns_none_when_tasks_lack_timestamps(self) -> None:
        class _Task:
            end_time = None
            execution_duration = None

        assert _compute_exec_duration([_Task()]) is None

    def test_computes_exec_window_excluding_queue(self) -> None:
        """Simulates a job queued 8 hours but executing in ~6.6 minutes."""

        class _TaskA:
            # Task started executing at t=0ms, ran for 300_000ms (5 min)
            end_time = 300_000
            execution_duration = 300_000

        class _TaskB:
            # Task started executing at t=100_000ms, ran for 296_000ms
            end_time = 396_000
            execution_duration = 296_000

        result = _compute_exec_duration([_TaskA(), _TaskB()])

        # exec window = max(300000, 396000) - min(0, 100000) = 396000 ms = 396.0 s
        assert result == 396.0

    def test_single_task(self) -> None:
        class _Task:
            end_time = 1_000_000
            execution_duration = 60_000  # 60 seconds

        result = _compute_exec_duration([_Task()])

        assert result == 60.0

    def test_returns_none_when_any_task_lacks_exec_duration(self) -> None:
        class _GoodTask:
            end_time = 500_000
            execution_duration = 100_000

        class _BadTask:
            end_time = None
            execution_duration = None

        result = _compute_exec_duration([_GoodTask(), _BadTask()])

        assert result is None


def _dummy_fn() -> None:
    pass


class TestGenerateAppResource:
    """Tests for generate_app_resource."""

    def setup_method(self) -> None:
        reset_registries()

    def test_generates_resource_for_registered_jobs(self) -> None:
        _JOB_REGISTRY["etl_daily"] = JobMeta(
            fn=_dummy_fn,
            name="etl_daily",
            dag={},
        )
        _TASK_REGISTRY["etl_daily.extract"] = TaskMeta(
            fn=_dummy_fn,
            task_key="extract",
        )
        _JOB_REGISTRY["backfill"] = JobMeta(
            fn=_dummy_fn,
            name="backfill",
            dag={},
        )

        result = generate_app_resource("my-observability")

        assert "my_observability" in result
        app_def = result["my_observability"]
        assert app_def["name"] == "my-observability"
        assert app_def["source_code_path"] == "./app"

        # Check resources
        resources = app_def["resources"]
        assert len(resources) == 2
        job_names = {r["description"] for r in resources}
        assert "Job: backfill" in job_names
        assert "Job: etl_daily" in job_names

        # No env in bundle config — env vars live in app.yaml
        assert "env" not in app_def["config"]

    def test_job_resource_uses_interpolation(self) -> None:
        _JOB_REGISTRY["my_job"] = JobMeta(
            fn=_dummy_fn,
            name="my_job",
            dag={},
        )

        result = generate_app_resource("test-app")

        app_def = result["test_app"]
        resources = app_def["resources"]
        assert len(resources) == 1
        assert resources[0]["job"]["id"] == "${resources.jobs.my_job.id}"
        assert resources[0]["job"]["permission"] == "CAN_VIEW"

    def test_custom_permission(self) -> None:
        _JOB_REGISTRY["my_job"] = JobMeta(
            fn=_dummy_fn,
            name="my_job",
            dag={},
        )

        result = generate_app_resource("test-app", permission="CAN_MANAGE_RUN")

        app_def = result["test_app"]
        assert app_def["resources"][0]["job"]["permission"] == "CAN_MANAGE_RUN"

    def test_custom_source_code_path(self) -> None:
        _JOB_REGISTRY["my_job"] = JobMeta(
            fn=_dummy_fn,
            name="my_job",
            dag={},
        )

        result = generate_app_resource("test-app", source_code_path="./my_app")

        app_def = result["test_app"]
        assert app_def["source_code_path"] == "./my_app"

    def test_empty_registry(self) -> None:
        result = generate_app_resource("test-app")

        app_def = result["test_app"]
        assert app_def["resources"] == []
        assert "env" not in app_def["config"]


class TestGenerateAppConfigYaml:
    """Tests for generate_app_config_yaml."""

    def setup_method(self) -> None:
        reset_registries()

    def test_produces_valid_yaml_structure(self) -> None:
        _JOB_REGISTRY["my_job"] = JobMeta(
            fn=_dummy_fn,
            name="my_job",
            dag={},
        )

        yaml_text = generate_app_config_yaml("test-app")

        assert "resources:" in yaml_text
        assert "  apps:" in yaml_text
        assert "    test_app:" in yaml_text
        assert "      name: test-app" in yaml_text

    def test_includes_job_bindings(self) -> None:
        _JOB_REGISTRY["etl_daily"] = JobMeta(
            fn=_dummy_fn,
            name="etl_daily",
            dag={},
        )

        yaml_text = generate_app_config_yaml("my-app")

        assert "etl-daily" in yaml_text
        assert "${resources.jobs.etl_daily.id}" in yaml_text

    def test_yaml_has_no_job_permissions_section(self) -> None:
        """Job permissions are handled in codegen, not in the YAML."""
        _JOB_REGISTRY["etl_daily"] = JobMeta(
            fn=_dummy_fn,
            name="etl_daily",
            dag={},
        )
        _JOB_REGISTRY["backfill"] = JobMeta(
            fn=_dummy_fn,
            name="backfill",
            dag={},
        )

        yaml_text = generate_app_config_yaml("my-app")

        # YAML should NOT contain a jobs section — permissions are
        # injected via generate_resources(app_resource_key=...) instead
        assert "  jobs:" not in yaml_text

    def test_empty_registry_no_env_or_resources(self) -> None:
        yaml_text = generate_app_config_yaml("test-app")

        assert "resources:" in yaml_text
        assert "  apps:" in yaml_text
        # No env or resource sections for empty registry
        assert "DBXDEC_JOB_" not in yaml_text

    def test_header_comments(self) -> None:
        yaml_text = generate_app_config_yaml("test-app")

        assert "Auto-generated by: dbxdec init --dashboard" in yaml_text
        assert "dbxdec app-config" in yaml_text


class TestDeserializeBackfillTag:
    """Tests for _deserialize_backfill_tag round-trip."""

    def test_daily_round_trip(self) -> None:
        original = DailyBackfill(
            start_date="2024-01-01", end_date="2024-03-31", tz="Europe/Berlin"
        )
        serialized = _serialize_backfill_tag(original)
        restored = _deserialize_backfill_tag(serialized)

        assert isinstance(restored, DailyBackfill)
        assert restored.start_date == "2024-01-01"
        assert restored.end_date == "2024-03-31"
        assert restored.tz == "Europe/Berlin"

    def test_daily_default_tz(self) -> None:
        original = DailyBackfill(start_date="2024-01-01")
        restored = _deserialize_backfill_tag(_serialize_backfill_tag(original))

        assert isinstance(restored, DailyBackfill)
        assert restored.tz == "UTC"

    def test_weekly_round_trip(self) -> None:
        original = WeeklyBackfill(start_date="2024-W01", end_date="2024-W10")
        restored = _deserialize_backfill_tag(_serialize_backfill_tag(original))

        assert isinstance(restored, WeeklyBackfill)
        assert restored.start_date == "2024-W01"
        assert restored.end_date == "2024-W10"

    def test_monthly_round_trip(self) -> None:
        original = MonthlyBackfill(start_date="2024-01-01", end_date="2024-06-01")
        restored = _deserialize_backfill_tag(_serialize_backfill_tag(original))

        assert isinstance(restored, MonthlyBackfill)
        assert restored.start_date == "2024-01-01"
        assert restored.end_date == "2024-06-01"

    def test_hourly_round_trip(self) -> None:
        original = HourlyBackfill(
            start_date="2024-01-01T00",
            end_date="2024-01-01T23",
            tz="America/New_York",
        )
        restored = _deserialize_backfill_tag(_serialize_backfill_tag(original))

        assert isinstance(restored, HourlyBackfill)
        assert restored.start_date == "2024-01-01T00"
        assert restored.end_date == "2024-01-01T23"
        assert restored.tz == "America/New_York"

    def test_static_round_trip(self) -> None:
        original = StaticBackfill(keys=["us", "eu", "jp"])
        restored = _deserialize_backfill_tag(_serialize_backfill_tag(original))

        assert isinstance(restored, StaticBackfill)
        assert restored.keys() == ["us", "eu", "jp"]

    def test_accepts_dict(self) -> None:
        d = {"type": "daily", "start_date": "2024-01-01"}
        restored = _deserialize_backfill_tag(d)

        assert isinstance(restored, DailyBackfill)
        assert restored.start_date == "2024-01-01"

    def test_unknown_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown backfill type"):
            _deserialize_backfill_tag({"type": "biweekly", "start_date": "x"})

    def test_keys_match_after_round_trip(self) -> None:
        original = DailyBackfill(start_date="2024-01-01", end_date="2024-01-05")
        restored = _deserialize_backfill_tag(_serialize_backfill_tag(original))

        assert restored.keys() == original.keys()


class TestGenerateRegistryJson:
    """Tests for generate_registry_json."""

    def setup_method(self) -> None:
        reset_registries()

    def test_empty_registry(self) -> None:
        result = json.loads(generate_registry_json())
        assert result == {}

    def test_jobs_without_backfill_included_as_null(self) -> None:
        _JOB_REGISTRY["no_backfill"] = JobMeta(fn=_dummy_fn, name="no_backfill", dag={})

        result = json.loads(generate_registry_json())
        assert "no_backfill" in result
        assert result["no_backfill"] is None

    def test_daily_backfill_serialized(self) -> None:
        _JOB_REGISTRY["etl_daily"] = JobMeta(
            fn=_dummy_fn,
            name="etl_daily",
            dag={},
            backfill=DailyBackfill(start_date="2024-01-01", tz="Europe/Berlin"),
        )

        result = json.loads(generate_registry_json())
        assert "etl_daily" in result
        assert result["etl_daily"]["type"] == "daily"
        assert result["etl_daily"]["start_date"] == "2024-01-01"
        assert result["etl_daily"]["tz"] == "Europe/Berlin"

    def test_static_backfill_serialized(self) -> None:
        _JOB_REGISTRY["partition_job"] = JobMeta(
            fn=_dummy_fn,
            name="partition_job",
            dag={},
            backfill=StaticBackfill(keys=["us", "eu"]),
        )

        result = json.loads(generate_registry_json())
        assert result["partition_job"]["type"] == "static"
        assert result["partition_job"]["keys"] == ["us", "eu"]

    def test_multiple_jobs(self) -> None:
        _JOB_REGISTRY["job_a"] = JobMeta(
            fn=_dummy_fn,
            name="job_a",
            dag={},
            backfill=DailyBackfill(start_date="2024-01-01"),
        )
        _JOB_REGISTRY["job_b"] = JobMeta(
            fn=_dummy_fn,
            name="job_b",
            dag={},
            backfill=StaticBackfill(keys=["x"]),
        )
        _JOB_REGISTRY["job_no_bf"] = JobMeta(fn=_dummy_fn, name="job_no_bf", dag={})

        result = json.loads(generate_registry_json())
        assert set(result.keys()) == {"job_a", "job_b", "job_no_bf"}
        assert result["job_no_bf"] is None

    def test_round_trip_preserves_keys(self) -> None:
        bf = DailyBackfill(start_date="2024-01-01", end_date="2024-01-05")
        _JOB_REGISTRY["my_job"] = JobMeta(
            fn=_dummy_fn, name="my_job", dag={}, backfill=bf
        )

        registry_data = json.loads(generate_registry_json())
        restored = _deserialize_backfill_tag(registry_data["my_job"])

        assert restored.keys() == bf.keys()

    def test_schedule_cron_included(self) -> None:
        class _FakeSchedule:
            quartz_cron_expression = "0 0 6 ? * 2-6"

        _JOB_REGISTRY["sched_job"] = JobMeta(
            fn=_dummy_fn,
            name="sched_job",
            dag={},
            backfill=DailyBackfill(start_date="2024-01-01", collect_schedule_gaps=True),
            sdk_config={"schedule": _FakeSchedule()},
        )

        result = json.loads(generate_registry_json())
        assert result["sched_job"]["schedule_cron"] == "0 0 6 ? * 2-6"
        assert result["sched_job"]["collect_schedule_gaps"] is True

    def test_no_schedule_cron_when_no_schedule(self) -> None:
        _JOB_REGISTRY["no_sched"] = JobMeta(
            fn=_dummy_fn,
            name="no_sched",
            dag={},
            backfill=DailyBackfill(start_date="2024-01-01"),
        )

        result = json.loads(generate_registry_json())
        assert "schedule_cron" not in result["no_sched"]


class TestSyncRegistryJson:
    """Tests for sync_registry_json."""

    def setup_method(self) -> None:
        reset_registries()

    def test_writes_file_when_app_dir_exists(self, tmp_path: Path) -> None:
        (tmp_path / "app").mkdir()
        _JOB_REGISTRY["my_job"] = JobMeta(
            fn=_dummy_fn,
            name="my_job",
            dag={},
            backfill=DailyBackfill(start_date="2024-01-01"),
        )

        result = sync_registry_json(project_root=tmp_path)

        assert result is True
        registry_file = tmp_path / "app" / "registry.json"
        assert registry_file.exists()
        data = json.loads(registry_file.read_text())
        assert "my_job" in data
        assert data["my_job"]["type"] == "daily"

    def test_returns_false_when_no_app_dir(self, tmp_path: Path) -> None:
        result = sync_registry_json(project_root=tmp_path)

        assert result is False
        assert not (tmp_path / "app" / "registry.json").exists()

    def test_overwrites_existing_file(self, tmp_path: Path) -> None:
        app_dir = tmp_path / "app"
        app_dir.mkdir()
        (app_dir / "registry.json").write_text('{"old": "data"}')

        _JOB_REGISTRY["new_job"] = JobMeta(
            fn=_dummy_fn,
            name="new_job",
            dag={},
            backfill=StaticBackfill(keys=["a"]),
        )

        sync_registry_json(project_root=tmp_path)

        data = json.loads((app_dir / "registry.json").read_text())
        assert "new_job" in data
        assert "old" not in data
