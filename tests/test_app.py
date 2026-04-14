"""Tests for the Databricks App module."""

from __future__ import annotations

import os

import pytest

from databricks_bundle_decorators.app._codegen import generate_app_resource
from databricks_bundle_decorators.app._fetch import (
    _JOB_ENV_PREFIX,
    resolve_job_ids_from_env,
    resolve_workspace_url,
)
from databricks_bundle_decorators.registry import (
    _JOB_REGISTRY,
    _TASK_REGISTRY,
    JobMeta,
    TaskMeta,
    reset_registries,
)


class TestResolveJobIdsFromEnv:
    """Tests for resolve_job_ids_from_env."""

    def setup_method(self) -> None:
        reset_registries()

    def test_reads_dbxdec_job_env_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DBXDEC_JOB_ETL_DAILY", "12345")
        monkeypatch.setenv("DBXDEC_JOB_BACKFILL", "67890")

        result = resolve_job_ids_from_env()

        assert result == {"etl_daily": 12345, "backfill": 67890}

    def test_ignores_non_dbxdec_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DBXDEC_JOB_MY_JOB", "111")
        monkeypatch.setenv("SOME_OTHER_VAR", "222")

        result = resolve_job_ids_from_env()

        assert result == {"my_job": 111}
        assert "SOME_OTHER_VAR" not in result

    def test_skips_non_numeric_values(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DBXDEC_JOB_GOOD", "999")
        monkeypatch.setenv("DBXDEC_JOB_BAD", "not_a_number")

        result = resolve_job_ids_from_env()

        assert result == {"good": 999}

    def test_empty_when_no_dbxdec_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Ensure no DBXDEC_JOB_* vars exist
        for key in list(os.environ):
            if key.startswith(_JOB_ENV_PREFIX):
                monkeypatch.delenv(key)

        result = resolve_job_ids_from_env()

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

        # Check env vars
        env = app_def["config"]["env"]
        assert len(env) == 2
        env_names = {e["name"] for e in env}
        assert "DBXDEC_JOB_BACKFILL" in env_names
        assert "DBXDEC_JOB_ETL_DAILY" in env_names

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
        assert app_def["config"]["env"] == []

    def test_env_valuefrom_matches_resource_name(self) -> None:
        _JOB_REGISTRY["etl_daily"] = JobMeta(
            fn=_dummy_fn,
            name="etl_daily",
            dag={},
        )

        result = generate_app_resource("test-app")

        app_def = result["test_app"]
        resource_name = app_def["resources"][0]["name"]
        env_value_from = app_def["config"]["env"][0]["valueFrom"]
        assert resource_name == env_value_from
        assert resource_name == "dbxdec-job-etl-daily"
