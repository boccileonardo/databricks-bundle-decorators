"""Tests for the CLI scaffolding command (dbxdec init)."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from databricks_bundle_decorators.backfill import DailyBackfill
from databricks_bundle_decorators.cli import (
    _cmd_backfill,
    _cmd_backfill_catchup,
    _cmd_init,
    _detect_package_name,
    _detect_src_layout,
    _get_launched_backfill_keys,
    _read_app_name_from_yml,
    _read_pyproject,
    app_config,
    main,
)
from databricks_bundle_decorators.decorators import job, task
from databricks_bundle_decorators.registry import reset_registries


def _raise_import_error(name: str) -> None:
    raise ImportError(name)


class TestReadPyproject:
    def test_reads_valid_toml(self, tmp_path: Path):
        (tmp_path / "pyproject.toml").write_text(
            '[project]\nname = "my-project"\nversion = "0.1.0"\n'
        )
        result = _read_pyproject(tmp_path)
        assert result["project"]["name"] == "my-project"

    def test_exits_when_missing(self, tmp_path: Path):
        with pytest.raises(SystemExit):
            _read_pyproject(tmp_path)


class TestDetectPackageName:
    def test_hyphens_become_underscores(self):
        assert (
            _detect_package_name({"project": {"name": "my-cool-project"}})
            == "my_cool_project"
        )

    def test_simple_name(self):
        assert _detect_package_name({"project": {"name": "simple"}}) == "simple"

    def test_exits_when_no_name(self):
        with pytest.raises(SystemExit):
            _detect_package_name({"project": {}})

    def test_exits_when_no_project(self):
        with pytest.raises(SystemExit):
            _detect_package_name({})


class TestDetectSrcLayout:
    def test_prefers_src_layout(self, tmp_path: Path):
        src_dir = tmp_path / "src" / "my_pkg"
        src_dir.mkdir(parents=True)
        assert _detect_src_layout(tmp_path, "my_pkg") == src_dir

    def test_falls_back_to_flat_layout(self, tmp_path: Path):
        flat_dir = tmp_path / "my_pkg"
        flat_dir.mkdir()
        assert _detect_src_layout(tmp_path, "my_pkg") == flat_dir

    def test_defaults_to_src_when_neither_exists(self, tmp_path: Path):
        result = _detect_src_layout(tmp_path, "my_pkg")
        assert result == tmp_path / "src" / "my_pkg"


class TestCmdInit:
    def _make_project(self, tmp_path: Path, name: str = "test-project") -> None:
        (tmp_path / "pyproject.toml").write_text(
            f'[project]\nname = "{name}"\nversion = "0.1.0"\n'
        )

    def test_creates_all_files(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)

        _cmd_init()

        assert (tmp_path / "resources" / "__init__.py").exists()
        assert (
            tmp_path / "src" / "test_project" / "pipelines" / "__init__.py"
        ).exists()
        assert (tmp_path / "src" / "test_project" / "pipelines" / "example.py").exists()
        assert (tmp_path / "databricks.yaml").exists()
        assert (tmp_path / "src" / "test_project" / "__init__.py").exists()

        # resources/__init__.py directly imports the user's pipelines package
        resources_content = (tmp_path / "resources" / "__init__.py").read_text()
        assert "import test_project.pipelines" in resources_content
        assert "discover_pipelines" not in resources_content

        # Entry point was auto-added to pyproject.toml
        pyproject_content = (tmp_path / "pyproject.toml").read_text()
        assert 'test_project = "test_project.pipelines"' in pyproject_content

    def test_skips_existing_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)

        # Pre-create a file that init would create
        resources_dir = tmp_path / "resources"
        resources_dir.mkdir()
        (resources_dir / "__init__.py").write_text("# existing")

        _cmd_init()

        # Should not overwrite
        assert (resources_dir / "__init__.py").read_text() == "# existing"
        # But other files should still be created
        assert (tmp_path / "databricks.yaml").exists()

    def test_databricks_yaml_contains_project_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        self._make_project(tmp_path, "my-pipeline")
        monkeypatch.chdir(tmp_path)

        _cmd_init()

        content = (tmp_path / "databricks.yaml").read_text()
        assert "my-pipeline" in content
        assert "my_pipeline" in content

    def test_prints_entry_point_hint_when_missing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture,
    ):
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)

        _cmd_init()

        captured = capsys.readouterr()
        assert "Modified" in captured.out
        assert "entry point" in captured.out

        # Entry point was actually added
        content = (tmp_path / "pyproject.toml").read_text()
        assert "databricks_bundle_decorators.pipelines" in content
        assert 'test_project = "test_project.pipelines"' in content

    def test_no_entry_point_hint_when_present(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture,
    ):
        pyproject = (
            '[project]\nname = "test-project"\nversion = "0.1.0"\n\n'
            '[project.entry-points."databricks_bundle_decorators.pipelines"]\n'
            'test_project = "test_project.pipelines"\n'
        )
        (tmp_path / "pyproject.toml").write_text(pyproject)
        monkeypatch.chdir(tmp_path)

        _cmd_init()

        captured = capsys.readouterr()
        assert "Modified" not in captured.out

        # pyproject.toml should not be modified
        content = (tmp_path / "pyproject.toml").read_text()
        assert content == pyproject

    def test_docker_flag_creates_docker_example(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)

        _cmd_init(docker=True)

        example_path = tmp_path / "src" / "test_project" / "pipelines" / "example.py"
        assert example_path.exists()
        content = example_path.read_text()
        # Docker example uses libraries=[] and docker_image
        assert "libraries=[]" in content
        assert "docker_image" in content
        assert "PolarsParquetIoManager" not in content

        # databricks.yaml should NOT have artifacts section
        yaml_content = (tmp_path / "databricks.yaml").read_text()
        assert "artifacts:" not in yaml_content
        assert "No artifacts section" in yaml_content

    def test_default_init_creates_wheel_example(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)

        _cmd_init(docker=False)

        example_path = tmp_path / "src" / "test_project" / "pipelines" / "example.py"
        content = example_path.read_text()
        # Default example uses PolarsParquetIoManager
        assert "PolarsParquetIoManager" in content
        assert "libraries=[]" not in content

        # databricks.yaml should have artifacts section
        yaml_content = (tmp_path / "databricks.yaml").read_text()
        assert "artifacts" in yaml_content

    def test_dashboard_flag_creates_app_files(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.subprocess.run",
            lambda *a, **kw: None,
        )

        _cmd_init(dashboard=True)

        # app/ directory files
        assert (tmp_path / "app" / "app.py").exists()
        assert (tmp_path / "app" / "app.yaml").exists()
        assert (tmp_path / "app" / "pyproject.toml").exists()

        # app.py is the app entry point
        app_py = (tmp_path / "app" / "app.py").read_text()
        assert "import test_project.pipelines" not in app_py
        assert "run_app" in app_py

        # app/pyproject.toml has the right deps and python version
        app_pyproject = (tmp_path / "app" / "pyproject.toml").read_text()
        assert "test_project" not in app_pyproject
        assert "databricks-bundle-decorators[app]" in app_pyproject
        assert 'requires-python = ">=3.12"' in app_pyproject

    def test_dashboard_flag_generates_app_yml(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.subprocess.run",
            lambda *a, **kw: None,
        )

        _cmd_init(dashboard=True)

        # resources/app.yml should exist (not resources/__init__.py with app code)
        app_yml = tmp_path / "resources" / "app.yml"
        assert app_yml.exists()
        content = app_yml.read_text()
        assert "resources:" in content
        assert "apps:" in content
        assert "test-project-observability" in content

        # resources/__init__.py should pass app_resource_key
        resources_init = (tmp_path / "resources" / "__init__.py").read_text()
        assert "generate_app_resource" not in resources_init
        assert 'app_resource_key="test_project_observability"' in resources_init

    def test_dashboard_flag_databricks_yaml_has_include(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.subprocess.run",
            lambda *a, **kw: None,
        )

        _cmd_init(dashboard=True)

        yaml_content = (tmp_path / "databricks.yaml").read_text()
        assert "include:" in yaml_content
        assert "resources/*.yml" in yaml_content

    def test_dashboard_flag_without_extra_exits(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)

        # Hide the dash module to simulate missing extra
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.importlib.import_module",
            _raise_import_error,
        )

        with pytest.raises(SystemExit):
            _cmd_init(dashboard=True)

    def test_default_init_no_app_files(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)

        _cmd_init()

        assert not (tmp_path / "app").exists()
        resources_content = (tmp_path / "resources" / "__init__.py").read_text()
        assert "generate_app_resource" not in resources_content

    def test_dashboard_hints_include_for_existing_yaml(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture,
    ):
        """When databricks.yaml already exists without include, print a hint."""
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.subprocess.run",
            lambda *a, **kw: None,
        )

        # Pre-create databricks.yaml without include
        (tmp_path / "databricks.yaml").write_text("bundle:\n  name: test\n")

        _cmd_init(dashboard=True)

        out = capsys.readouterr().out
        assert "resources/*.yml" in out


class TestMainCli:
    def test_init_subcommand(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """main() dispatches the init subcommand."""
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, "argv", ["dbxdec", "init"])

        main()

        assert (tmp_path / "databricks.yaml").exists()

    def test_init_docker_subcommand(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """main() dispatches the init --docker subcommand."""
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, "argv", ["dbxdec", "init", "--docker"])

        main()

        assert (tmp_path / "databricks.yaml").exists()
        content = (
            tmp_path / "src" / "test_project" / "pipelines" / "example.py"
        ).read_text()
        assert "libraries=[]" in content

    def test_init_dashboard_subcommand(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """main() dispatches the init --dashboard subcommand."""
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, "argv", ["dbxdec", "init", "--dashboard"])
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        main()

        assert (tmp_path / "app" / "app.py").exists()
        assert (tmp_path / "app" / "app.yaml").exists()
        assert (tmp_path / "app" / "registry.json").exists()
        assert (tmp_path / "resources" / "app.yml").exists()

    def test_no_subcommand_exits(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(sys, "argv", ["dbxdec"])

        with pytest.raises(SystemExit):
            main()

    def test_main_prints_error_on_exception(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ):
        """main() prints the exception message instead of silently exiting."""
        monkeypatch.setattr(sys, "argv", ["dbxdec", "backfill", "some_job"])
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: (_ for _ in ()).throw(RuntimeError("boom")),
        )

        with pytest.raises(SystemExit):
            main()

        err = capsys.readouterr().err
        assert "boom" in err

    @staticmethod
    def _make_project(tmp_path: Path) -> None:
        (tmp_path / "pyproject.toml").write_text(
            '[project]\nname = "test-project"\nversion = "0.1.0"\n'
        )


# ---------------------------------------------------------------------------
# Backfill CLI tests
# ---------------------------------------------------------------------------


class TestBackfillCmd:
    """Tests for the ``dbxdec backfill`` subcommand."""

    def setup_method(self):
        reset_registries()

    def _make_job_with_partition(self):
        """Register a job with a daily backfill in the registry."""

        @job(backfill=DailyBackfill(start_date="2024-01-01", end_date="2024-01-05"))
        def test_pipeline():
            @task
            def step():
                pass

    def _make_job_without_partition(self):
        @job
        def no_part_job():
            @task
            def step():
                pass

    def test_dry_run_lists_keys(self, monkeypatch, capsys):
        """--dry-run prints partition keys without submitting."""
        self._make_job_with_partition()

        # Monkeypatch discover_pipelines to no-op (registry already populated)
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        _cmd_backfill(
            job_name="test_pipeline",
            dry_run=True,
        )

        out = capsys.readouterr().out
        assert "test_pipeline" in out
        assert "2024-01-01" in out
        assert "DRY RUN" in out

    def test_dry_run_with_explicit_keys(self, monkeypatch, capsys):
        """--keys provides explicit partition keys."""
        self._make_job_with_partition()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        _cmd_backfill(
            job_name="test_pipeline",
            keys="a,b,c",
            dry_run=True,
        )

        out = capsys.readouterr().out
        assert "a" in out
        assert "b" in out
        assert "DRY RUN" in out

    def test_dry_run_with_range_override(self, monkeypatch, capsys):
        """--start/--end overrides the partition definition range."""
        self._make_job_with_partition()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        _cmd_backfill(
            job_name="test_pipeline",
            start="2024-01-02",
            end="2024-01-03",
            dry_run=True,
        )

        out = capsys.readouterr().out
        assert "2024-01-02" in out
        assert "2024-01-03" in out
        assert "Backfill keys (2)" in out

    def test_job_not_found_exits(self, monkeypatch):
        """Exit with error when job name is not in the registry."""
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        with pytest.raises(SystemExit):
            _cmd_backfill(job_name="nonexistent")

    def test_job_not_found_empty_registry_hint(self, monkeypatch, capsys):
        """When no jobs are discovered at all, show a helpful hint."""
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        with pytest.raises(SystemExit):
            _cmd_backfill(job_name="nonexistent")

        err = capsys.readouterr().err
        assert "No jobs were discovered" in err
        assert "entry point" in err

    def test_no_partition_no_keys_exits(self, monkeypatch):
        """Exit when job has no backfill definition and --keys is not provided."""
        self._make_job_without_partition()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        with pytest.raises(SystemExit):
            _cmd_backfill(job_name="no_part_job")

    def test_explicit_keys_on_unpartitioned_job(self, monkeypatch, capsys):
        """--keys works even when job has no backfill definition."""
        self._make_job_without_partition()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        _cmd_backfill(
            job_name="no_part_job",
            keys="x,y",
            dry_run=True,
        )

        out = capsys.readouterr().out
        assert "x" in out
        assert "DRY RUN" in out

    def test_empty_keys_exits(self, monkeypatch):
        """Exit when --keys resolves to empty list."""
        self._make_job_with_partition()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        with pytest.raises(SystemExit):
            _cmd_backfill(
                job_name="test_pipeline",
                keys=",,,",
            )

    def test_submit_runs_via_bundle_run(self, monkeypatch, capsys):
        """Non-dry-run submits via ``databricks bundle run``."""
        self._make_job_with_partition()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/databricks")

        calls: list[list[str]] = []

        def _fake_run(cmd, *, capture_output=False, text=False):
            calls.append(list(cmd))
            return subprocess.CompletedProcess(cmd, 0, stdout="Run submitted\n")

        monkeypatch.setattr("subprocess.run", _fake_run)

        _cmd_backfill(
            job_name="test_pipeline",
            keys="2024-01-01,2024-01-02",
        )

        assert len(calls) == 2
        # Each call should use 'databricks bundle run'
        for call in calls:
            assert call[:4] == ["databricks", "bundle", "run", "test_pipeline"]
            assert "--no-wait" in call
            assert any("backfill_key=" in arg for arg in call)

        out = capsys.readouterr().out
        assert "Submitted 2/2" in out

    def test_submit_with_target_and_profile(self, monkeypatch, capsys):
        """--target and --profile are forwarded to ``databricks bundle run``."""
        self._make_job_with_partition()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/databricks")

        calls: list[list[str]] = []

        def _fake_run(cmd, *, capture_output=False, text=False):
            calls.append(list(cmd))
            return subprocess.CompletedProcess(cmd, 0, stdout="OK\n")

        monkeypatch.setattr("subprocess.run", _fake_run)

        _cmd_backfill(
            job_name="test_pipeline",
            keys="2024-01-01",
            target="dev",
            profile="myprofile",
        )

        assert len(calls) == 1
        cmd = calls[0]
        assert "--target" in cmd
        assert cmd[cmd.index("--target") + 1] == "dev"
        assert "--profile" in cmd
        assert cmd[cmd.index("--profile") + 1] == "myprofile"

    def test_wait_omits_no_wait_flag(self, monkeypatch, capsys):
        """--wait causes ``databricks bundle run`` to block (no --no-wait)."""
        self._make_job_with_partition()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/databricks")

        calls: list[list[str]] = []

        def _fake_run(cmd, *, capture_output=False, text=False):
            calls.append(list(cmd))
            return subprocess.CompletedProcess(cmd, 0, stdout="SUCCESS\n")

        monkeypatch.setattr("subprocess.run", _fake_run)

        _cmd_backfill(
            job_name="test_pipeline",
            keys="2024-01-01",
            wait=True,
        )

        assert len(calls) == 1
        assert "--no-wait" not in calls[0]

        out = capsys.readouterr().out
        assert "Completed 1/1" in out

    def test_missing_databricks_cli_exits(self, monkeypatch):
        """Exit with error when databricks CLI is not on PATH."""
        self._make_job_with_partition()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        monkeypatch.setattr("shutil.which", lambda _name: None)

        with pytest.raises(SystemExit):
            _cmd_backfill(
                job_name="test_pipeline",
                keys="2024-01-01",
            )


# ---------------------------------------------------------------------------
# Catchup CLI tests
# ---------------------------------------------------------------------------


class TestBackfillCatchupCmd:
    """Tests for the ``dbxdec catchup`` subcommand."""

    def setup_method(self):
        reset_registries()

    def _make_job_with_backfill(self):
        """Register a job with a daily backfill (5 days)."""

        @job(backfill=DailyBackfill(start_date="2024-01-01", end_date="2024-01-05"))
        def test_pipeline():
            @task
            def step():
                pass

    def _make_job_without_backfill(self):
        @job
        def no_backfill_job():
            @task
            def step():
                pass

    @staticmethod
    def _fake_bundle_summary(job_id: str = "12345"):
        """Return a fake subprocess handler for ``databricks bundle summary``."""
        summary = {
            "resources": {
                "jobs": {
                    "test_pipeline": {"id": job_id},
                }
            }
        }

        def handler(cmd, *, capture_output=False, text=False):
            if "bundle" in cmd and "summary" in cmd:
                return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(summary))
            raise AssertionError(f"Unexpected command: {cmd}")

        return handler

    @staticmethod
    def _fake_list_runs(runs: list[dict]):
        """Return a fake subprocess handler for ``databricks jobs list-runs``."""

        def handler(cmd, *, capture_output=False, text=False):
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout=json.dumps(runs),
            )

        return handler

    @staticmethod
    def _combined_handler(*handlers):
        """Combine multiple fake subprocess handlers by dispatch."""

        def handler(cmd, **kwargs):
            if "bundle" in cmd and "summary" in cmd:
                return handlers[0](cmd, **kwargs)
            if "list-runs" in cmd:
                return handlers[1](cmd, **kwargs)
            if "bundle" in cmd and "run" in cmd:
                return handlers[2](cmd, **kwargs) if len(handlers) > 2 else None
            raise AssertionError(f"Unexpected command: {cmd}")

        return handler

    def test_dry_run_shows_missing_keys(self, monkeypatch, capsys):
        """--dry-run shows missing keys without submitting."""
        self._make_job_with_backfill()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/databricks")

        # Simulate: 2024-01-01 and 2024-01-03 already succeeded
        runs = [
            {
                "state": {"result_state": "SUCCESS"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-01"}],
            },
            {
                "state": {"result_state": "SUCCESS"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-03"}],
            },
        ]

        monkeypatch.setattr(
            "subprocess.run",
            self._combined_handler(
                self._fake_bundle_summary(),
                self._fake_list_runs(runs),
            ),
        )

        _cmd_backfill_catchup(job_name="test_pipeline", dry_run=True)

        out = capsys.readouterr().out
        assert "All backfill keys: 5" in out
        assert "Already launched: 2" in out
        assert "Missing: 3" in out
        assert "DRY RUN" in out

    def test_active_runs_are_not_relaunched(self, monkeypatch, capsys):
        """Active (in-flight) runs should count as launched."""
        self._make_job_with_backfill()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/databricks")

        # 2024-01-01 succeeded, 2024-01-02 is still running (no result_state)
        runs = [
            {
                "state": {"result_state": "SUCCESS"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-01"}],
            },
            {
                "state": {"life_cycle_state": "RUNNING"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-02"}],
            },
        ]

        monkeypatch.setattr(
            "subprocess.run",
            self._combined_handler(
                self._fake_bundle_summary(),
                self._fake_list_runs(runs),
            ),
        )

        _cmd_backfill_catchup(job_name="test_pipeline", dry_run=True)

        out = capsys.readouterr().out
        assert "Already launched: 2" in out
        assert "Missing: 3" in out

    def test_failed_runs_are_retried(self, monkeypatch, capsys):
        """Terminally failed runs should NOT count as launched."""
        self._make_job_with_backfill()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/databricks")

        runs = [
            {
                "state": {"result_state": "SUCCESS"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-01"}],
            },
            {
                "state": {"result_state": "FAILED"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-02"}],
            },
            {
                "state": {"result_state": "CANCELED"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-03"}],
            },
        ]

        monkeypatch.setattr(
            "subprocess.run",
            self._combined_handler(
                self._fake_bundle_summary(),
                self._fake_list_runs(runs),
            ),
        )

        _cmd_backfill_catchup(job_name="test_pipeline", dry_run=True)

        out = capsys.readouterr().out
        # Only 2024-01-01 is launched; failed and canceled should be retried
        assert "Already launched: 1" in out
        assert "Missing: 4" in out

    def test_all_complete_prints_done(self, monkeypatch, capsys):
        """When all keys are launched, print completion message."""
        self._make_job_with_backfill()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/databricks")

        runs = [
            {
                "state": {"result_state": "SUCCESS"},
                "job_parameters": [{"name": "backfill_key", "value": f"2024-01-0{i}"}],
            }
            for i in range(1, 6)
        ]

        monkeypatch.setattr(
            "subprocess.run",
            self._combined_handler(
                self._fake_bundle_summary(),
                self._fake_list_runs(runs),
            ),
        )

        _cmd_backfill_catchup(job_name="test_pipeline")

        out = capsys.readouterr().out
        assert "All backfill keys have been completed" in out

    def test_no_backfill_def_exits(self, monkeypatch):
        """Exit when job has no backfill definition."""
        self._make_job_without_backfill()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        with pytest.raises(SystemExit):
            _cmd_backfill_catchup(job_name="no_backfill_job")

    def test_job_not_found_exits(self, monkeypatch):
        """Exit when job name is not in the registry."""
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        with pytest.raises(SystemExit):
            _cmd_backfill_catchup(job_name="nonexistent")

    def test_job_not_found_empty_registry_hint(self, monkeypatch, capsys):
        """When no jobs are discovered at all, show a helpful hint."""
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        with pytest.raises(SystemExit):
            _cmd_backfill_catchup(job_name="nonexistent")

        err = capsys.readouterr().err
        assert "No jobs were discovered" in err
        assert "entry point" in err

    def test_submits_missing_keys(self, monkeypatch, capsys):
        """Non-dry-run submits only missing keys via ``databricks bundle run``."""
        self._make_job_with_backfill()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/databricks")

        # 2024-01-01, 2024-01-02 already done
        runs = [
            {
                "state": {"result_state": "SUCCESS"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-01"}],
            },
            {
                "state": {"result_state": "SUCCESS"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-02"}],
            },
        ]

        submitted_keys: list[str] = []

        def _combined(cmd, *, capture_output=False, text=False):
            if "bundle" in cmd and "summary" in cmd:
                return self._fake_bundle_summary()(
                    cmd, capture_output=capture_output, text=text
                )
            if "list-runs" in cmd:
                return self._fake_list_runs(runs)(
                    cmd, capture_output=capture_output, text=text
                )
            if "bundle" in cmd and "run" in cmd:
                # Extract the backfill_key from --params
                submitted_keys.extend(
                    arg.split("=", 1)[1]
                    for arg in cmd
                    if arg.startswith("backfill_key=")
                )
                return subprocess.CompletedProcess(cmd, 0, stdout="Run submitted\n")
            raise AssertionError(f"Unexpected command: {cmd}")

        monkeypatch.setattr("subprocess.run", _combined)

        _cmd_backfill_catchup(job_name="test_pipeline")

        out = capsys.readouterr().out
        assert "Submitted 3/3" in out
        assert sorted(submitted_keys) == ["2024-01-03", "2024-01-04", "2024-01-05"]

    def test_target_and_profile_forwarded(self, monkeypatch, capsys):
        """--target and --profile are passed to all CLI calls."""
        self._make_job_with_backfill()

        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/databricks")

        all_cmds: list[list[str]] = []

        # All keys already done
        runs = [
            {
                "state": {"result_state": "SUCCESS"},
                "job_parameters": [{"name": "backfill_key", "value": f"2024-01-0{i}"}],
            }
            for i in range(1, 6)
        ]

        def _spy(cmd, *, capture_output=False, text=False):
            all_cmds.append(list(cmd))
            if "bundle" in cmd and "summary" in cmd:
                return self._fake_bundle_summary()(
                    cmd, capture_output=capture_output, text=text
                )
            if "list-runs" in cmd:
                return self._fake_list_runs(runs)(
                    cmd, capture_output=capture_output, text=text
                )
            raise AssertionError(f"Unexpected command: {cmd}")

        monkeypatch.setattr("subprocess.run", _spy)

        _cmd_backfill_catchup(
            job_name="test_pipeline",
            target="prod",
            profile="myprofile",
        )

        # bundle summary should have --target and --profile
        summary_cmd = all_cmds[0]
        assert "--target" in summary_cmd
        assert summary_cmd[summary_cmd.index("--target") + 1] == "prod"
        assert "--profile" in summary_cmd
        assert summary_cmd[summary_cmd.index("--profile") + 1] == "myprofile"

        # list-runs should have --profile
        list_cmd = all_cmds[1]
        assert "--profile" in list_cmd
        assert list_cmd[list_cmd.index("--profile") + 1] == "myprofile"


class TestGetLaunchedBackfillKeys:
    """Unit tests for _get_launched_backfill_keys logic."""

    def test_includes_active_runs(self, monkeypatch):
        """Active runs (no result_state) are counted as launched."""
        runs = [
            {
                "state": {"life_cycle_state": "RUNNING"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-01"}],
            },
            {
                "state": {"life_cycle_state": "PENDING"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-02"}],
            },
        ]

        monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/databricks")
        monkeypatch.setattr(
            "subprocess.run",
            lambda cmd, **kw: subprocess.CompletedProcess(
                cmd, 0, stdout=json.dumps(runs)
            ),
        )

        result = _get_launched_backfill_keys("123", None, None)
        assert result == {"2024-01-01", "2024-01-02"}

    def test_excludes_failed_runs(self, monkeypatch):
        """Failed/canceled runs are not counted as launched."""
        runs = [
            {
                "state": {"result_state": "SUCCESS"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-01"}],
            },
            {
                "state": {"result_state": "FAILED"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-02"}],
            },
            {
                "state": {"result_state": "CANCELED"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-03"}],
            },
            {
                "state": {"result_state": "TIMED_OUT"},
                "job_parameters": [{"name": "backfill_key", "value": "2024-01-04"}],
            },
        ]

        monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/databricks")
        monkeypatch.setattr(
            "subprocess.run",
            lambda cmd, **kw: subprocess.CompletedProcess(
                cmd, 0, stdout=json.dumps(runs)
            ),
        )

        result = _get_launched_backfill_keys("123", None, None)
        assert result == {"2024-01-01"}


class TestReadAppNameFromYml:
    """Tests for _read_app_name_from_yml."""

    def test_returns_none_when_file_missing(self, tmp_path: Path) -> None:
        assert _read_app_name_from_yml(tmp_path) is None

    def test_reads_name_from_existing_yml(self, tmp_path: Path) -> None:
        (tmp_path / "resources").mkdir()
        (tmp_path / "resources" / "app.yml").write_text(
            "resources:\n"
            "  apps:\n"
            "    my_custom_app:\n"
            "      name: my-custom-app\n"
            "      description: Pipeline observability dashboard\n"
        )

        assert _read_app_name_from_yml(tmp_path) == "my-custom-app"

    def test_reads_user_edited_name(self, tmp_path: Path) -> None:
        (tmp_path / "resources").mkdir()
        (tmp_path / "resources" / "app.yml").write_text(
            "resources:\n"
            "  apps:\n"
            "    my_custom_name:\n"
            "      name: totally-renamed-app\n"
            "      description: Pipeline observability dashboard\n"
        )

        assert _read_app_name_from_yml(tmp_path) == "totally-renamed-app"


class TestAppConfigNameResolution:
    """Tests for app_config --name flag and existing name preservation."""

    def _make_project(self, tmp_path: Path, name: str = "test-project") -> None:
        (tmp_path / "pyproject.toml").write_text(
            f'[project]\nname = "{name}"\nversion = "0.1.0"\n'
        )

    def test_uses_name_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        app_config(permission="CAN_VIEW", name="custom-name")

        content = (tmp_path / "resources" / "app.yml").read_text()
        assert "name: custom-name" in content

    def test_preserves_existing_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        # Pre-create app.yml with a user-edited name
        (tmp_path / "resources").mkdir(parents=True)
        (tmp_path / "resources" / "app.yml").write_text(
            "resources:\n"
            "  apps:\n"
            "    user_app:\n"
            "      name: user-edited-name\n"
            "      description: Pipeline observability dashboard\n"
        )

        app_config(permission="CAN_VIEW", name=None)

        content = (tmp_path / "resources" / "app.yml").read_text()
        assert "name: user-edited-name" in content
        assert "test-project-observability" not in content

    def test_falls_back_to_pyproject_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )

        app_config(permission="CAN_VIEW", name=None)

        content = (tmp_path / "resources" / "app.yml").read_text()
        assert "name: test-project-observability" in content

    def test_name_flag_overrides_existing_yml(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "databricks_bundle_decorators.cli.discover_pipelines",
            lambda: None,
        )
        # Pre-create app.yml with one name
        (tmp_path / "resources").mkdir(parents=True)
        (tmp_path / "resources" / "app.yml").write_text(
            "resources:\n"
            "  apps:\n"
            "    old_app:\n"
            "      name: old-app\n"
            "      description: Pipeline observability dashboard\n"
        )

        app_config(permission="CAN_VIEW", name="new-name")

        content = (tmp_path / "resources" / "app.yml").read_text()
        assert "name: new-name" in content
        assert "old-app" not in content
