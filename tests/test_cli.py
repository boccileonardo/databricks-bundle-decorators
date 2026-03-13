"""Tests for the CLI scaffolding command (dbxdec init)."""

import sys
from pathlib import Path

import pytest

from databricks_bundle_decorators.cli import (
    _cmd_init,
    _cmd_backfill,
    _detect_package_name,
    _detect_src_layout,
    _read_pyproject,
)


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


class TestMainCli:
    def test_init_subcommand(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """main() dispatches the init subcommand."""
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, "argv", ["dbxdec", "init"])
        from databricks_bundle_decorators.cli import main

        main()

        assert (tmp_path / "databricks.yaml").exists()

    def test_init_docker_subcommand(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """main() dispatches the init --docker subcommand."""
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, "argv", ["dbxdec", "init", "--docker"])
        from databricks_bundle_decorators.cli import main

        main()

        assert (tmp_path / "databricks.yaml").exists()
        content = (
            tmp_path / "src" / "test_project" / "pipelines" / "example.py"
        ).read_text()
        assert "libraries=[]" in content

    def test_no_subcommand_exits(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(sys, "argv", ["dbxdec"])
        from databricks_bundle_decorators.cli import main

        with pytest.raises(SystemExit):
            main()

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
        from databricks_bundle_decorators.registry import (
            reset_registries,
        )

        reset_registries()

    def _make_job_with_partition(self):
        """Register a job with a daily partition in the registry."""
        from databricks_bundle_decorators.decorators import job, task
        from databricks_bundle_decorators.partitions import DailyPartition

        @job(partition=DailyPartition(start_date="2024-01-01", end_date="2024-01-05"))
        def test_pipeline():
            @task
            def step():
                pass

    def _make_job_without_partition(self):
        from databricks_bundle_decorators.decorators import job, task

        @job
        def no_part_job():
            @task
            def step():
                pass

    def test_dry_run_lists_keys(self, monkeypatch, capsys):
        """--dry-run prints partition keys without submitting."""
        self._make_job_with_partition()

        from databricks_bundle_decorators.cli import _cmd_backfill

        # Monkeypatch discover_pipelines to no-op (registry already populated)
        monkeypatch.setattr(
            "databricks_bundle_decorators.discovery.discover_pipelines",
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
            "databricks_bundle_decorators.discovery.discover_pipelines",
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
            "databricks_bundle_decorators.discovery.discover_pipelines",
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
        assert "Partition keys (2)" in out

    def test_job_not_found_exits(self, monkeypatch):
        """Exit with error when job name is not in the registry."""
        monkeypatch.setattr(
            "databricks_bundle_decorators.discovery.discover_pipelines",
            lambda: None,
        )

        with pytest.raises(SystemExit):
            from databricks_bundle_decorators.cli import _cmd_backfill

            _cmd_backfill(job_name="nonexistent")

    def test_no_partition_no_keys_exits(self, monkeypatch):
        """Exit when job has no partition and --keys is not provided."""
        self._make_job_without_partition()

        monkeypatch.setattr(
            "databricks_bundle_decorators.discovery.discover_pipelines",
            lambda: None,
        )

        with pytest.raises(SystemExit):
            from databricks_bundle_decorators.cli import _cmd_backfill

            _cmd_backfill(job_name="no_part_job")

    def test_explicit_keys_on_unpartitioned_job(self, monkeypatch, capsys):
        """--keys works even when job has no partition definition."""
        self._make_job_without_partition()

        monkeypatch.setattr(
            "databricks_bundle_decorators.discovery.discover_pipelines",
            lambda: None,
        )

        from databricks_bundle_decorators.cli import _cmd_backfill

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
            "databricks_bundle_decorators.discovery.discover_pipelines",
            lambda: None,
        )

        with pytest.raises(SystemExit):
            from databricks_bundle_decorators.cli import _cmd_backfill

            _cmd_backfill(
                job_name="test_pipeline",
                keys=",,,",
            )
