"""Tests for the CLI scaffolding command (dbxdec init)."""

import sys
from pathlib import Path

import pytest

from databricks_bundle_decorators.cli import (
    _cmd_init,
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
        import argparse

        _cmd_init(argparse.Namespace())

        assert (tmp_path / "resources" / "__init__.py").exists()
        assert (
            tmp_path / "src" / "test_project" / "pipelines" / "__init__.py"
        ).exists()
        assert (tmp_path / "src" / "test_project" / "pipelines" / "example.py").exists()
        assert (tmp_path / "databricks.yaml").exists()
        assert (tmp_path / "src" / "test_project" / "__init__.py").exists()

    def test_skips_existing_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)

        # Pre-create a file that init would create
        resources_dir = tmp_path / "resources"
        resources_dir.mkdir()
        (resources_dir / "__init__.py").write_text("# existing")

        import argparse

        _cmd_init(argparse.Namespace())

        # Should not overwrite
        assert (resources_dir / "__init__.py").read_text() == "# existing"
        # But other files should still be created
        assert (tmp_path / "databricks.yaml").exists()

    def test_databricks_yaml_contains_project_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        self._make_project(tmp_path, "my-pipeline")
        monkeypatch.chdir(tmp_path)
        import argparse

        _cmd_init(argparse.Namespace())

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
        import argparse

        _cmd_init(argparse.Namespace())

        captured = capsys.readouterr()
        assert "entry-points" in captured.out or "entry point" in captured.out

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
        import argparse

        _cmd_init(argparse.Namespace())

        captured = capsys.readouterr()
        assert "Next step" not in captured.out


class TestMainCli:
    def test_init_subcommand(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """main() dispatches the init subcommand."""
        self._make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, "argv", ["dbxdec", "init"])
        from databricks_bundle_decorators.cli import main

        main()

        assert (tmp_path / "databricks.yaml").exists()

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
