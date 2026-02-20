"""Tests for PolarsParquetIoManager with mocked polars."""

from __future__ import annotations

import sys
import types
from typing import Any
from unittest.mock import MagicMock

import pytest

from databricks_bundle_decorators.io_manager import InputContext, OutputContext


# ---------------------------------------------------------------------------
# Helpers – create a fake ``polars`` module so the IoManager can be imported
# and exercised without a real polars installation.
# ---------------------------------------------------------------------------


def _make_mock_polars() -> types.ModuleType:
    """Build a minimal mock ``polars`` module with DataFrame and LazyFrame."""
    mod = types.ModuleType("polars")

    class _DataFrame:
        """Fake polars.DataFrame."""

        def __init__(self, data: Any = None) -> None:
            self.data = data

        write_parquet = MagicMock()

    class _LazyFrame:
        """Fake polars.LazyFrame."""

        def __init__(self, data: Any = None) -> None:
            self.data = data

        sink_parquet = MagicMock()

    mod.DataFrame = _DataFrame  # type: ignore[attr-defined]
    mod.LazyFrame = _LazyFrame  # type: ignore[attr-defined]
    mod.read_parquet = MagicMock(return_value=_DataFrame({"col": [1, 2]}))  # type: ignore[attr-defined]
    mod.scan_parquet = MagicMock(return_value=_LazyFrame({"col": [1, 2]}))  # type: ignore[attr-defined]

    return mod


@pytest.fixture(autouse=True)
def _mock_polars(monkeypatch: pytest.MonkeyPatch):
    """Inject a fake ``polars`` module for every test in this file."""
    mock_pl = _make_mock_polars()
    monkeypatch.setitem(sys.modules, "polars", mock_pl)
    # Reset call tracking between tests
    mock_pl.DataFrame.write_parquet.reset_mock()  # type: ignore[union-attr]
    mock_pl.LazyFrame.sink_parquet.reset_mock()  # type: ignore[union-attr]
    mock_pl.read_parquet.reset_mock()  # type: ignore[union-attr]
    mock_pl.scan_parquet.reset_mock()  # type: ignore[union-attr]
    yield mock_pl


def _output_ctx(task_key: str = "my_task") -> OutputContext:
    return OutputContext(job_name="j", task_key=task_key, run_id="r1")


def _input_ctx(
    upstream: str = "producer",
    expected_type: type | None = None,
) -> InputContext:
    return InputContext(
        job_name="j",
        task_key="consumer",
        upstream_task_key=upstream,
        run_id="r1",
        expected_type=expected_type,
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_strips_trailing_slash(self):
        from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

        io = PolarsParquetIoManager(base_path="/data/lake/")
        assert io.base_path == "/data/lake"

    def test_storage_options_default_none(self):
        from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

        io = PolarsParquetIoManager(base_path="/data")
        assert io.storage_options is None

    def test_uri_generation(self):
        from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

        io = PolarsParquetIoManager(base_path="s3://bucket/prefix")
        assert io._uri("extract") == "s3://bucket/prefix/extract.parquet"


# ---------------------------------------------------------------------------
# Write dispatch
# ---------------------------------------------------------------------------


class TestWrite:
    def test_write_dataframe_calls_write_parquet(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

        io = PolarsParquetIoManager(
            base_path="/data",
            storage_options={"key": "val"},
        )
        df = _mock_polars.DataFrame()

        io.write(_output_ctx("extract"), df)

        df.write_parquet.assert_called_once_with(
            "/data/extract.parquet",
            storage_options={"key": "val"},
        )

    def test_write_lazyframe_calls_sink_parquet(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

        io = PolarsParquetIoManager(
            base_path="/data",
            storage_options={"key": "val"},
        )
        lf = _mock_polars.LazyFrame()

        io.write(_output_ctx("extract"), lf)

        lf.sink_parquet.assert_called_once_with(
            "/data/extract.parquet",
            storage_options={"key": "val"},
        )

    def test_write_unsupported_type_raises(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

        io = PolarsParquetIoManager(base_path="/data")

        with pytest.raises(TypeError, match="got dict"):
            io.write(_output_ctx(), {"not": "a dataframe"})

    def test_write_none_storage_options(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

        io = PolarsParquetIoManager(base_path="/data")
        df = _mock_polars.DataFrame()

        io.write(_output_ctx("t"), df)

        df.write_parquet.assert_called_once_with(
            "/data/t.parquet",
            storage_options=None,
        )


# ---------------------------------------------------------------------------
# Read dispatch
# ---------------------------------------------------------------------------


class TestRead:
    def test_read_defaults_to_scan(self, _mock_polars):
        """Unannotated parameter → scan_parquet (lazy)."""
        from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

        io = PolarsParquetIoManager(
            base_path="/data",
            storage_options={"key": "val"},
        )

        result = io.read(_input_ctx("upstream_task", expected_type=None))

        _mock_polars.scan_parquet.assert_called_once_with(
            "/data/upstream_task.parquet",
            storage_options={"key": "val"},
        )
        assert isinstance(result, _mock_polars.LazyFrame)

    def test_read_lazyframe_annotation_uses_scan(self, _mock_polars):
        """Explicit LazyFrame annotation → scan_parquet."""
        from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

        io = PolarsParquetIoManager(base_path="/data")

        result = io.read(
            _input_ctx("upstream_task", expected_type=_mock_polars.LazyFrame)
        )

        _mock_polars.scan_parquet.assert_called_once()
        assert isinstance(result, _mock_polars.LazyFrame)

    def test_read_dataframe_annotation_uses_read(self, _mock_polars):
        """Explicit DataFrame annotation → read_parquet (eager)."""
        from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

        io = PolarsParquetIoManager(
            base_path="/data",
            storage_options={"key": "val"},
        )

        result = io.read(
            _input_ctx("upstream_task", expected_type=_mock_polars.DataFrame)
        )

        _mock_polars.read_parquet.assert_called_once_with(
            "/data/upstream_task.parquet",
            storage_options={"key": "val"},
        )
        _mock_polars.scan_parquet.assert_not_called()
        assert isinstance(result, _mock_polars.DataFrame)

    def test_read_none_storage_options(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsParquetIoManager

        io = PolarsParquetIoManager(base_path="/data")

        io.read(_input_ctx("t"))

        _mock_polars.scan_parquet.assert_called_once_with(
            "/data/t.parquet",
            storage_options=None,
        )
