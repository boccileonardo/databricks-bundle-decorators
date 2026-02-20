"""Tests for PolarsDeltaIoManager with mocked polars."""

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

        write_delta = MagicMock()

    class _LazyFrame:
        """Fake polars.LazyFrame."""

        def __init__(self, data: Any = None) -> None:
            self.data = data

        sink_delta = MagicMock()

    mod.DataFrame = _DataFrame  # type: ignore[attr-defined]
    mod.LazyFrame = _LazyFrame  # type: ignore[attr-defined]
    mod.read_delta = MagicMock(return_value=_DataFrame({"col": [1, 2]}))  # type: ignore[attr-defined]
    mod.scan_delta = MagicMock(return_value=_LazyFrame({"col": [1, 2]}))  # type: ignore[attr-defined]

    return mod


@pytest.fixture(autouse=True)
def _mock_polars(monkeypatch: pytest.MonkeyPatch):
    """Inject a fake ``polars`` module for every test in this file."""
    mock_pl = _make_mock_polars()
    monkeypatch.setitem(sys.modules, "polars", mock_pl)
    # Reset call tracking between tests
    mock_pl.DataFrame.write_delta.reset_mock()
    mock_pl.LazyFrame.sink_delta.reset_mock()
    mock_pl.read_delta.reset_mock()
    mock_pl.scan_delta.reset_mock()
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
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(base_path="/data/lake/")
        assert io.base_path == "/data/lake"

    def test_storage_options_default_none(self):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(base_path="/data")
        assert io.storage_options is None

    def test_storage_options_as_dict(self):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        opts = {
            "AZURE_STORAGE_ACCOUNT_NAME": "sa",
            "AZURE_STORAGE_ACCOUNT_KEY": "secret",
        }
        io = PolarsDeltaIoManager(base_path="/data", storage_options=opts)
        assert io.storage_options == opts

    def test_storage_options_as_callable(self):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        opts = {
            "AZURE_STORAGE_ACCOUNT_NAME": "sa",
            "AZURE_STORAGE_ACCOUNT_KEY": "secret",
        }
        io = PolarsDeltaIoManager(base_path="/data", storage_options=lambda: opts)
        assert io.storage_options == opts

    def test_storage_options_callable_invoked_each_time(self):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        call_count = 0

        def _factory() -> dict[str, str]:
            nonlocal call_count
            call_count += 1
            return {"key": str(call_count)}

        io = PolarsDeltaIoManager(base_path="/data", storage_options=_factory)
        assert io.storage_options == {"key": "1"}
        assert io.storage_options == {"key": "2"}
        assert call_count == 2

    def test_uri_generation_no_extension(self):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(base_path="s3://bucket/prefix")
        assert io._uri("extract") == "s3://bucket/prefix/extract"


# ---------------------------------------------------------------------------
# Write dispatch
# ---------------------------------------------------------------------------


class TestWrite:
    def test_write_dataframe_calls_write_delta(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(
            base_path="/data",
            storage_options={"key": "val"},
        )
        df = _mock_polars.DataFrame()

        io.write(_output_ctx("extract"), df)

        df.write_delta.assert_called_once_with(
            "/data/extract",
            mode="error",
            storage_options={"key": "val"},
        )

    def test_write_lazyframe_calls_sink_delta(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(
            base_path="/data",
            storage_options={"key": "val"},
        )
        lf = _mock_polars.LazyFrame()

        io.write(_output_ctx("extract"), lf)

        lf.sink_delta.assert_called_once_with(
            "/data/extract",
            mode="error",
            storage_options={"key": "val"},
        )

    def test_write_unsupported_type_raises(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(base_path="/data")

        with pytest.raises(TypeError, match="got dict"):
            io.write(_output_ctx(), {"not": "a dataframe"})

    def test_write_none_storage_options(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(base_path="/data")
        df = _mock_polars.DataFrame()

        io.write(_output_ctx("t"), df)

        df.write_delta.assert_called_once_with(
            "/data/t",
            mode="error",
            storage_options=None,
        )

    def test_write_callable_storage_options(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        opts = {
            "AZURE_STORAGE_ACCOUNT_NAME": "sa",
            "AZURE_STORAGE_ACCOUNT_KEY": "secret",
        }
        io = PolarsDeltaIoManager(base_path="/data", storage_options=lambda: opts)
        df = _mock_polars.DataFrame()

        io.write(_output_ctx("t"), df)

        df.write_delta.assert_called_once_with(
            "/data/t",
            mode="error",
            storage_options=opts,
        )


# ---------------------------------------------------------------------------
# Read dispatch
# ---------------------------------------------------------------------------


class TestRead:
    def test_read_defaults_to_scan(self, _mock_polars):
        """Unannotated parameter → scan_delta (lazy)."""
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(
            base_path="/data",
            storage_options={"key": "val"},
        )

        result = io.read(_input_ctx("upstream_task", expected_type=None))

        _mock_polars.scan_delta.assert_called_once_with(
            "/data/upstream_task",
            storage_options={"key": "val"},
        )
        assert isinstance(result, _mock_polars.LazyFrame)

    def test_read_lazyframe_annotation_uses_scan(self, _mock_polars):
        """Explicit LazyFrame annotation → scan_delta."""
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(base_path="/data")

        result = io.read(
            _input_ctx("upstream_task", expected_type=_mock_polars.LazyFrame)
        )

        _mock_polars.scan_delta.assert_called_once()
        assert isinstance(result, _mock_polars.LazyFrame)

    def test_read_dataframe_annotation_uses_read(self, _mock_polars):
        """Explicit DataFrame annotation → read_delta (eager)."""
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(
            base_path="/data",
            storage_options={"key": "val"},
        )

        result = io.read(
            _input_ctx("upstream_task", expected_type=_mock_polars.DataFrame)
        )

        _mock_polars.read_delta.assert_called_once_with(
            "/data/upstream_task",
            storage_options={"key": "val"},
        )
        _mock_polars.scan_delta.assert_not_called()
        assert isinstance(result, _mock_polars.DataFrame)

    def test_read_none_storage_options(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(base_path="/data")

        io.read(_input_ctx("t"))

        _mock_polars.scan_delta.assert_called_once_with(
            "/data/t",
            storage_options=None,
        )

    def test_read_callable_storage_options(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        opts = {
            "AZURE_STORAGE_ACCOUNT_NAME": "sa",
            "AZURE_STORAGE_ACCOUNT_KEY": "secret",
        }
        io = PolarsDeltaIoManager(base_path="/data", storage_options=lambda: opts)

        io.read(_input_ctx("t"))

        _mock_polars.scan_delta.assert_called_once_with(
            "/data/t",
            storage_options=opts,
        )


# ---------------------------------------------------------------------------
# write_options / read_options passthrough
# ---------------------------------------------------------------------------


class TestWriteOptions:
    def test_write_options_forwarded_to_write_delta(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(
            base_path="/data",
            write_options={"delta_write_options": {"partition_by": ["region"]}},
        )
        df = _mock_polars.DataFrame()

        io.write(_output_ctx("t"), df)

        df.write_delta.assert_called_once_with(
            "/data/t",
            mode="error",
            storage_options=None,
            delta_write_options={"partition_by": ["region"]},
        )

    def test_write_options_forwarded_to_sink_delta(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(
            base_path="/data",
            write_options={"delta_write_options": {"partition_by": ["region"]}},
        )
        lf = _mock_polars.LazyFrame()

        io.write(_output_ctx("t"), lf)

        lf.sink_delta.assert_called_once_with(
            "/data/t",
            mode="error",
            storage_options=None,
            delta_write_options={"partition_by": ["region"]},
        )

    def test_write_options_default_empty(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(base_path="/data")
        assert io._write_options == {}


class TestReadOptions:
    def test_read_options_forwarded_to_scan_delta(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(
            base_path="/data",
            read_options={"version": 3},
        )

        io.read(_input_ctx("t"))

        _mock_polars.scan_delta.assert_called_once_with(
            "/data/t",
            storage_options=None,
            version=3,
        )

    def test_read_options_forwarded_to_read_delta(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(
            base_path="/data",
            read_options={"version": 3},
        )

        io.read(_input_ctx("t", expected_type=_mock_polars.DataFrame))

        _mock_polars.read_delta.assert_called_once_with(
            "/data/t",
            storage_options=None,
            version=3,
        )

    def test_read_options_default_empty(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(base_path="/data")
        assert io._read_options == {}


# ---------------------------------------------------------------------------
# mode parameter
# ---------------------------------------------------------------------------


class TestModeParameter:
    def test_default_mode_is_error(self):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(base_path="/data")
        assert io._mode == "error"

    def test_custom_mode_append_dataframe(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(base_path="/data", mode="append")
        df = _mock_polars.DataFrame()

        io.write(_output_ctx("t"), df)

        df.write_delta.assert_called_once_with(
            "/data/t",
            mode="append",
            storage_options=None,
        )

    def test_custom_mode_append_lazyframe(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(base_path="/data", mode="append")
        lf = _mock_polars.LazyFrame()

        io.write(_output_ctx("t"), lf)

        lf.sink_delta.assert_called_once_with(
            "/data/t",
            mode="append",
            storage_options=None,
        )

    def test_custom_mode_error(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        io = PolarsDeltaIoManager(base_path="/data", mode="error")
        df = _mock_polars.DataFrame()

        io.write(_output_ctx("t"), df)

        df.write_delta.assert_called_once_with(
            "/data/t",
            mode="error",
            storage_options=None,
        )


# ---------------------------------------------------------------------------
# TableMerger support
# ---------------------------------------------------------------------------


class TestTableMerger:
    def test_table_merger_calls_execute(self, _mock_polars, monkeypatch):
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        # Create a fake deltalake.table module with a TableMerger class.
        deltalake_mod = types.ModuleType("deltalake")
        table_mod = types.ModuleType("deltalake.table")

        class _FakeTableMerger:
            execute = MagicMock()

        table_mod.TableMerger = _FakeTableMerger  # type: ignore[attr-defined]
        deltalake_mod.table = table_mod  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "deltalake", deltalake_mod)
        monkeypatch.setitem(sys.modules, "deltalake.table", table_mod)

        io = PolarsDeltaIoManager(base_path="/data")
        merger = _FakeTableMerger()

        io.write(_output_ctx("t"), merger)

        merger.execute.assert_called_once()

    def test_table_merger_skips_dataframe_write(self, _mock_polars, monkeypatch):
        """When a TableMerger is written, DataFrame write_delta is NOT called."""
        from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

        deltalake_mod = types.ModuleType("deltalake")
        table_mod = types.ModuleType("deltalake.table")

        class _FakeTableMerger:
            execute = MagicMock()

        table_mod.TableMerger = _FakeTableMerger  # type: ignore[attr-defined]
        deltalake_mod.table = table_mod  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "deltalake", deltalake_mod)
        monkeypatch.setitem(sys.modules, "deltalake.table", table_mod)

        io = PolarsDeltaIoManager(base_path="/data")
        merger = _FakeTableMerger()

        io.write(_output_ctx("t"), merger)

        # DataFrame / LazyFrame write methods must NOT be called.
        _mock_polars.DataFrame.write_delta.assert_not_called()
        _mock_polars.LazyFrame.sink_delta.assert_not_called()
