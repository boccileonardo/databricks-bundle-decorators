"""Tests for PolarsJsonIoManager with mocked polars."""

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

    class _LazyFrame:
        """Fake polars.LazyFrame."""

        def __init__(self, data: Any = None) -> None:
            self.data = data

        sink_ndjson = MagicMock()

    class _DataFrame:
        """Fake polars.DataFrame."""

        def __init__(self, data: Any = None) -> None:
            self.data = data

        def lazy(self) -> _LazyFrame:
            return _LazyFrame(self.data)

    mod.DataFrame = _DataFrame  # type: ignore[attr-defined]
    mod.LazyFrame = _LazyFrame  # type: ignore[attr-defined]
    mod.read_ndjson = MagicMock(return_value=_DataFrame({"col": [1, 2]}))  # type: ignore[attr-defined]
    mod.scan_ndjson = MagicMock(return_value=_LazyFrame({"col": [1, 2]}))  # type: ignore[attr-defined]

    return mod


@pytest.fixture(autouse=True)
def _mock_polars(monkeypatch: pytest.MonkeyPatch):
    """Inject a fake ``polars`` module for every test in this file."""
    mock_pl = _make_mock_polars()
    monkeypatch.setitem(sys.modules, "polars", mock_pl)
    # Reset call tracking between tests
    mock_pl.LazyFrame.sink_ndjson.reset_mock()
    mock_pl.read_ndjson.reset_mock()
    mock_pl.scan_ndjson.reset_mock()
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
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(base_path="/data/lake/")
        assert io.base_path == "/data/lake"

    def test_storage_options_default_none(self):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(base_path="/data")
        assert io.storage_options is None

    def test_storage_options_as_dict(self):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        opts = {"account_name": "sa", "account_key": "secret"}
        io = PolarsJsonIoManager(base_path="/data", storage_options=opts)
        assert io.storage_options == opts

    def test_storage_options_as_callable(self):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        opts = {"account_name": "sa", "account_key": "secret"}
        io = PolarsJsonIoManager(base_path="/data", storage_options=lambda: opts)
        assert io.storage_options == opts

    def test_storage_options_callable_invoked_each_time(self):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        call_count = 0

        def _factory() -> dict[str, str]:
            nonlocal call_count
            call_count += 1
            return {"key": str(call_count)}

        io = PolarsJsonIoManager(base_path="/data", storage_options=_factory)
        assert io.storage_options == {"key": "1"}
        assert io.storage_options == {"key": "2"}
        assert call_count == 2

    def test_uri_generation(self):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(base_path="s3://bucket/prefix")
        assert io._uri("extract") == "s3://bucket/prefix/extract.ndjson"


# ---------------------------------------------------------------------------
# Write dispatch
# ---------------------------------------------------------------------------


class TestWrite:
    def test_write_lazyframe_calls_sink_ndjson(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(
            base_path="/data",
            storage_options={"key": "val"},
        )
        lf = _mock_polars.LazyFrame()

        io.write(_output_ctx("extract"), lf)

        lf.sink_ndjson.assert_called_once_with(
            "/data/extract.ndjson",
            storage_options={"key": "val"},
        )

    def test_write_dataframe_routes_through_lazy(self, _mock_polars):
        """DataFrame is converted to lazy then sink_ndjson is called."""
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(
            base_path="/data",
            storage_options={"key": "val"},
        )
        df = _mock_polars.DataFrame()

        io.write(_output_ctx("extract"), df)

        # df.lazy() returns a LazyFrame; sink_ndjson is a class-level mock
        _mock_polars.LazyFrame.sink_ndjson.assert_called_with(
            "/data/extract.ndjson",
            storage_options={"key": "val"},
        )

    def test_write_unsupported_type_raises(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(base_path="/data")

        with pytest.raises(TypeError, match="got dict"):
            io.write(_output_ctx(), {"not": "a dataframe"})

    def test_write_none_storage_options(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(base_path="/data")
        lf = _mock_polars.LazyFrame()

        io.write(_output_ctx("t"), lf)

        lf.sink_ndjson.assert_called_once_with(
            "/data/t.ndjson",
            storage_options=None,
        )

    def test_write_callable_storage_options(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        opts = {"account_name": "sa", "account_key": "secret"}
        io = PolarsJsonIoManager(base_path="/data", storage_options=lambda: opts)
        lf = _mock_polars.LazyFrame()

        io.write(_output_ctx("t"), lf)

        lf.sink_ndjson.assert_called_once_with(
            "/data/t.ndjson",
            storage_options=opts,
        )


# ---------------------------------------------------------------------------
# Read dispatch
# ---------------------------------------------------------------------------


class TestRead:
    def test_read_defaults_to_scan(self, _mock_polars):
        """Unannotated parameter → scan_ndjson (lazy)."""
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(
            base_path="/data",
            storage_options={"key": "val"},
        )

        result = io.read(_input_ctx("upstream_task", expected_type=None))

        _mock_polars.scan_ndjson.assert_called_once_with(
            "/data/upstream_task.ndjson",
            storage_options={"key": "val"},
        )
        assert isinstance(result, _mock_polars.LazyFrame)

    def test_read_lazyframe_annotation_uses_scan(self, _mock_polars):
        """Explicit LazyFrame annotation → scan_ndjson."""
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(base_path="/data")

        result = io.read(
            _input_ctx("upstream_task", expected_type=_mock_polars.LazyFrame)
        )

        _mock_polars.scan_ndjson.assert_called_once()
        assert isinstance(result, _mock_polars.LazyFrame)

    def test_read_dataframe_annotation_uses_read(self, _mock_polars):
        """Explicit DataFrame annotation → read_ndjson (eager)."""
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(
            base_path="/data",
            storage_options={"key": "val"},
        )

        result = io.read(
            _input_ctx("upstream_task", expected_type=_mock_polars.DataFrame)
        )

        _mock_polars.read_ndjson.assert_called_once_with(
            "/data/upstream_task.ndjson",
            storage_options={"key": "val"},
        )
        _mock_polars.scan_ndjson.assert_not_called()
        assert isinstance(result, _mock_polars.DataFrame)

    def test_read_none_storage_options(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(base_path="/data")

        io.read(_input_ctx("t"))

        _mock_polars.scan_ndjson.assert_called_once_with(
            "/data/t.ndjson",
            storage_options=None,
        )

    def test_read_callable_storage_options(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        opts = {"account_name": "sa", "account_key": "secret"}
        io = PolarsJsonIoManager(base_path="/data", storage_options=lambda: opts)

        io.read(_input_ctx("t"))

        _mock_polars.scan_ndjson.assert_called_once_with(
            "/data/t.ndjson",
            storage_options=opts,
        )


# ---------------------------------------------------------------------------
# write_options / read_options passthrough
# ---------------------------------------------------------------------------


class TestWriteOptions:
    def test_write_options_forwarded_to_sink_ndjson(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(
            base_path="/data",
            write_options={"maintain_order": True},
        )
        lf = _mock_polars.LazyFrame()

        io.write(_output_ctx("t"), lf)

        lf.sink_ndjson.assert_called_once_with(
            "/data/t.ndjson",
            storage_options=None,
            maintain_order=True,
        )

    def test_write_options_forwarded_to_dataframe_lazy_sink(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(
            base_path="/data",
            write_options={"maintain_order": True},
        )
        df = _mock_polars.DataFrame()

        io.write(_output_ctx("t"), df)

        # df.lazy() returns a LazyFrame; sink_ndjson is a class-level mock
        _mock_polars.LazyFrame.sink_ndjson.assert_called_with(
            "/data/t.ndjson",
            storage_options=None,
            maintain_order=True,
        )

    def test_write_options_default_empty(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(base_path="/data")
        assert io._write_options == {}


class TestReadOptions:
    def test_read_options_forwarded_to_scan_ndjson(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(
            base_path="/data",
            read_options={"n_rows": 50},
        )

        io.read(_input_ctx("t"))

        _mock_polars.scan_ndjson.assert_called_once_with(
            "/data/t.ndjson",
            storage_options=None,
            n_rows=50,
        )

    def test_read_options_forwarded_to_read_ndjson(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(
            base_path="/data",
            read_options={"n_rows": 50},
        )

        io.read(_input_ctx("t", expected_type=_mock_polars.DataFrame))

        _mock_polars.read_ndjson.assert_called_once_with(
            "/data/t.ndjson",
            storage_options=None,
            n_rows=50,
        )

    def test_read_options_default_empty(self, _mock_polars):
        from databricks_bundle_decorators.io_managers import PolarsJsonIoManager

        io = PolarsJsonIoManager(base_path="/data")
        assert io._read_options == {}
