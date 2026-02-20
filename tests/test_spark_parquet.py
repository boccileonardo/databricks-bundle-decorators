"""Tests for SparkParquetIoManager and SparkServerlessParquetIoManager."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

from databricks_bundle_decorators.io_manager import InputContext, OutputContext


# ---------------------------------------------------------------------------
# Helpers – mock pyspark so IoManagers can be tested without Spark.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _mock_pyspark(monkeypatch: pytest.MonkeyPatch):
    """Inject a fake ``pyspark`` module for every test in this file."""
    pyspark_mod = types.ModuleType("pyspark")
    sql_mod = types.ModuleType("pyspark.sql")

    mock_session = MagicMock()

    class _MockSparkSession:
        @staticmethod
        def getActiveSession() -> MagicMock:
            return mock_session

    sql_mod.SparkSession = _MockSparkSession  # type: ignore[attr-defined]
    pyspark_mod.sql = sql_mod  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "pyspark", pyspark_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql_mod)

    yield mock_session


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
# SparkParquetIoManager (classic compute)
# ---------------------------------------------------------------------------


class TestSparkParquetIoManagerConstruction:
    def test_strips_trailing_slash(self):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        io = SparkParquetIoManager(base_path="/data/lake/")
        assert io.base_path == "/data/lake"

    def test_spark_configs_default_none(self):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        io = SparkParquetIoManager(base_path="/data")
        assert io.spark_configs is None

    def test_spark_configs_as_dict(self):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        configs = {"fs.azure.account.key.sa.dfs.core.windows.net": "secret"}
        io = SparkParquetIoManager(base_path="/data", spark_configs=configs)
        assert io.spark_configs == configs

    def test_spark_configs_as_callable(self):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        configs = {"fs.azure.account.key.sa.dfs.core.windows.net": "secret"}
        io = SparkParquetIoManager(base_path="/data", spark_configs=lambda: configs)
        assert io.spark_configs == configs

    def test_spark_configs_callable_invoked_each_time(self):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        call_count = 0

        def _factory() -> dict[str, str]:
            nonlocal call_count
            call_count += 1
            return {"key": str(call_count)}

        io = SparkParquetIoManager(base_path="/data", spark_configs=_factory)
        assert io.spark_configs == {"key": "1"}
        assert io.spark_configs == {"key": "2"}
        assert call_count == 2

    def test_uri_generation(self):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        io = SparkParquetIoManager(
            base_path="abfss://container@sa.dfs.core.windows.net/prefix",
        )
        assert (
            io._uri("extract")
            == "abfss://container@sa.dfs.core.windows.net/prefix/extract.parquet"
        )


class TestSparkParquetIoManagerSetup:
    def test_setup_applies_spark_configs(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        configs = {
            "fs.azure.account.key.sa.dfs.core.windows.net": "secret",
            "spark.some.other.config": "value",
        }
        io = SparkParquetIoManager(base_path="/data", spark_configs=configs)
        io.setup()

        assert _mock_pyspark.conf.set.call_count == 2
        _mock_pyspark.conf.set.assert_any_call(
            "fs.azure.account.key.sa.dfs.core.windows.net", "secret"
        )
        _mock_pyspark.conf.set.assert_any_call("spark.some.other.config", "value")

    def test_setup_no_configs(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        io = SparkParquetIoManager(base_path="/data")
        io.setup()

        _mock_pyspark.conf.set.assert_not_called()

    def test_setup_no_active_session_raises(self, monkeypatch):
        """If no active SparkSession, setup should raise RuntimeError."""
        pyspark_mod = types.ModuleType("pyspark")
        sql_mod = types.ModuleType("pyspark.sql")

        class _NoSession:
            @staticmethod
            def getActiveSession() -> None:
                return None

        sql_mod.SparkSession = _NoSession  # type: ignore[attr-defined]
        pyspark_mod.sql = sql_mod  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "pyspark", pyspark_mod)
        monkeypatch.setitem(sys.modules, "pyspark.sql", sql_mod)

        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        io = SparkParquetIoManager(base_path="/data")

        with pytest.raises(RuntimeError, match="No active SparkSession"):
            io.setup()


class TestSparkParquetIoManagerWrite:
    def test_write_parquet(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        io = SparkParquetIoManager(base_path="/data")
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        spark_df.write.format.assert_called_once_with("parquet")
        spark_df.write.format.return_value.mode.assert_called_once_with("overwrite")
        spark_df.write.format.return_value.mode.return_value.save.assert_called_once_with(
            "/data/my_task.parquet"
        )

    def test_write_with_partition_by_string(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        io = SparkParquetIoManager(base_path="/data", partition_by="region")
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        writer = spark_df.write.format.return_value.mode.return_value
        writer.partitionBy.assert_called_once_with("region")
        writer.partitionBy.return_value.save.assert_called_once_with(
            "/data/my_task.parquet"
        )

    def test_write_with_partition_by_list(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        io = SparkParquetIoManager(base_path="/data", partition_by=["region", "date"])
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        writer = spark_df.write.format.return_value.mode.return_value
        writer.partitionBy.assert_called_once_with("region", "date")

    def test_write_with_write_options(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        io = SparkParquetIoManager(
            base_path="/data",
            write_options={"compression": "snappy"},
        )
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        writer = spark_df.write.format.return_value.mode.return_value
        writer.option.assert_called_once_with("compression", "snappy")


class TestSparkParquetIoManagerRead:
    def test_read_parquet(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        io = SparkParquetIoManager(base_path="/data")
        io.setup()

        io.read(_input_ctx("upstream_task"))

        _mock_pyspark.read.format.assert_called_once_with("parquet")
        _mock_pyspark.read.format.return_value.load.assert_called_once_with(
            "/data/upstream_task.parquet"
        )

    def test_read_with_read_options(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        io = SparkParquetIoManager(
            base_path="/data",
            read_options={"mergeSchema": "true"},
        )
        io.setup()

        io.read(_input_ctx("upstream_task"))

        reader = _mock_pyspark.read.format.return_value
        reader.option.assert_called_once_with("mergeSchema", "true")


# ---------------------------------------------------------------------------
# SparkServerlessParquetIoManager
# ---------------------------------------------------------------------------


class TestSparkServerlessParquetIoManagerConstruction:
    def test_strips_trailing_slash(self):
        from databricks_bundle_decorators.io_managers import (
            SparkServerlessParquetIoManager,
        )

        io = SparkServerlessParquetIoManager(base_path="/data/lake/")
        assert io.base_path == "/data/lake"


class TestSparkServerlessParquetIoManagerSetup:
    def test_setup_does_not_set_configs(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import (
            SparkServerlessParquetIoManager,
        )

        io = SparkServerlessParquetIoManager(base_path="/data")
        io.setup()

        _mock_pyspark.conf.set.assert_not_called()

    def test_setup_no_active_session_raises(self, monkeypatch):
        pyspark_mod = types.ModuleType("pyspark")
        sql_mod = types.ModuleType("pyspark.sql")

        class _NoSession:
            @staticmethod
            def getActiveSession() -> None:
                return None

        sql_mod.SparkSession = _NoSession  # type: ignore[attr-defined]
        pyspark_mod.sql = sql_mod  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "pyspark", pyspark_mod)
        monkeypatch.setitem(sys.modules, "pyspark.sql", sql_mod)

        from databricks_bundle_decorators.io_managers import (
            SparkServerlessParquetIoManager,
        )

        io = SparkServerlessParquetIoManager(base_path="/data")

        with pytest.raises(RuntimeError, match="No active SparkSession"):
            io.setup()


class TestSparkServerlessParquetIoManagerWrite:
    def test_write_parquet(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import (
            SparkServerlessParquetIoManager,
        )

        io = SparkServerlessParquetIoManager(base_path="/data")
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        spark_df.write.format.assert_called_once_with("parquet")
        spark_df.write.format.return_value.mode.assert_called_once_with("overwrite")
        spark_df.write.format.return_value.mode.return_value.save.assert_called_once_with(
            "/data/my_task.parquet"
        )


class TestSparkServerlessParquetIoManagerRead:
    def test_read_parquet(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import (
            SparkServerlessParquetIoManager,
        )

        io = SparkServerlessParquetIoManager(base_path="/data")
        io.setup()

        io.read(_input_ctx("upstream_task"))

        _mock_pyspark.read.format.assert_called_once_with("parquet")
        _mock_pyspark.read.format.return_value.load.assert_called_once_with(
            "/data/upstream_task.parquet"
        )
