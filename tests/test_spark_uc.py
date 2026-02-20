"""Tests for SparkUCTableIoManager, SparkUCVolumeDeltaIoManager, and SparkUCVolumeParquetIoManager."""

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


# ===========================================================================
# SparkUCTableIoManager
# ===========================================================================


class TestSparkUCTableIoManagerConstruction:
    def test_stores_catalog_and_schema(self):
        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        io = SparkUCTableIoManager(catalog="main", schema="staging")
        assert io.catalog == "main"
        assert io.schema == "staging"

    def test_table_name_generation(self):
        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        io = SparkUCTableIoManager(catalog="main", schema="staging")
        assert io._table_name("extract") == "main.staging.extract"


class TestSparkUCTableIoManagerSetup:
    def test_setup_obtains_session(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        io = SparkUCTableIoManager(catalog="main", schema="staging")
        io.setup()

        # No spark.conf.set calls — UC handles auth
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

        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        io = SparkUCTableIoManager(catalog="main", schema="staging")

        with pytest.raises(RuntimeError, match="No active SparkSession"):
            io.setup()


class TestSparkUCTableIoManagerWrite:
    def test_write_uses_save_as_table(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        io = SparkUCTableIoManager(catalog="main", schema="staging")
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        spark_df.write.format.assert_called_once_with("delta")
        spark_df.write.format.return_value.mode.assert_called_once_with("error")
        spark_df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
            "main.staging.my_task"
        )

    def test_write_with_partition_by(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        io = SparkUCTableIoManager(
            catalog="main", schema="staging", partition_by="region"
        )
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        writer = spark_df.write.format.return_value.mode.return_value
        writer.partitionBy.assert_called_once_with("region")
        writer.partitionBy.return_value.saveAsTable.assert_called_once_with(
            "main.staging.my_task"
        )

    def test_write_with_write_options(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        io = SparkUCTableIoManager(
            catalog="main",
            schema="staging",
            write_options={"mergeSchema": "true"},
        )
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        writer = spark_df.write.format.return_value.mode.return_value
        writer.option.assert_called_once_with("mergeSchema", "true")


class TestSparkUCTableIoManagerRead:
    def test_read_uses_spark_table(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        io = SparkUCTableIoManager(catalog="main", schema="staging")
        io.setup()

        io.read(_input_ctx("upstream_task"))

        _mock_pyspark.table.assert_called_once_with("main.staging.upstream_task")


# ===========================================================================
# SparkUCVolumeDeltaIoManager
# ===========================================================================


class TestSparkUCVolumeDeltaIoManagerConstruction:
    def test_stores_catalog_schema_volume(self):
        from databricks_bundle_decorators.io_managers import SparkUCVolumeDeltaIoManager

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )
        assert io.catalog == "main"
        assert io.schema == "staging"
        assert io.volume == "raw_data"

    def test_uri_generation(self):
        from databricks_bundle_decorators.io_managers import SparkUCVolumeDeltaIoManager

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )
        assert io._uri("extract") == "/Volumes/main/staging/raw_data/extract"


class TestSparkUCVolumeDeltaIoManagerSetup:
    def test_setup_obtains_session(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCVolumeDeltaIoManager

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )
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

        from databricks_bundle_decorators.io_managers import SparkUCVolumeDeltaIoManager

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )

        with pytest.raises(RuntimeError, match="No active SparkSession"):
            io.setup()


class TestSparkUCVolumeDeltaIoManagerWrite:
    def test_write_delta_to_volume(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCVolumeDeltaIoManager

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        spark_df.write.format.assert_called_once_with("delta")
        spark_df.write.format.return_value.mode.assert_called_once_with("error")
        spark_df.write.format.return_value.mode.return_value.save.assert_called_once_with(
            "/Volumes/main/staging/raw_data/my_task"
        )

    def test_write_with_partition_by(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCVolumeDeltaIoManager

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
            partition_by="region",
        )
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        writer = spark_df.write.format.return_value.mode.return_value
        writer.partitionBy.assert_called_once_with("region")

    def test_write_with_write_options(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCVolumeDeltaIoManager

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
            write_options={"mergeSchema": "true"},
        )
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        writer = spark_df.write.format.return_value.mode.return_value
        writer.option.assert_called_once_with("mergeSchema", "true")


class TestSparkUCVolumeDeltaIoManagerRead:
    def test_read_delta_from_volume(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCVolumeDeltaIoManager

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )
        io.setup()

        io.read(_input_ctx("upstream_task"))

        _mock_pyspark.read.format.assert_called_once_with("delta")
        _mock_pyspark.read.format.return_value.load.assert_called_once_with(
            "/Volumes/main/staging/raw_data/upstream_task"
        )

    def test_read_with_read_options(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCVolumeDeltaIoManager

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
            read_options={"versionAsOf": "3"},
        )
        io.setup()

        io.read(_input_ctx("upstream_task"))

        reader = _mock_pyspark.read.format.return_value
        reader.option.assert_called_once_with("versionAsOf", "3")


# ===========================================================================
# SparkUCVolumeParquetIoManager
# ===========================================================================


class TestSparkUCVolumeParquetIoManagerConstruction:
    def test_stores_catalog_schema_volume(self):
        from databricks_bundle_decorators.io_managers import (
            SparkUCVolumeParquetIoManager,
        )

        io = SparkUCVolumeParquetIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )
        assert io.catalog == "main"
        assert io.schema == "staging"
        assert io.volume == "raw_data"

    def test_uri_generation(self):
        from databricks_bundle_decorators.io_managers import (
            SparkUCVolumeParquetIoManager,
        )

        io = SparkUCVolumeParquetIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )
        assert io._uri("extract") == "/Volumes/main/staging/raw_data/extract.parquet"


class TestSparkUCVolumeParquetIoManagerSetup:
    def test_setup_obtains_session(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import (
            SparkUCVolumeParquetIoManager,
        )

        io = SparkUCVolumeParquetIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )
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
            SparkUCVolumeParquetIoManager,
        )

        io = SparkUCVolumeParquetIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )

        with pytest.raises(RuntimeError, match="No active SparkSession"):
            io.setup()


class TestSparkUCVolumeParquetIoManagerWrite:
    def test_write_parquet_to_volume(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import (
            SparkUCVolumeParquetIoManager,
        )

        io = SparkUCVolumeParquetIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        spark_df.write.format.assert_called_once_with("parquet")
        spark_df.write.format.return_value.mode.assert_called_once_with("overwrite")
        spark_df.write.format.return_value.mode.return_value.save.assert_called_once_with(
            "/Volumes/main/staging/raw_data/my_task.parquet"
        )

    def test_write_with_partition_by(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import (
            SparkUCVolumeParquetIoManager,
        )

        io = SparkUCVolumeParquetIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
            partition_by=["region", "date"],
        )
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        writer = spark_df.write.format.return_value.mode.return_value
        writer.partitionBy.assert_called_once_with("region", "date")

    def test_write_with_write_options(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import (
            SparkUCVolumeParquetIoManager,
        )

        io = SparkUCVolumeParquetIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
            write_options={"compression": "snappy"},
        )
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        writer = spark_df.write.format.return_value.mode.return_value
        writer.option.assert_called_once_with("compression", "snappy")


class TestSparkUCVolumeParquetIoManagerRead:
    def test_read_parquet_from_volume(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import (
            SparkUCVolumeParquetIoManager,
        )

        io = SparkUCVolumeParquetIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )
        io.setup()

        io.read(_input_ctx("upstream_task"))

        _mock_pyspark.read.format.assert_called_once_with("parquet")
        _mock_pyspark.read.format.return_value.load.assert_called_once_with(
            "/Volumes/main/staging/raw_data/upstream_task.parquet"
        )

    def test_read_with_read_options(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import (
            SparkUCVolumeParquetIoManager,
        )

        io = SparkUCVolumeParquetIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
            read_options={"mergeSchema": "true"},
        )
        io.setup()

        io.read(_input_ctx("upstream_task"))

        reader = _mock_pyspark.read.format.return_value
        reader.option.assert_called_once_with("mergeSchema", "true")


# ===========================================================================
# mode parameter – UC Delta IoManagers
# ===========================================================================


class TestSparkUCTableModeParameter:
    def test_default_mode_is_error(self):
        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        io = SparkUCTableIoManager(catalog="main", schema="staging")
        assert io._mode == "error"

    def test_custom_mode_append(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        io = SparkUCTableIoManager(catalog="main", schema="staging", mode="append")
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        spark_df.write.format.return_value.mode.assert_called_once_with("append")

    def test_custom_mode_error(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        io = SparkUCTableIoManager(catalog="main", schema="staging", mode="error")
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        spark_df.write.format.return_value.mode.assert_called_once_with("error")


class TestSparkUCVolumeDeltaModeParameter:
    def test_default_mode_is_error(self):
        from databricks_bundle_decorators.io_managers import SparkUCVolumeDeltaIoManager

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )
        assert io._mode == "error"

    def test_custom_mode_append(self, _mock_pyspark):
        from databricks_bundle_decorators.io_managers import SparkUCVolumeDeltaIoManager

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
            mode="append",
        )
        io.setup()

        spark_df = MagicMock()
        io.write(_output_ctx("my_task"), spark_df)

        spark_df.write.format.return_value.mode.assert_called_once_with("append")


# ===========================================================================
# DeltaMergeBuilder support – UC Delta IoManagers
# ===========================================================================


class TestSparkUCTableMergeBuilder:
    def test_merge_builder_calls_execute(self, _mock_pyspark, monkeypatch):
        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        delta_mod = types.ModuleType("delta")
        tables_mod = types.ModuleType("delta.tables")

        class _FakeDeltaMergeBuilder:
            execute = MagicMock()

        tables_mod.DeltaMergeBuilder = _FakeDeltaMergeBuilder  # type: ignore[attr-defined]
        delta_mod.tables = tables_mod  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "delta", delta_mod)
        monkeypatch.setitem(sys.modules, "delta.tables", tables_mod)

        io = SparkUCTableIoManager(catalog="main", schema="staging")
        io.setup()

        builder = _FakeDeltaMergeBuilder()
        io.write(_output_ctx("t"), builder)

        builder.execute.assert_called_once()

    def test_merge_builder_skips_save_as_table(self, _mock_pyspark, monkeypatch):
        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        delta_mod = types.ModuleType("delta")
        tables_mod = types.ModuleType("delta.tables")

        class _FakeDeltaMergeBuilder:
            execute = MagicMock()

        tables_mod.DeltaMergeBuilder = _FakeDeltaMergeBuilder  # type: ignore[attr-defined]
        delta_mod.tables = tables_mod  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "delta", delta_mod)
        monkeypatch.setitem(sys.modules, "delta.tables", tables_mod)

        io = SparkUCTableIoManager(catalog="main", schema="staging")
        io.setup()

        spark_df = MagicMock()
        builder = _FakeDeltaMergeBuilder()
        io.write(_output_ctx("t"), builder)

        spark_df.write.format.assert_not_called()


class TestSparkUCVolumeDeltaMergeBuilder:
    def test_merge_builder_calls_execute(self, _mock_pyspark, monkeypatch):
        from databricks_bundle_decorators.io_managers import SparkUCVolumeDeltaIoManager

        delta_mod = types.ModuleType("delta")
        tables_mod = types.ModuleType("delta.tables")

        class _FakeDeltaMergeBuilder:
            execute = MagicMock()

        tables_mod.DeltaMergeBuilder = _FakeDeltaMergeBuilder  # type: ignore[attr-defined]
        delta_mod.tables = tables_mod  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "delta", delta_mod)
        monkeypatch.setitem(sys.modules, "delta.tables", tables_mod)

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )
        io.setup()

        builder = _FakeDeltaMergeBuilder()
        io.write(_output_ctx("t"), builder)

        builder.execute.assert_called_once()

    def test_merge_builder_skips_save(self, _mock_pyspark, monkeypatch):
        from databricks_bundle_decorators.io_managers import SparkUCVolumeDeltaIoManager

        delta_mod = types.ModuleType("delta")
        tables_mod = types.ModuleType("delta.tables")

        class _FakeDeltaMergeBuilder:
            execute = MagicMock()

        tables_mod.DeltaMergeBuilder = _FakeDeltaMergeBuilder  # type: ignore[attr-defined]
        delta_mod.tables = tables_mod  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "delta", delta_mod)
        monkeypatch.setitem(sys.modules, "delta.tables", tables_mod)

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )
        io.setup()

        spark_df = MagicMock()
        builder = _FakeDeltaMergeBuilder()
        io.write(_output_ctx("t"), builder)

        spark_df.write.format.assert_not_called()
