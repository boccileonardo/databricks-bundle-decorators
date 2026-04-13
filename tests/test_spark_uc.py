"""Tests for SparkUCTableIoManager, SparkUCVolumeDeltaIoManager, and SparkUCVolumeParquetIoManager."""

from __future__ import annotations

import uuid

import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from databricks_bundle_decorators.io_manager import InputContext, OutputContext
from databricks_bundle_decorators.io_managers import (
    SparkUCTableIoManager,
    SparkUCVolumeDeltaIoManager,
    SparkUCVolumeParquetIoManager,
)


def _output_ctx(task_key: str = "my_task", **kwargs) -> OutputContext:
    return OutputContext(job_name="j", task_key=task_key, run_id="r1", **kwargs)


def _input_ctx(upstream: str = "producer", **kwargs) -> InputContext:
    return InputContext(
        job_name="j",
        task_key="consumer",
        upstream_task_key=upstream,
        run_id="r1",
        **kwargs,
    )


@pytest.fixture
def uc_schema(spark: SparkSession):
    """Create a unique schema in the default catalog for UC table tests."""
    schema_name = f"test_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{schema_name}`")
    yield schema_name
    spark.sql(f"DROP DATABASE IF EXISTS `{schema_name}` CASCADE")


# ===========================================================================
# SparkUCTableIoManager – construction
# ===========================================================================


class TestSparkUCTableConstruction:
    def test_stores_catalog_and_schema(self):
        io = SparkUCTableIoManager(catalog="main", schema="staging")
        assert io.catalog == "main"
        assert io.schema == "staging"

    def test_table_name_generation(self):
        io = SparkUCTableIoManager(catalog="main", schema="staging")
        assert io._table_name("extract") == "main.staging.extract"

    def test_default_mode_is_error(self):
        io = SparkUCTableIoManager(catalog="main", schema="staging")
        assert io._mode == "error"


class TestSparkUCTableSetup:
    def test_setup_obtains_session(self, spark: SparkSession):
        io = SparkUCTableIoManager(catalog="spark_catalog", schema="default")
        io.setup()
        assert io._spark is spark


# ---------------------------------------------------------------------------
# SparkUCTableIoManager – round-trip using local catalog
#
# OSS Delta V2 catalog does not support saveAsTable with mode("overwrite")
# (raises "does not support truncate in batch mode").  We use mode="error"
# for first writes and mode="append" for append tests.  Overwrite + save
# is already validated in the non-UC SparkDeltaIoManager tests.
# ---------------------------------------------------------------------------


class TestSparkUCTableRoundTrip:
    def test_basic_round_trip(self, spark: SparkSession, uc_schema: str):
        io = SparkUCTableIoManager(
            catalog="spark_catalog", schema=uc_schema, mode="error"
        )
        io.setup()

        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        io.write(_output_ctx("my_task"), df)

        result = io.read(_input_ctx("my_task"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 2
        assert rows[0]["val"] == "a"

    def test_mode_append(self, spark: SparkSession, uc_schema: str):
        io = SparkUCTableIoManager(
            catalog="spark_catalog", schema=uc_schema, mode="append"
        )
        io.setup()

        df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("task"), df1)

        df2 = spark.createDataFrame([(2, "b")], ["id", "val"])
        io.write(_output_ctx("task"), df2)

        result = io.read(_input_ctx("task"))
        assert result.count() == 2

    def test_mode_error_on_existing(self, spark: SparkSession, uc_schema: str):
        io = SparkUCTableIoManager(
            catalog="spark_catalog", schema=uc_schema, mode="error"
        )
        io.setup()

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("err_task"), df)

        with pytest.raises(Exception):
            io.write(_output_ctx("err_task"), df)

    def test_partition_by(self, spark: SparkSession, uc_schema: str):
        io = SparkUCTableIoManager(
            catalog="spark_catalog", schema=uc_schema, mode="error"
        )
        io.setup()

        df = spark.createDataFrame(
            [(1, "us", "a"), (2, "eu", "b")],
            ["id", "region", "val"],
        )
        ctx = _output_ctx("partitioned", partition_by=["region"])
        io.write(ctx, df)

        pv = io._extract_partition_values(ctx)
        assert pv == {"region": ["eu", "us"]}

        result = io.read(_input_ctx("partitioned"))
        assert result.count() == 2

    def test_write_options_applied(self, spark: SparkSession, uc_schema: str):
        io = SparkUCTableIoManager(
            catalog="spark_catalog",
            schema=uc_schema,
            mode="error",
            write_options={"mergeSchema": "true"},
        )
        io.setup()

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("opts"), df)
        assert io.read(_input_ctx("opts")).count() == 1


class TestSparkUCTableMergeBuilder:
    def test_merge_upsert(self, spark: SparkSession, uc_schema: str):
        io = SparkUCTableIoManager(
            catalog="spark_catalog", schema=uc_schema, mode="error"
        )
        io.setup()

        initial = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        io.write(_output_ctx("merge_tbl"), initial)

        # DeltaTable.forName with 3-level names fails in OSS Spark;
        # use the schema-qualified name directly.
        dt = DeltaTable.forName(spark, f"`{uc_schema}`.merge_tbl")
        updates = spark.createDataFrame([(2, "B"), (3, "c")], ["id", "val"])
        builder = (
            dt.alias("t")
            .merge(updates.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
        )

        io.write(_output_ctx("merge_tbl"), builder)

        result = io.read(_input_ctx("merge_tbl"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 3
        assert rows[0]["val"] == "a"
        assert rows[1]["val"] == "B"
        assert rows[2]["val"] == "c"


# ===========================================================================
# SparkUCVolumeDeltaIoManager – construction & round-trip
# ===========================================================================


class TestSparkUCVolumeDeltaConstruction:
    def test_stores_catalog_schema_volume(self):
        io = SparkUCVolumeDeltaIoManager(
            catalog="main", schema="staging", volume="raw_data"
        )
        assert io.catalog == "main"
        assert io.schema == "staging"
        assert io.volume == "raw_data"

    def test_uri_generation(self):
        io = SparkUCVolumeDeltaIoManager(
            catalog="main", schema="staging", volume="raw_data"
        )
        assert io._uri("extract") == "/Volumes/main/staging/raw_data/extract"

    def test_default_mode_is_error(self):
        io = SparkUCVolumeDeltaIoManager(
            catalog="main", schema="staging", volume="raw_data"
        )
        assert io._mode == "error"


class TestSparkUCVolumeDeltaSetup:
    def test_setup_obtains_session(self, spark: SparkSession):
        io = SparkUCVolumeDeltaIoManager(
            catalog="main", schema="staging", volume="raw_data"
        )
        io.setup()
        assert io._spark is spark


class TestSparkUCVolumeDeltaRoundTrip:
    """Test real I/O by redirecting _uri to tmp_path (volumes are just paths)."""

    def test_basic_round_trip(self, spark: SparkSession, tmp_path, monkeypatch):
        io = SparkUCVolumeDeltaIoManager(
            catalog="main", schema="staging", volume="raw_data", mode="overwrite"
        )
        io.setup()
        monkeypatch.setattr(io, "_uri", lambda key: str(tmp_path / key))

        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        io.write(_output_ctx("my_task"), df)

        result = io.read(_input_ctx("my_task"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 2
        assert rows[0]["val"] == "a"

    def test_mode_append(self, spark: SparkSession, tmp_path, monkeypatch):
        io = SparkUCVolumeDeltaIoManager(
            catalog="main", schema="staging", volume="raw_data", mode="append"
        )
        io.setup()
        monkeypatch.setattr(io, "_uri", lambda key: str(tmp_path / key))

        df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("task"), df1)

        df2 = spark.createDataFrame([(2, "b")], ["id", "val"])
        io.write(_output_ctx("task"), df2)

        result = io.read(_input_ctx("task"))
        assert result.count() == 2

    def test_partition_by(self, spark: SparkSession, tmp_path, monkeypatch):
        io = SparkUCVolumeDeltaIoManager(
            catalog="main", schema="staging", volume="raw_data", mode="overwrite"
        )
        io.setup()
        monkeypatch.setattr(io, "_uri", lambda key: str(tmp_path / key))

        df = spark.createDataFrame([(1, "us"), (2, "eu")], ["id", "region"])
        ctx = _output_ctx("partitioned", partition_by=["region"])
        io.write(ctx, df)

        pv = io._extract_partition_values(ctx)
        assert pv == {"region": ["eu", "us"]}

    def test_write_options(self, spark: SparkSession, tmp_path, monkeypatch):
        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
            mode="overwrite",
            write_options={"mergeSchema": "true"},
        )
        io.setup()
        monkeypatch.setattr(io, "_uri", lambda key: str(tmp_path / key))

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("opts"), df)
        assert io.read(_input_ctx("opts")).count() == 1

    def test_read_options(self, spark: SparkSession, tmp_path, monkeypatch):
        io_write = SparkUCVolumeDeltaIoManager(
            catalog="main", schema="staging", volume="raw_data", mode="overwrite"
        )
        io_write.setup()

        def uri_fn(key: str) -> str:
            return str(tmp_path / key)

        monkeypatch.setattr(io_write, "_uri", uri_fn)

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io_write.write(_output_ctx("read_opts"), df)

        io_read = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
            read_options={"versionAsOf": "0"},
        )
        io_read.setup()
        monkeypatch.setattr(io_read, "_uri", uri_fn)

        result = io_read.read(_input_ctx("read_opts"))
        assert result.count() == 1


class TestSparkUCVolumeDeltaMergeBuilder:
    def test_merge_upsert(self, spark: SparkSession, tmp_path, monkeypatch):
        io = SparkUCVolumeDeltaIoManager(
            catalog="main", schema="staging", volume="raw_data", mode="overwrite"
        )
        io.setup()

        def uri_fn(key: str) -> str:
            return str(tmp_path / key)

        monkeypatch.setattr(io, "_uri", uri_fn)

        initial = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        io.write(_output_ctx("merge_vol"), initial)

        dt = DeltaTable.forPath(spark, str(tmp_path / "merge_vol"))
        updates = spark.createDataFrame([(2, "B"), (3, "c")], ["id", "val"])
        builder = (
            dt.alias("t")
            .merge(updates.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
        )

        io.write(_output_ctx("merge_vol"), builder)

        result = io.read(_input_ctx("merge_vol"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 3
        assert rows[1]["val"] == "B"


# ===========================================================================
# SparkUCVolumeParquetIoManager – construction & round-trip
# ===========================================================================


class TestSparkUCVolumeParquetConstruction:
    def test_stores_catalog_schema_volume(self):
        io = SparkUCVolumeParquetIoManager(
            catalog="main", schema="staging", volume="raw_data"
        )
        assert io.catalog == "main"
        assert io.schema == "staging"
        assert io.volume == "raw_data"

    def test_uri_generation(self):
        io = SparkUCVolumeParquetIoManager(
            catalog="main", schema="staging", volume="raw_data"
        )
        assert io._uri("extract") == "/Volumes/main/staging/raw_data/extract"


class TestSparkUCVolumeParquetSetup:
    def test_setup_obtains_session(self, spark: SparkSession):
        io = SparkUCVolumeParquetIoManager(
            catalog="main", schema="staging", volume="raw_data"
        )
        io.setup()
        assert io._spark is spark


class TestSparkUCVolumeParquetRoundTrip:
    """Test real I/O by redirecting _uri to tmp_path."""

    def test_basic_round_trip(self, spark: SparkSession, tmp_path, monkeypatch):
        io = SparkUCVolumeParquetIoManager(
            catalog="main", schema="staging", volume="raw_data"
        )
        io.setup()
        monkeypatch.setattr(io, "_uri", lambda key: str(tmp_path / key))

        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        io.write(_output_ctx("my_task"), df)

        result = io.read(_input_ctx("my_task"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 2
        assert rows[0]["val"] == "a"

    def test_overwrite_mode(self, spark: SparkSession, tmp_path, monkeypatch):
        io = SparkUCVolumeParquetIoManager(
            catalog="main", schema="staging", volume="raw_data"
        )
        io.setup()
        monkeypatch.setattr(io, "_uri", lambda key: str(tmp_path / key))

        df1 = spark.createDataFrame([(1, "old")], ["id", "val"])
        io.write(_output_ctx("task"), df1)

        df2 = spark.createDataFrame([(2, "new")], ["id", "val"])
        io.write(_output_ctx("task"), df2)

        result = io.read(_input_ctx("task"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["val"] == "new"

    def test_partition_by(self, spark: SparkSession, tmp_path, monkeypatch):
        io = SparkUCVolumeParquetIoManager(
            catalog="main", schema="staging", volume="raw_data"
        )
        io.setup()
        monkeypatch.setattr(io, "_uri", lambda key: str(tmp_path / key))

        df = spark.createDataFrame(
            [(1, "us", "a"), (2, "eu", "b")],
            ["id", "region", "val"],
        )
        ctx = _output_ctx("partitioned", partition_by=["region"])
        io.write(ctx, df)

        pv = io._extract_partition_values(ctx)
        assert pv == {"region": ["eu", "us"]}

        result = io.read(_input_ctx("partitioned"))
        assert result.count() == 2

    def test_write_options(self, spark: SparkSession, tmp_path, monkeypatch):
        io = SparkUCVolumeParquetIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
            write_options={"compression": "snappy"},
        )
        io.setup()
        monkeypatch.setattr(io, "_uri", lambda key: str(tmp_path / key))

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("opts"), df)
        assert io.read(_input_ctx("opts")).count() == 1

    def test_read_options(self, spark: SparkSession, tmp_path, monkeypatch):
        io = SparkUCVolumeParquetIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
            read_options={"mergeSchema": "true"},
        )
        io.setup()
        monkeypatch.setattr(io, "_uri", lambda key: str(tmp_path / key))

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("read_opts"), df)

        result = io.read(_input_ctx("read_opts"))
        assert result.count() == 1


# ===========================================================================
# Partition value extraction – all UC IoManagers
# ===========================================================================


class TestPartitionValueExtraction:
    def test_uc_table_caches_partition_values(
        self, spark: SparkSession, uc_schema: str
    ):
        io = SparkUCTableIoManager(
            catalog="spark_catalog", schema=uc_schema, mode="error"
        )
        io.setup()

        df = spark.createDataFrame([(1, "us"), (2, "eu")], ["id", "region"])
        ctx = _output_ctx("pv_task", partition_by=["region"])
        io.write(ctx, df)

        assert io._last_partition_values == {"region": ["eu", "us"]}

    def test_uc_volume_delta_caches_partition_values(
        self, spark: SparkSession, tmp_path, monkeypatch
    ):
        io = SparkUCVolumeDeltaIoManager(
            catalog="main", schema="staging", volume="raw_data", mode="overwrite"
        )
        io.setup()
        monkeypatch.setattr(io, "_uri", lambda key: str(tmp_path / key))

        df = spark.createDataFrame(
            [(1, "2026-03-01"), (2, "2026-03-02")], ["id", "date"]
        )
        ctx = _output_ctx("pv_vol_d", partition_by=["date"])
        io.write(ctx, df)

        assert io._last_partition_values == {"date": ["2026-03-01", "2026-03-02"]}

    def test_uc_volume_parquet_caches_partition_values(
        self, spark: SparkSession, tmp_path, monkeypatch
    ):
        io = SparkUCVolumeParquetIoManager(
            catalog="main", schema="staging", volume="raw_data"
        )
        io.setup()
        monkeypatch.setattr(io, "_uri", lambda key: str(tmp_path / key))

        df = spark.createDataFrame(
            [(1, "2026-03-01"), (2, "2026-03-02")], ["id", "date"]
        )
        ctx = _output_ctx("pv_vol_p", partition_by=["date"])
        io.write(ctx, df)

        assert io._last_partition_values == {"date": ["2026-03-01", "2026-03-02"]}

    def test_no_partition_write_does_not_set_values(
        self, spark: SparkSession, uc_schema: str
    ):
        io = SparkUCTableIoManager(
            catalog="spark_catalog", schema=uc_schema, mode="error"
        )
        io.setup()

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("no_part"), df)
        assert io._last_partition_values is None

    def test_sequential_writes_replace_values(
        self, spark: SparkSession, uc_schema: str
    ):
        io = SparkUCTableIoManager(
            catalog="spark_catalog", schema=uc_schema, mode="append"
        )
        io.setup()

        df1 = spark.createDataFrame([(1, "us")], ["id", "region"])
        ctx1 = _output_ctx("seq1", partition_by=["region"])
        io.write(ctx1, df1)
        assert io._extract_partition_values(ctx1) == {"region": ["us"]}

        df2 = spark.createDataFrame([(2, "eu"), (3, "ap")], ["id", "region"])
        ctx2 = _output_ctx("seq2", partition_by=["region"])
        io.write(ctx2, df2)
        assert io._extract_partition_values(ctx2) == {"region": ["ap", "eu"]}


# ===========================================================================
# Partition-scoped overwrite (backfill safety) – UC Table
# ===========================================================================


class TestSparkUCTablePartitionScopedOverwrite:
    """Verify replaceWhere is passed to the Spark writer.

    The local Delta v2 catalog does not support ``saveAsTable`` +
    ``replaceWhere``, so we intercept the DataFrameWriter calls and
    assert the option is set with the correct predicate.
    """

    def test_sets_replace_where_when_table_exists(
        self, spark: SparkSession, uc_schema: str, monkeypatch: pytest.MonkeyPatch
    ):
        """When the table already exists, replaceWhere must be set."""
        from pyspark.sql import DataFrameWriter

        io = SparkUCTableIoManager(
            catalog="spark_catalog", schema=uc_schema, mode="overwrite"
        )
        io.setup()

        df = spark.createDataFrame([(1, "eu", "x")], ["id", "region", "val"])

        captured_options: list[tuple[str, str]] = []
        _real_option = DataFrameWriter.option

        def _spy_option(self_w, key, value):
            captured_options.append((key, value))
            return _real_option(self_w, key, value)

        monkeypatch.setattr(DataFrameWriter, "option", _spy_option)
        monkeypatch.setattr(DataFrameWriter, "saveAsTable", lambda *_a, **_kw: None)
        monkeypatch.setattr(io._spark.catalog, "tableExists", lambda _: True)

        io.write(_output_ctx("uc_rw", partition_by=["region"]), df)

        assert ("replaceWhere", "region = 'eu'") in captured_options

    def test_skips_replace_where_on_first_write(
        self, spark: SparkSession, uc_schema: str, monkeypatch: pytest.MonkeyPatch
    ):
        """When the table does not exist yet, replaceWhere must NOT be set."""
        from pyspark.sql import DataFrameWriter

        io = SparkUCTableIoManager(
            catalog="spark_catalog", schema=uc_schema, mode="overwrite"
        )
        io.setup()

        df = spark.createDataFrame([(1, "eu", "x")], ["id", "region", "val"])

        captured_options: list[tuple[str, str]] = []
        _real_option = DataFrameWriter.option

        def _spy_option(self_w, key, value):
            captured_options.append((key, value))
            return _real_option(self_w, key, value)

        monkeypatch.setattr(DataFrameWriter, "option", _spy_option)
        monkeypatch.setattr(DataFrameWriter, "saveAsTable", lambda *_a, **_kw: None)
        monkeypatch.setattr(io._spark.catalog, "tableExists", lambda _: False)

        io.write(_output_ctx("uc_new", partition_by=["region"]), df)

        option_keys = [k for k, _v in captured_options]
        assert "replaceWhere" not in option_keys

    def test_replace_where_multi_value_partition(
        self, spark: SparkSession, uc_schema: str, monkeypatch: pytest.MonkeyPatch
    ):
        """Multiple partition values produce an IN clause."""
        from pyspark.sql import DataFrameWriter

        io = SparkUCTableIoManager(
            catalog="spark_catalog", schema=uc_schema, mode="overwrite"
        )
        io.setup()

        df = spark.createDataFrame(
            [(1, "eu", "x"), (2, "us", "y")], ["id", "region", "val"]
        )

        captured_options: list[tuple[str, str]] = []
        _real_option = DataFrameWriter.option

        def _spy_option(self_w, key, value):
            captured_options.append((key, value))
            return _real_option(self_w, key, value)

        monkeypatch.setattr(DataFrameWriter, "option", _spy_option)
        monkeypatch.setattr(DataFrameWriter, "saveAsTable", lambda *_a, **_kw: None)
        monkeypatch.setattr(io._spark.catalog, "tableExists", lambda _: True)

        io.write(_output_ctx("uc_multi", partition_by=["region"]), df)

        assert ("replaceWhere", "region IN ('eu', 'us')") in captured_options


# ===========================================================================
# Partition-scoped overwrite (backfill safety) – UC Volume Delta
# ===========================================================================


class TestSparkUCVolumeDeltaPartitionScopedOverwrite:
    def test_overwrite_preserves_other_partitions(
        self, spark: SparkSession, tmp_path, monkeypatch
    ):
        """Overwriting one partition must not destroy other partitions' data."""
        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
            mode="overwrite",
        )
        io.setup()
        monkeypatch.setattr(io, "_uri", lambda key: str(tmp_path / key))

        df_us = spark.createDataFrame(
            [(1, "us", "a"), (2, "us", "b")], ["id", "region", "val"]
        )
        io.write(_output_ctx("t", partition_by=["region"]), df_us)

        df_eu = spark.createDataFrame([(3, "eu", "c")], ["id", "region", "val"])
        io.write(_output_ctx("t", partition_by=["region"]), df_eu)

        result = io.read(_input_ctx("t"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 3
        assert [r["region"] for r in rows] == ["us", "us", "eu"]


# ===========================================================================
# Partition-scoped overwrite (backfill safety) – UC Volume Parquet
# ===========================================================================


class TestSparkUCVolumeParquetPartitionScopedOverwrite:
    def test_overwrite_preserves_other_partitions(
        self, spark: SparkSession, tmp_path, monkeypatch
    ):
        """Overwriting one partition must not destroy other partitions' data."""
        io = SparkUCVolumeParquetIoManager(
            catalog="main", schema="staging", volume="raw_data"
        )
        io.setup()
        monkeypatch.setattr(io, "_uri", lambda key: str(tmp_path / key))

        df_us = spark.createDataFrame(
            [(1, "us", "a"), (2, "us", "b")], ["id", "region", "val"]
        )
        io.write(_output_ctx("t", partition_by=["region"]), df_us)

        df_eu = spark.createDataFrame([(3, "eu", "c")], ["id", "region", "val"])
        io.write(_output_ctx("t", partition_by=["region"]), df_eu)

        result = io.read(_input_ctx("t"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 3
        assert [r["region"] for r in rows] == ["us", "us", "eu"]
