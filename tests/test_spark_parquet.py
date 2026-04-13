"""Tests for SparkParquetIoManager and SparkServerlessParquetIoManager."""

from __future__ import annotations

from pyspark.sql import SparkSession

from databricks_bundle_decorators.io_manager import InputContext, OutputContext
from databricks_bundle_decorators.io_managers import (
    SparkParquetIoManager,
    SparkServerlessParquetIoManager,
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


# ---------------------------------------------------------------------------
# SparkParquetIoManager - construction
# ---------------------------------------------------------------------------


class TestSparkParquetConstruction:
    def test_strips_trailing_slash(self):
        io = SparkParquetIoManager(base_path="/data/lake/")
        assert io.base_path == "/data/lake"

    def test_base_path_as_callable(self):
        io = SparkParquetIoManager(base_path=lambda: "/data/lake/")
        assert io.base_path == "/data/lake"

    def test_base_path_callable_invoked_each_time(self):
        call_count = 0

        def _factory() -> str:
            nonlocal call_count
            call_count += 1
            return f"/data/{call_count}"

        io = SparkParquetIoManager(base_path=_factory)
        assert io.base_path == "/data/1"
        assert io.base_path == "/data/2"
        assert call_count == 2

    def test_spark_configs_default_none(self):
        io = SparkParquetIoManager(base_path="/data")
        assert io.spark_configs is None

    def test_spark_configs_as_dict(self):
        configs = {"fs.azure.account.key.sa.dfs.core.windows.net": "secret"}
        io = SparkParquetIoManager(base_path="/data", spark_configs=configs)
        assert io.spark_configs == configs

    def test_spark_configs_as_callable(self):
        configs = {"fs.azure.account.key.sa.dfs.core.windows.net": "secret"}
        io = SparkParquetIoManager(base_path="/data", spark_configs=lambda: configs)
        assert io.spark_configs == configs

    def test_spark_configs_callable_invoked_each_time(self):
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
        io = SparkParquetIoManager(
            base_path="abfss://container@sa.dfs.core.windows.net/prefix",
        )
        assert (
            io._uri("extract")
            == "abfss://container@sa.dfs.core.windows.net/prefix/extract"
        )


class TestSparkServerlessParquetConstruction:
    def test_strips_trailing_slash(self):
        io = SparkServerlessParquetIoManager(base_path="/data/lake/")
        assert io.base_path == "/data/lake"

    def test_base_path_as_callable(self):
        io = SparkServerlessParquetIoManager(base_path=lambda: "/data/lake/")
        assert io.base_path == "/data/lake"


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


class TestSparkParquetSetup:
    def test_setup_applies_spark_configs(self, spark: SparkSession):
        io = SparkParquetIoManager(
            base_path="/data",
            spark_configs={"spark.databricks.test.parquet.key1": "v1"},
        )
        io.setup()

        assert spark.conf.get("spark.databricks.test.parquet.key1") == "v1"

    def test_setup_no_configs(self, spark: SparkSession):
        io = SparkParquetIoManager(base_path="/data")
        io.setup()
        assert io._spark is spark


class TestSparkServerlessParquetSetup:
    def test_setup_obtains_session(self, spark: SparkSession):
        io = SparkServerlessParquetIoManager(base_path="/data")
        io.setup()
        assert io._spark is spark


# ---------------------------------------------------------------------------
# Round-trip (write + read)
# ---------------------------------------------------------------------------


class TestSparkParquetRoundTrip:
    def test_basic_round_trip(self, spark: SparkSession, tmp_path):
        io = SparkParquetIoManager(base_path=str(tmp_path))
        io.setup()

        df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "val"])
        io.write(_output_ctx("my_task"), df)

        result = io.read(_input_ctx("my_task"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 3
        assert rows[0]["id"] == 1
        assert rows[0]["val"] == "a"
        assert rows[2]["id"] == 3

    def test_round_trip_preserves_types(self, spark: SparkSession, tmp_path):
        io = SparkParquetIoManager(base_path=str(tmp_path))
        io.setup()

        df = spark.createDataFrame(
            [(1, 2.5, True, "x")], ["int_col", "float_col", "bool_col", "str_col"]
        )
        io.write(_output_ctx("typed"), df)

        result = io.read(_input_ctx("typed"))
        row = result.collect()[0]
        assert row["int_col"] == 1
        assert row["float_col"] == 2.5
        assert row["bool_col"] is True
        assert row["str_col"] == "x"

    def test_overwrite_mode(self, spark: SparkSession, tmp_path):
        """Parquet IoManager always uses overwrite mode."""
        io = SparkParquetIoManager(base_path=str(tmp_path))
        io.setup()

        df1 = spark.createDataFrame([(1, "old")], ["id", "val"])
        io.write(_output_ctx("task"), df1)

        df2 = spark.createDataFrame([(2, "new")], ["id", "val"])
        io.write(_output_ctx("task"), df2)

        result = io.read(_input_ctx("task"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["val"] == "new"


class TestSparkServerlessParquetRoundTrip:
    def test_basic_round_trip(self, spark: SparkSession, tmp_path):
        io = SparkServerlessParquetIoManager(base_path=str(tmp_path))
        io.setup()

        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        io.write(_output_ctx("my_task"), df)

        result = io.read(_input_ctx("my_task"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 2
        assert rows[0]["val"] == "a"


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------


class TestSparkParquetPartitioning:
    def test_partition_by_single_column(self, spark: SparkSession, tmp_path):
        io = SparkParquetIoManager(base_path=str(tmp_path))
        io.setup()

        df = spark.createDataFrame(
            [(1, "us", "a"), (2, "eu", "b"), (3, "us", "c")],
            ["id", "region", "val"],
        )
        ctx = _output_ctx("partitioned", partition_by=["region"])
        io.write(ctx, df)

        # Verify partition directories were created
        task_dir = tmp_path / "partitioned"
        partition_dirs = sorted(
            d.name
            for d in task_dir.iterdir()
            if d.is_dir() and d.name.startswith("region=")
        )
        assert "region=eu" in partition_dirs
        assert "region=us" in partition_dirs

        # Read back all data
        result = io.read(_input_ctx("partitioned"))
        assert result.count() == 3

    def test_partition_by_multiple_columns(self, spark: SparkSession, tmp_path):
        io = SparkParquetIoManager(base_path=str(tmp_path))
        io.setup()

        df = spark.createDataFrame(
            [
                (1, "us", "2024-01-01", "a"),
                (2, "eu", "2024-01-01", "b"),
                (3, "us", "2024-01-02", "c"),
            ],
            ["id", "region", "date", "val"],
        )
        ctx = _output_ctx("multi_part", partition_by=["region", "date"])
        io.write(ctx, df)

        result = io.read(_input_ctx("multi_part"))
        assert result.count() == 3

    def test_partition_values_extracted(self, spark: SparkSession, tmp_path):
        io = SparkParquetIoManager(base_path=str(tmp_path))
        io.setup()

        df = spark.createDataFrame(
            [(1, "us"), (2, "eu"), (3, "us")],
            ["id", "region"],
        )
        ctx = _output_ctx("extract_vals", partition_by=["region"])
        io.write(ctx, df)

        pv = io._extract_partition_values(ctx)
        assert pv == {"region": ["eu", "us"]}

    def test_sequential_writes_replace_partition_values(
        self, spark: SparkSession, tmp_path
    ):
        io = SparkParquetIoManager(base_path=str(tmp_path))
        io.setup()

        df1 = spark.createDataFrame([(1, "us")], ["id", "region"])
        ctx1 = _output_ctx("seq", partition_by=["region"])
        io.write(ctx1, df1)
        assert io._extract_partition_values(ctx1) == {"region": ["us"]}

        df2 = spark.createDataFrame([(2, "eu"), (3, "ap")], ["id", "region"])
        ctx2 = _output_ctx("seq2", partition_by=["region"])
        io.write(ctx2, df2)
        assert io._extract_partition_values(ctx2) == {"region": ["ap", "eu"]}

    def test_no_partition_write_does_not_set_values(
        self, spark: SparkSession, tmp_path
    ):
        io = SparkParquetIoManager(base_path=str(tmp_path))
        io.setup()

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("no_part"), df)
        assert io._last_partition_values is None


# ---------------------------------------------------------------------------
# Write / read options
# ---------------------------------------------------------------------------


class TestSparkParquetOptions:
    def test_write_options_applied(self, spark: SparkSession, tmp_path):
        io = SparkParquetIoManager(
            base_path=str(tmp_path),
            write_options={"compression": "gzip"},
        )
        io.setup()

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("opts"), df)

        # Verify round-trip still works with gzip compression
        result = io.read(_input_ctx("opts"))
        assert result.count() == 1

    def test_read_options_applied(self, spark: SparkSession, tmp_path):
        io = SparkParquetIoManager(
            base_path=str(tmp_path),
            read_options={"mergeSchema": "true"},
        )
        io.setup()

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("read_opts"), df)

        result = io.read(_input_ctx("read_opts"))
        assert result.count() == 1


# ---------------------------------------------------------------------------
# Partition-scoped overwrite (backfill safety)
# ---------------------------------------------------------------------------


class TestSparkParquetPartitionScopedOverwrite:
    def test_overwrite_preserves_other_partitions(self, spark: SparkSession, tmp_path):
        """Overwriting one partition must not destroy other partitions' data."""
        io = SparkParquetIoManager(base_path=str(tmp_path))
        io.setup()

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

    def test_overwrite_replaces_same_partition(self, spark: SparkSession, tmp_path):
        """Overwriting the same partition should replace its data."""
        io = SparkParquetIoManager(base_path=str(tmp_path))
        io.setup()

        df_old = spark.createDataFrame([(1, "us", "old")], ["id", "region", "val"])
        io.write(_output_ctx("t", partition_by=["region"]), df_old)

        df_new = spark.createDataFrame([(2, "us", "new")], ["id", "region", "val"])
        io.write(_output_ctx("t", partition_by=["region"]), df_new)

        result = io.read(_input_ctx("t"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["val"] == "new"

    def test_overwrite_with_backfill_key_preserves_other_keys(
        self, spark: SparkSession, tmp_path
    ):
        """Backfill run for one date must not destroy other dates."""
        io = SparkParquetIoManager(base_path=str(tmp_path))
        io.setup()

        df_day1 = spark.createDataFrame([(1, 10), (2, 20)], ["id", "val"])
        io.write(
            _output_ctx(
                "t",
                partition_by=["backfill_key"],
                backfill_key="2024-01-01",
            ),
            df_day1,
        )

        df_day2 = spark.createDataFrame([(3, 30)], ["id", "val"])
        io.write(
            _output_ctx(
                "t",
                partition_by=["backfill_key"],
                backfill_key="2024-01-02",
            ),
            df_day2,
        )

        result = io.read(_input_ctx("t"))
        assert result.count() == 3

    def test_serverless_overwrite_preserves_other_partitions(
        self, spark: SparkSession, tmp_path
    ):
        """SparkServerlessParquetIoManager should also scope overwrites."""
        io = SparkServerlessParquetIoManager(base_path=str(tmp_path))
        io.setup()

        df_us = spark.createDataFrame([(1, "us")], ["id", "region"])
        io.write(_output_ctx("t", partition_by=["region"]), df_us)

        df_eu = spark.createDataFrame([(2, "eu")], ["id", "region"])
        io.write(_output_ctx("t", partition_by=["region"]), df_eu)

        result = io.read(_input_ctx("t"))
        assert result.count() == 2
