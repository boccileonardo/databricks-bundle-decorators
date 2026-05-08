"""Tests for SparkDeltaIoManager and SparkServerlessDeltaIoManager."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from databricks_bundle_decorators.io_manager import InputContext, OutputContext
from databricks_bundle_decorators.io_managers import (
    SparkDeltaIoManager,
    SparkServerlessDeltaIoManager,
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
# SparkDeltaIoManager - construction
# ---------------------------------------------------------------------------


class TestSparkDeltaConstruction:
    def test_strips_trailing_slash(self):
        io = SparkDeltaIoManager(base_path="/data/lake/")
        assert io.base_path == "/data/lake"

    def test_base_path_as_callable(self):
        io = SparkDeltaIoManager(base_path=lambda: "/data/lake/")
        assert io.base_path == "/data/lake"

    def test_base_path_callable_invoked_each_time(self):
        call_count = 0

        def _factory() -> str:
            nonlocal call_count
            call_count += 1
            return f"/data/{call_count}"

        io = SparkDeltaIoManager(base_path=_factory)
        assert io.base_path == "/data/1"
        assert io.base_path == "/data/2"
        assert call_count == 2

    def test_spark_configs_default_none(self):
        io = SparkDeltaIoManager(base_path="/data")
        assert io.spark_configs is None

    def test_spark_configs_as_dict(self):
        configs = {"fs.azure.account.key.sa.dfs.core.windows.net": "secret"}
        io = SparkDeltaIoManager(base_path="/data", spark_configs=configs)
        assert io.spark_configs == configs

    def test_spark_configs_as_callable(self):
        configs = {"fs.azure.account.key.sa.dfs.core.windows.net": "secret"}
        io = SparkDeltaIoManager(base_path="/data", spark_configs=lambda: configs)
        assert io.spark_configs == configs

    def test_spark_configs_callable_invoked_each_time(self):
        call_count = 0

        def _factory() -> dict[str, str]:
            nonlocal call_count
            call_count += 1
            return {"key": str(call_count)}

        io = SparkDeltaIoManager(base_path="/data", spark_configs=_factory)
        assert io.spark_configs == {"key": "1"}
        assert io.spark_configs == {"key": "2"}
        assert call_count == 2

    def test_uri_generation(self):
        io = SparkDeltaIoManager(
            base_path="abfss://container@sa.dfs.core.windows.net/prefix"
        )
        assert (
            io._uri("extract")
            == "abfss://container@sa.dfs.core.windows.net/prefix/extract"
        )

    def test_default_mode_is_error(self):
        io = SparkDeltaIoManager(base_path="/data")
        assert io._mode == "error"


class TestSparkServerlessDeltaConstruction:
    def test_strips_trailing_slash(self):
        io = SparkServerlessDeltaIoManager(base_path="/data/lake/")
        assert io.base_path == "/data/lake"

    def test_base_path_as_callable(self):
        io = SparkServerlessDeltaIoManager(base_path=lambda: "/data/lake/")
        assert io.base_path == "/data/lake"

    def test_default_mode_is_error(self):
        io = SparkServerlessDeltaIoManager(base_path="/data")
        assert io._mode == "error"


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


class TestSparkDeltaSetup:
    def test_setup_applies_spark_configs(self, spark: SparkSession):
        io = SparkDeltaIoManager(
            base_path="/data",
            spark_configs={"spark.databricks.test.delta.key1": "v1"},
        )
        io.setup()

        assert spark.conf.get("spark.databricks.test.delta.key1") == "v1"

    def test_setup_no_configs(self, spark: SparkSession):
        io = SparkDeltaIoManager(base_path="/data")
        io.setup()
        assert io._spark is spark


class TestSparkServerlessDeltaSetup:
    def test_setup_obtains_session(self, spark: SparkSession):
        io = SparkServerlessDeltaIoManager(base_path="/data")
        io.setup()
        assert io._spark is spark


# ---------------------------------------------------------------------------
# Round-trip (write + read)
# ---------------------------------------------------------------------------


class TestSparkDeltaRoundTrip:
    def test_basic_round_trip(self, spark: SparkSession, tmp_path):
        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
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
        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
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


class TestSparkServerlessDeltaRoundTrip:
    def test_basic_round_trip(self, spark: SparkSession, tmp_path):
        io = SparkServerlessDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.setup()

        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        io.write(_output_ctx("my_task"), df)

        result = io.read(_input_ctx("my_task"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 2
        assert rows[0]["val"] == "a"


# ---------------------------------------------------------------------------
# Mode parameter
# ---------------------------------------------------------------------------


class TestSparkDeltaMode:
    def test_merge_mode_raises_value_error(self):
        with pytest.raises(ValueError, match='mode="merge" is not supported'):
            SparkDeltaIoManager(base_path="/data", mode="merge")

    def test_serverless_merge_mode_raises_value_error(self):
        with pytest.raises(ValueError, match='mode="merge" is not supported'):
            SparkServerlessDeltaIoManager(base_path="/data", mode="merge")

    def test_invalid_mode_raises_value_error(self):
        with pytest.raises(ValueError, match="invalid mode"):
            SparkDeltaIoManager(base_path="/data", mode="upsert")

    def test_mode_overwrite(self, spark: SparkSession, tmp_path):
        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.setup()

        df1 = spark.createDataFrame([(1, "old")], ["id", "val"])
        io.write(_output_ctx("task"), df1)

        df2 = spark.createDataFrame([(2, "new")], ["id", "val"])
        io.write(_output_ctx("task"), df2)

        result = io.read(_input_ctx("task"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["val"] == "new"

    def test_mode_append(self, spark: SparkSession, tmp_path):
        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="append")
        io.setup()

        df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("task"), df1)

        df2 = spark.createDataFrame([(2, "b")], ["id", "val"])
        io.write(_output_ctx("task"), df2)

        result = io.read(_input_ctx("task"))
        assert result.count() == 2

    def test_mode_error_on_existing(self, spark: SparkSession, tmp_path):
        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="error")
        io.setup()

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("task"), df)

        with pytest.raises(Exception):
            io.write(_output_ctx("task"), df)

    def test_serverless_custom_mode(self, spark: SparkSession, tmp_path):
        io = SparkServerlessDeltaIoManager(base_path=str(tmp_path), mode="append")
        io.setup()

        df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("task"), df1)

        df2 = spark.createDataFrame([(2, "b")], ["id", "val"])
        io.write(_output_ctx("task"), df2)

        result = io.read(_input_ctx("task"))
        assert result.count() == 2


# ---------------------------------------------------------------------------
# DeltaMergeBuilder support
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# DeltaMerge (declarative) with Spark
# ---------------------------------------------------------------------------


class TestSparkDeltaMerge:
    def test_delta_merge_upsert(self, spark: SparkSession, tmp_path):
        """DeltaMerge upsert with Spark DataFrame source."""
        from databricks_bundle_decorators import DeltaMerge  # noqa: PLC0415

        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.setup()

        # Write initial data
        initial = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        io.write(_output_ctx("dm_task"), initial)

        # Build DeltaMerge
        updates = spark.createDataFrame([(2, "B"), (3, "c")], ["id", "val"])
        merge = (
            DeltaMerge(source=updates, predicate="s.id = t.id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
        )

        io.write(_output_ctx("dm_task"), merge)

        assert io._last_partition_values == {}

        result = io.read(_input_ctx("dm_task"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 3
        assert rows[0]["val"] == "a"
        assert rows[1]["val"] == "B"
        assert rows[2]["val"] == "c"

    def test_delta_merge_first_write_creates_table(self, spark: SparkSession, tmp_path):
        """DeltaMerge on non-existent table does an initial write."""
        from databricks_bundle_decorators import DeltaMerge  # noqa: PLC0415

        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.setup()

        source = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "val"])
        merge = (
            DeltaMerge(source=source, predicate="s.id = t.id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
        )

        io.write(_output_ctx("new_task"), merge)

        result = io.read(_input_ctx("new_task"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 2
        assert rows[0]["val"] == "x"
        assert rows[1]["val"] == "y"

    def test_delta_merge_matched_delete(self, spark: SparkSession, tmp_path):
        """DeltaMerge with when_matched_delete removes matching rows."""
        from databricks_bundle_decorators import DeltaMerge  # noqa: PLC0415

        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.setup()

        initial = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "val"])
        io.write(_output_ctx("del_task"), initial)

        # Delete rows with id=2
        deletes = spark.createDataFrame([(2, "ignored")], ["id", "val"])
        merge = DeltaMerge(
            source=deletes, predicate="s.id = t.id"
        ).when_matched_delete()

        io.write(_output_ctx("del_task"), merge)

        result = io.read(_input_ctx("del_task"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 2
        assert rows[0]["id"] == 1
        assert rows[1]["id"] == 3

    def test_delta_merge_first_write_then_merge(self, spark: SparkSession, tmp_path):
        """First write creates table, second write merges."""
        from databricks_bundle_decorators import DeltaMerge  # noqa: PLC0415

        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.setup()

        # First write — table doesn't exist yet
        source1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        merge1 = (
            DeltaMerge(source=source1, predicate="s.id = t.id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
        )
        io.write(_output_ctx("fw_task"), merge1)

        # Second write — table exists, merge
        source2 = spark.createDataFrame([(1, "A"), (2, "b")], ["id", "val"])
        merge2 = (
            DeltaMerge(source=source2, predicate="s.id = t.id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
        )
        io.write(_output_ctx("fw_task"), merge2)

        result = io.read(_input_ctx("fw_task"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 2
        assert rows[0]["val"] == "A"
        assert rows[1]["val"] == "b"

    def test_delta_merge_first_write_respects_partition_by(
        self, spark: SparkSession, tmp_path
    ):
        """DeltaMerge initial write creates a partitioned Delta table."""
        from databricks_bundle_decorators import DeltaMerge  # noqa: PLC0415

        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.setup()

        source = spark.createDataFrame(
            [(1, "us", "a"), (2, "eu", "b")], ["id", "region", "val"]
        )
        merge = (
            DeltaMerge(source=source, predicate="s.id = t.id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
        )

        io.write(_output_ctx("pt_task", partition_by=["region"]), merge)

        # Verify partition directories exist
        task_dir = tmp_path / "pt_task"
        partition_dirs = sorted(
            d.name
            for d in task_dir.iterdir()
            if d.is_dir() and d.name.startswith("region=")
        )
        assert partition_dirs == ["region=eu", "region=us"]


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------


class TestSparkDeltaPartitioning:
    def test_partition_by_single_column(self, spark: SparkSession, tmp_path):
        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.setup()

        df = spark.createDataFrame(
            [(1, "us", "a"), (2, "eu", "b"), (3, "us", "c")],
            ["id", "region", "val"],
        )
        ctx = _output_ctx("partitioned", partition_by=["region"])
        io.write(ctx, df)

        # Verify partition directories
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
        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
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
        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
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
        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
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
        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.setup()

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("no_part"), df)
        assert io._last_partition_values is None


# ---------------------------------------------------------------------------
# Write / read options
# ---------------------------------------------------------------------------


class TestSparkDeltaOptions:
    def test_write_options_applied(self, spark: SparkSession, tmp_path):
        io = SparkDeltaIoManager(
            base_path=str(tmp_path),
            mode="overwrite",
            write_options={"mergeSchema": "true"},
        )
        io.setup()

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io.write(_output_ctx("opts"), df)

        result = io.read(_input_ctx("opts"))
        assert result.count() == 1

    def test_read_options_applied(self, spark: SparkSession, tmp_path):
        io_write = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io_write.setup()

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        io_write.write(_output_ctx("read_opts"), df)

        io_read = SparkDeltaIoManager(
            base_path=str(tmp_path),
            read_options={"versionAsOf": "0"},
        )
        io_read.setup()

        result = io_read.read(_input_ctx("read_opts"))
        assert result.count() == 1


# ---------------------------------------------------------------------------
# Partition-scoped overwrite (backfill safety)
# ---------------------------------------------------------------------------


class TestSparkDeltaPartitionScopedOverwrite:
    def test_overwrite_preserves_other_partitions(self, spark: SparkSession, tmp_path):
        """Overwriting one partition must not destroy other partitions' data."""
        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.setup()

        # Write partition region=us
        df_us = spark.createDataFrame(
            [(1, "us", "a"), (2, "us", "b")], ["id", "region", "val"]
        )
        io.write(_output_ctx("t", partition_by=["region"]), df_us)

        # Write partition region=eu (should NOT delete region=us)
        df_eu = spark.createDataFrame([(3, "eu", "c")], ["id", "region", "val"])
        io.write(_output_ctx("t", partition_by=["region"]), df_eu)

        # Read all data — both partitions should be present
        result = io.read(_input_ctx("t"))
        rows = sorted(result.collect(), key=lambda r: r["id"])
        assert len(rows) == 3
        assert [r["region"] for r in rows] == ["us", "us", "eu"]

    def test_overwrite_replaces_same_partition(self, spark: SparkSession, tmp_path):
        """Overwriting the same partition should replace its data."""
        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
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
        io = SparkDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
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
        """SparkServerlessDeltaIoManager should also scope overwrites."""
        io = SparkServerlessDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.setup()

        df_us = spark.createDataFrame([(1, "us")], ["id", "region"])
        io.write(_output_ctx("t", partition_by=["region"]), df_us)

        df_eu = spark.createDataFrame([(2, "eu")], ["id", "region"])
        io.write(_output_ctx("t", partition_by=["region"]), df_eu)

        result = io.read(_input_ctx("t"))
        assert result.count() == 2
