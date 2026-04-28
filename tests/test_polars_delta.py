"""Tests for PolarsDeltaIoManager using real Polars + deltalake on local files."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable

from databricks_bundle_decorators.io_manager import (
    InputContext,
    OutputContext,
    _build_replace_where,
)
from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})


def _output_ctx(task_key: str = "my_task", **kwargs: object) -> OutputContext:
    return OutputContext(job_name="j", task_key=task_key, run_id="r1", **kwargs)  # ty: ignore[invalid-argument-type]


def _input_ctx(
    upstream: str = "producer",
    expected_type: type | None = None,
    **kwargs: object,
) -> InputContext:
    return InputContext(
        job_name="j",
        task_key="consumer",
        upstream_task_key=upstream,
        run_id="r1",
        expected_type=expected_type,
        **kwargs,  # ty: ignore[invalid-argument-type]
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_strips_trailing_slash(self) -> None:
        io = PolarsDeltaIoManager(base_path="/data/lake/")
        assert io.base_path == "/data/lake"

    def test_base_path_as_callable(self) -> None:
        io = PolarsDeltaIoManager(base_path=lambda: "/data/lake/")
        assert io.base_path == "/data/lake"

    def test_base_path_callable_invoked_each_time(self) -> None:
        call_count = 0

        def _factory() -> str:
            nonlocal call_count
            call_count += 1
            return f"/data/{call_count}"

        io = PolarsDeltaIoManager(base_path=_factory)
        assert io.base_path == "/data/1"
        assert io.base_path == "/data/2"
        assert call_count == 2

    def test_storage_options_default_none(self) -> None:
        io = PolarsDeltaIoManager(base_path="/data")
        assert io.storage_options is None

    def test_storage_options_as_dict(self) -> None:
        opts = {
            "AZURE_STORAGE_ACCOUNT_NAME": "sa",
            "AZURE_STORAGE_ACCOUNT_KEY": "secret",
        }
        io = PolarsDeltaIoManager(base_path="/data", storage_options=opts)
        assert io.storage_options == opts

    def test_storage_options_as_callable(self) -> None:
        opts = {
            "AZURE_STORAGE_ACCOUNT_NAME": "sa",
            "AZURE_STORAGE_ACCOUNT_KEY": "secret",
        }
        io = PolarsDeltaIoManager(base_path="/data", storage_options=lambda: opts)
        assert io.storage_options == opts

    def test_storage_options_callable_invoked_each_time(self) -> None:
        call_count = 0

        def _factory() -> dict[str, str]:
            nonlocal call_count
            call_count += 1
            return {"key": str(call_count)}

        io = PolarsDeltaIoManager(base_path="/data", storage_options=_factory)
        assert io.storage_options == {"key": "1"}
        assert io.storage_options == {"key": "2"}
        assert call_count == 2

    def test_uri_generation_no_extension(self) -> None:
        io = PolarsDeltaIoManager(base_path="s3://bucket/prefix")
        assert io._uri("extract") == "s3://bucket/prefix/extract"


# ---------------------------------------------------------------------------
# Write + Read round-trips
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_dataframe_round_trip(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.write(_output_ctx("t"), _SAMPLE)

        result = io.read(_input_ctx("t", expected_type=pl.DataFrame))
        assert isinstance(result, pl.DataFrame)
        assert result.equals(_SAMPLE)

    def test_lazyframe_round_trip(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.write(_output_ctx("t"), _SAMPLE.lazy())

        result = io.read(_input_ctx("t"))
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().sort("a").equals(_SAMPLE.sort("a"))

    def test_write_creates_delta_table(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.write(_output_ctx("extract"), _SAMPLE)
        # Delta table is a directory with _delta_log
        assert (tmp_path / "extract" / "_delta_log").is_dir()

    def test_unsupported_type_raises(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path))
        with pytest.raises(TypeError, match="got dict"):
            io.write(_output_ctx(), {"not": "a dataframe"})


# ---------------------------------------------------------------------------
# Read type dispatch
# ---------------------------------------------------------------------------


class TestReadDispatch:
    def test_defaults_to_lazyframe(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        _SAMPLE.write_delta(str(tmp_path / "t"), mode="overwrite")
        result = io.read(_input_ctx("t"))
        assert isinstance(result, pl.LazyFrame)

    def test_dataframe_annotation(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        _SAMPLE.write_delta(str(tmp_path / "t"), mode="overwrite")
        result = io.read(_input_ctx("t", expected_type=pl.DataFrame))
        assert isinstance(result, pl.DataFrame)

    def test_lazyframe_annotation(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        _SAMPLE.write_delta(str(tmp_path / "t"), mode="overwrite")
        result = io.read(_input_ctx("t", expected_type=pl.LazyFrame))
        assert isinstance(result, pl.LazyFrame)


# ---------------------------------------------------------------------------
# mode parameter
# ---------------------------------------------------------------------------


class TestModeParameter:
    def test_default_mode_is_error(self) -> None:
        io = PolarsDeltaIoManager(base_path="/data")
        assert io._mode == "error"

    def test_merge_mode_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match='mode="merge" is not supported'):
            PolarsDeltaIoManager(base_path="/data", mode="merge")

    def test_invalid_mode_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="invalid mode"):
            PolarsDeltaIoManager(base_path="/data", mode="upsert")

    def test_overwrite_mode(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.write(_output_ctx("t"), _SAMPLE)
        # Second write should succeed with overwrite
        io.write(_output_ctx("t"), _SAMPLE)

        result = io.read(_input_ctx("t", expected_type=pl.DataFrame))
        assert result.equals(_SAMPLE)

    def test_append_mode(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="append")
        io.write(_output_ctx("t"), _SAMPLE)
        io.write(_output_ctx("t"), _SAMPLE)

        result = io.read(_input_ctx("t", expected_type=pl.DataFrame))
        assert len(result) == 6  # 3 + 3

    def test_error_mode_raises_on_existing_table(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="error")
        io.write(_output_ctx("t"), _SAMPLE)
        with pytest.raises(Exception):
            io.write(_output_ctx("t"), _SAMPLE)


# ---------------------------------------------------------------------------
# write_options / read_options
# ---------------------------------------------------------------------------


class TestOptions:
    def test_write_options_default_empty(self) -> None:
        io = PolarsDeltaIoManager(base_path="/data")
        assert io._write_options == {}

    def test_read_options_default_empty(self) -> None:
        io = PolarsDeltaIoManager(base_path="/data")
        assert io._read_options == {}


# ---------------------------------------------------------------------------
# TableMerger support
# ---------------------------------------------------------------------------


class TestTableMerger:
    def test_table_merger_executes_merge(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")

        # Write initial data
        initial = pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        io.write(_output_ctx("t"), initial)

        # Build a real merge
        dt = DeltaTable(str(tmp_path / "t"))
        new_data = pl.DataFrame({"id": [2, 4], "val": ["B", "d"]}).to_arrow()
        merger = (
            dt.merge(
                new_data,
                predicate="s.id = t.id",
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
        )

        io.write(_output_ctx("t"), merger)

        result = io.read(_input_ctx("t", expected_type=pl.DataFrame))
        result = result.sort("id")
        assert result["id"].to_list() == [1, 2, 3, 4]
        assert result["val"].to_list() == ["a", "B", "c", "d"]

    def test_table_merger_skips_normal_write(self, tmp_path: Path) -> None:
        """When a TableMerger is written, DataFrame write path is NOT used."""
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")

        initial = pl.DataFrame({"id": [1], "val": ["a"]})
        io.write(_output_ctx("t"), initial)

        dt = DeltaTable(str(tmp_path / "t"))
        new_data = pl.DataFrame({"id": [1], "val": ["A"]}).to_arrow()
        merger = dt.merge(
            new_data,
            predicate="s.id = t.id",
            source_alias="s",
            target_alias="t",
        ).when_matched_update_all()
        # Should succeed without errors — only calls execute()
        io.write(_output_ctx("t"), merger)


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------


class TestPartitioning:
    def test_extracts_partition_values(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        df = pl.DataFrame({"region": ["us", "eu", "us"], "val": [1, 2, 3]})
        ctx = _output_ctx("t", partition_by=["region"])
        io.write(ctx, df)
        assert io._last_partition_values == {"region": ["eu", "us"]}

    def test_partitioned_round_trip(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        df = pl.DataFrame({"region": ["us", "eu", "us"], "val": [1, 2, 3]})
        io.write(_output_ctx("t", partition_by=["region"]), df)
        result = io.read(_input_ctx("t", partition_by=["region"], all_partitions=True))
        collected = result.collect().sort("val")
        assert collected["val"].to_list() == [1, 2, 3]

    def test_no_partition_write_does_not_set_values(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")
        io.write(_output_ctx("t"), _SAMPLE)
        assert io._last_partition_values is None

    def test_sequential_writes_replace_partition_values(self, tmp_path: Path) -> None:
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")

        df1 = pl.DataFrame({"region": ["us"], "val": [1]})
        io.write(_output_ctx("t1", partition_by=["region"]), df1)
        assert io._last_partition_values == {"region": ["us"]}

        df2 = pl.DataFrame({"region": ["eu"], "val": [2]})
        io.write(_output_ctx("t2", partition_by=["region"]), df2)
        assert io._last_partition_values == {"region": ["eu"]}


# ---------------------------------------------------------------------------
# _build_replace_where helper
# ---------------------------------------------------------------------------


class TestBuildReplaceWhere:
    def test_single_column_single_value(self) -> None:
        assert _build_replace_where({"region": ["us"]}) == "region = 'us'"

    def test_single_column_multiple_values(self) -> None:
        result = _build_replace_where({"region": ["eu", "us"]})
        assert result == "region IN ('eu', 'us')"

    def test_multiple_columns(self) -> None:
        result = _build_replace_where(
            {"region": ["us"], "backfill_key": ["2024-01-01"]}
        )
        assert "region = 'us'" in result
        assert "backfill_key = '2024-01-01'" in result
        assert " AND " in result

    def test_escapes_single_quotes(self) -> None:
        result = _build_replace_where({"col": ["it's"]})
        assert result == "col = 'it''s'"


# ---------------------------------------------------------------------------
# Partition-scoped overwrite (backfill safety)
# ---------------------------------------------------------------------------


class TestPartitionScopedOverwrite:
    def test_overwrite_preserves_other_partitions_dataframe(
        self, tmp_path: Path
    ) -> None:
        """Overwriting one partition must not destroy other partitions' data."""
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")

        # Write partition region=us
        df_us = pl.DataFrame({"region": ["us", "us"], "val": [1, 2]})
        io.write(_output_ctx("t", partition_by=["region"]), df_us)

        # Write partition region=eu (should NOT delete region=us)
        df_eu = pl.DataFrame({"region": ["eu"], "val": [3]})
        io.write(_output_ctx("t", partition_by=["region"]), df_eu)

        # Read all partitions — both should be present
        result = io.read(
            _input_ctx("t", expected_type=pl.DataFrame, all_partitions=True)
        )
        assert sorted(result["region"].to_list()) == ["eu", "us", "us"]
        assert sorted(result["val"].to_list()) == [1, 2, 3]

    def test_overwrite_preserves_other_partitions_lazyframe(
        self, tmp_path: Path
    ) -> None:
        """Same test with LazyFrame writes."""
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")

        df_us = pl.DataFrame({"region": ["us"], "val": [1]})
        io.write(_output_ctx("t", partition_by=["region"]), df_us.lazy())

        df_eu = pl.DataFrame({"region": ["eu"], "val": [2]})
        io.write(_output_ctx("t", partition_by=["region"]), df_eu.lazy())

        result = io.read(_input_ctx("t", all_partitions=True)).collect()
        assert sorted(result["region"].to_list()) == ["eu", "us"]

    def test_overwrite_replaces_same_partition(self, tmp_path: Path) -> None:
        """Overwriting the same partition should replace its data."""
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")

        df_old = pl.DataFrame({"region": ["us"], "val": [1]})
        io.write(_output_ctx("t", partition_by=["region"]), df_old)

        df_new = pl.DataFrame({"region": ["us"], "val": [99]})
        io.write(_output_ctx("t", partition_by=["region"]), df_new)

        result = io.read(
            _input_ctx("t", expected_type=pl.DataFrame, all_partitions=True)
        )
        assert result["val"].to_list() == [99]

    def test_overwrite_with_backfill_key_preserves_other_keys(
        self, tmp_path: Path
    ) -> None:
        """Backfill run for one date must not destroy other dates."""
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")

        df_day1 = pl.DataFrame({"val": [10, 20]})
        io.write(
            _output_ctx(
                "t",
                partition_by=["backfill_key"],
                backfill_key="2024-01-01",
            ),
            df_day1,
        )

        df_day2 = pl.DataFrame({"val": [30]})
        io.write(
            _output_ctx(
                "t",
                partition_by=["backfill_key"],
                backfill_key="2024-01-02",
            ),
            df_day2,
        )

        result = io.read(
            _input_ctx("t", expected_type=pl.DataFrame, all_partitions=True)
        )
        assert len(result) == 3
        assert sorted(result["backfill_key"].to_list()) == [
            "2024-01-01",
            "2024-01-01",
            "2024-01-02",
        ]

    def test_overwrite_without_partition_by_replaces_whole_table(
        self, tmp_path: Path
    ) -> None:
        """Without partition_by, overwrite should replace everything (no change)."""
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="overwrite")

        io.write(_output_ctx("t"), pl.DataFrame({"val": [1, 2, 3]}))
        io.write(_output_ctx("t"), pl.DataFrame({"val": [99]}))

        result = io.read(_input_ctx("t", expected_type=pl.DataFrame))
        assert result["val"].to_list() == [99]

    def test_append_mode_not_affected(self, tmp_path: Path) -> None:
        """Append mode should still append, not use replaceWhere."""
        io = PolarsDeltaIoManager(base_path=str(tmp_path), mode="append")

        df1 = pl.DataFrame({"region": ["us"], "val": [1]})
        io.write(_output_ctx("t", partition_by=["region"]), df1)

        df2 = pl.DataFrame({"region": ["us"], "val": [2]})
        io.write(_output_ctx("t", partition_by=["region"]), df2)

        result = io.read(
            _input_ctx("t", expected_type=pl.DataFrame, all_partitions=True)
        )
        assert len(result) == 2
