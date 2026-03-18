"""Tests for PolarsParquetIoManager using real Polars I/O on local files."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from databricks_bundle_decorators.io_manager import InputContext, OutputContext
from databricks_bundle_decorators.io_managers import PolarsParquetIoManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})


def _output_ctx(task_key: str = "my_task", **kwargs: object) -> OutputContext:
    return OutputContext(job_name="j", task_key=task_key, run_id="r1", **kwargs)  # type: ignore[arg-type]


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
        **kwargs,  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_strips_trailing_slash(self) -> None:
        io = PolarsParquetIoManager(base_path="/data/lake/")
        assert io.base_path == "/data/lake"

    def test_storage_options_default_none(self) -> None:
        io = PolarsParquetIoManager(base_path="/data")
        assert io.storage_options is None

    def test_storage_options_as_dict(self) -> None:
        opts = {"account_name": "sa", "account_key": "secret"}
        io = PolarsParquetIoManager(base_path="/data", storage_options=opts)
        assert io.storage_options == opts

    def test_storage_options_as_callable(self) -> None:
        opts = {"account_name": "sa", "account_key": "secret"}
        io = PolarsParquetIoManager(base_path="/data", storage_options=lambda: opts)
        assert io.storage_options == opts

    def test_storage_options_callable_invoked_each_time(self) -> None:
        call_count = 0

        def _factory() -> dict[str, str]:
            nonlocal call_count
            call_count += 1
            return {"key": str(call_count)}

        io = PolarsParquetIoManager(base_path="/data", storage_options=_factory)
        assert io.storage_options == {"key": "1"}
        assert io.storage_options == {"key": "2"}
        assert call_count == 2

    def test_uri_generation(self) -> None:
        io = PolarsParquetIoManager(base_path="s3://bucket/prefix")
        assert io._uri("extract") == "s3://bucket/prefix/extract"


# ---------------------------------------------------------------------------
# Write + Read round-trips
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_dataframe_round_trip(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))
        io.write(_output_ctx("t"), _SAMPLE)

        result = io.read(_input_ctx("t", expected_type=pl.DataFrame))
        assert isinstance(result, pl.DataFrame)
        assert result.equals(_SAMPLE)

    def test_lazyframe_round_trip(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))
        io.write(_output_ctx("t"), _SAMPLE.lazy())

        result = io.read(_input_ctx("t"))
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().equals(_SAMPLE)

    def test_write_creates_parquet_file(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))
        io.write(_output_ctx("extract"), _SAMPLE)
        assert (tmp_path / "extract.parquet").exists()

    def test_unsupported_type_raises(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))
        with pytest.raises(TypeError, match="got dict"):
            io.write(_output_ctx(), {"not": "a dataframe"})


# ---------------------------------------------------------------------------
# Read type dispatch
# ---------------------------------------------------------------------------


class TestReadDispatch:
    def test_defaults_to_lazyframe(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))
        _SAMPLE.write_parquet(tmp_path / "t.parquet")
        result = io.read(_input_ctx("t"))
        assert isinstance(result, pl.LazyFrame)

    def test_dataframe_annotation(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))
        _SAMPLE.write_parquet(tmp_path / "t.parquet")
        result = io.read(_input_ctx("t", expected_type=pl.DataFrame))
        assert isinstance(result, pl.DataFrame)

    def test_lazyframe_annotation(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))
        _SAMPLE.write_parquet(tmp_path / "t.parquet")
        result = io.read(_input_ctx("t", expected_type=pl.LazyFrame))
        assert isinstance(result, pl.LazyFrame)


# ---------------------------------------------------------------------------
# write_options / read_options
# ---------------------------------------------------------------------------


class TestOptions:
    def test_write_options_applied(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(
            base_path=str(tmp_path),
            write_options={"compression": "zstd"},
        )
        io.write(_output_ctx("t"), _SAMPLE)
        result = pl.read_parquet(tmp_path / "t.parquet")
        assert result.equals(_SAMPLE)

    def test_read_options_applied(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(
            base_path=str(tmp_path),
            read_options={"n_rows": 1},
        )
        _SAMPLE.write_parquet(tmp_path / "t.parquet")
        result = io.read(_input_ctx("t", expected_type=pl.DataFrame))
        assert len(result) == 1

    def test_write_options_default_empty(self) -> None:
        io = PolarsParquetIoManager(base_path="/data")
        assert io._write_options == {}

    def test_read_options_default_empty(self) -> None:
        io = PolarsParquetIoManager(base_path="/data")
        assert io._read_options == {}


# ---------------------------------------------------------------------------
# Partitioning (Hive-style)
# ---------------------------------------------------------------------------


class TestPartitioning:
    def test_dataframe_creates_hive_dirs(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))
        df = pl.DataFrame({"region": ["us", "eu"], "val": [1, 2]})
        io.write(_output_ctx("t", partition_by=["region"]), df)
        assert (tmp_path / "t" / "region=eu").is_dir()
        assert (tmp_path / "t" / "region=us").is_dir()

    def test_lazyframe_creates_hive_dirs(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))
        lf = pl.LazyFrame({"region": ["us", "eu"], "val": [1, 2]})
        io.write(_output_ctx("t", partition_by=["region"]), lf)
        assert (tmp_path / "t" / "region=eu").is_dir()
        assert (tmp_path / "t" / "region=us").is_dir()

    def test_extracts_partition_values(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))
        df = pl.DataFrame({"region": ["us", "eu", "us"], "val": [1, 2, 3]})
        ctx = _output_ctx("t", partition_by=["region"])
        io.write(ctx, df)
        assert io._last_partition_values == {"region": ["eu", "us"]}

    def test_partitioned_round_trip(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))
        df = pl.DataFrame({"region": ["us", "eu", "us"], "val": [1, 2, 3]})
        io.write(_output_ctx("t", partition_by=["region"]), df)
        result = io.read(_input_ctx("t", partition_by=["region"], all_partitions=True))
        collected = result.collect().sort("val")
        assert collected["val"].to_list() == [1, 2, 3]

    def test_partition_filter_on_read(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))
        df = pl.DataFrame({"region": ["us", "eu", "us"], "val": [1, 2, 3]})
        io.write(_output_ctx("t", partition_by=["region"]), df)
        result = io.read(
            _input_ctx(
                "t",
                partition_by=["region"],
                partition_filter={"region": ["us"]},
            )
        )
        collected = result.collect().sort("val")
        assert collected["region"].to_list() == ["us", "us"]
        assert collected["val"].to_list() == [1, 3]

    def test_no_partition_write_does_not_set_values(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))
        io.write(_output_ctx("t"), _SAMPLE)
        assert io._last_partition_values is None

    def test_sequential_writes_replace_partition_values(self, tmp_path: Path) -> None:
        io = PolarsParquetIoManager(base_path=str(tmp_path))

        df1 = pl.DataFrame({"region": ["us"], "val": [1]})
        io.write(_output_ctx("t1", partition_by=["region"]), df1)
        assert io._last_partition_values == {"region": ["us"]}

        df2 = pl.DataFrame({"region": ["eu"], "val": [2]})
        io.write(_output_ctx("t2", partition_by=["region"]), df2)
        assert io._last_partition_values == {"region": ["eu"]}


# ---------------------------------------------------------------------------
# Partition-scoped overwrite (backfill safety)
# ---------------------------------------------------------------------------


class TestPartitionScopedOverwrite:
    def test_overwrite_preserves_other_partitions_dataframe(
        self, tmp_path: Path
    ) -> None:
        """Overwriting one partition must not destroy other partitions' data."""
        io = PolarsParquetIoManager(base_path=str(tmp_path))

        df_us = pl.DataFrame({"region": ["us", "us"], "val": [1, 2]})
        io.write(_output_ctx("t", partition_by=["region"]), df_us)

        df_eu = pl.DataFrame({"region": ["eu"], "val": [3]})
        io.write(_output_ctx("t", partition_by=["region"]), df_eu)

        result = io.read(
            _input_ctx(
                "t",
                expected_type=pl.DataFrame,
                partition_by=["region"],
                all_partitions=True,
            )
        )
        assert sorted(result["region"].to_list()) == ["eu", "us", "us"]
        assert sorted(result["val"].to_list()) == [1, 2, 3]

    def test_overwrite_preserves_other_partitions_lazyframe(
        self, tmp_path: Path
    ) -> None:
        """Same test with LazyFrame writes."""
        io = PolarsParquetIoManager(base_path=str(tmp_path))

        df_us = pl.DataFrame({"region": ["us"], "val": [1]})
        io.write(_output_ctx("t", partition_by=["region"]), df_us.lazy())

        df_eu = pl.DataFrame({"region": ["eu"], "val": [2]})
        io.write(_output_ctx("t", partition_by=["region"]), df_eu.lazy())

        result = io.read(
            _input_ctx("t", partition_by=["region"], all_partitions=True)
        ).collect()
        assert sorted(result["region"].to_list()) == ["eu", "us"]

    def test_overwrite_replaces_same_partition(self, tmp_path: Path) -> None:
        """Overwriting the same partition should replace its data."""
        io = PolarsParquetIoManager(base_path=str(tmp_path))

        df_old = pl.DataFrame({"region": ["us"], "val": [1]})
        io.write(_output_ctx("t", partition_by=["region"]), df_old)

        df_new = pl.DataFrame({"region": ["us"], "val": [99]})
        io.write(_output_ctx("t", partition_by=["region"]), df_new)

        result = io.read(
            _input_ctx(
                "t",
                expected_type=pl.DataFrame,
                partition_by=["region"],
                all_partitions=True,
            )
        )
        assert result["val"].to_list() == [99]

    def test_overwrite_with_backfill_key_preserves_other_keys(
        self, tmp_path: Path
    ) -> None:
        """Backfill run for one date must not destroy other dates."""
        io = PolarsParquetIoManager(base_path=str(tmp_path))

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
            _input_ctx(
                "t",
                expected_type=pl.DataFrame,
                partition_by=["backfill_key"],
                all_partitions=True,
            )
        )
        assert len(result) == 3
        assert sorted(result["backfill_key"].to_list()) == [
            "2024-01-01",
            "2024-01-01",
            "2024-01-02",
        ]
