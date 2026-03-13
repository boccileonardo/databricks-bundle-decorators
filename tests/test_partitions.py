"""Tests for partition definitions and current_logical_date helper."""

from __future__ import annotations

import warnings

import pytest
import whenever

from databricks_bundle_decorators.context import _populate_params
from databricks_bundle_decorators.partitions import (
    DailyPartition,
    HourlyPartition,
    MonthlyPartition,
    PartitionDef,
    StaticPartition,
    WeeklyPartition,
    current_logical_date,
)


class TestDailyPartition:
    def test_basic_range(self):
        p = DailyPartition(start_date="2024-01-01", end_date="2024-01-05")
        keys = p.partition_keys()
        assert keys == [
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
        ]

    def test_override_start_end(self):
        p = DailyPartition(start_date="2024-01-01", end_date="2024-12-31")
        keys = p.partition_keys(start="2024-03-01", end="2024-03-03")
        assert keys == ["2024-03-01", "2024-03-02", "2024-03-03"]

    def test_single_day(self):
        p = DailyPartition(start_date="2024-06-15", end_date="2024-06-15")
        assert p.partition_keys() == ["2024-06-15"]

    def test_empty_range(self):
        p = DailyPartition(start_date="2024-06-15", end_date="2024-06-14")
        assert p.partition_keys() == []

    def test_default_end_is_yesterday(self):
        yesterday = whenever.ZonedDateTime.now("UTC").date().subtract(days=1)
        key = yesterday.py_date().strftime("%Y-%m-%d")
        p = DailyPartition(start_date=key)
        keys = p.partition_keys()
        assert keys == [key]

    def test_custom_format(self):
        p = DailyPartition(
            start_date="01/01/2024",
            end_date="01/03/2024",
            fmt="%m/%d/%Y",
        )
        keys = p.partition_keys()
        assert keys == ["01/01/2024", "01/02/2024", "01/03/2024"]

    def test_is_frozen(self):
        p = DailyPartition(start_date="2024-01-01")
        with pytest.raises(AttributeError):
            p.start_date = "2024-02-01"  # type: ignore[misc]

    def test_is_partition_def(self):
        p = DailyPartition(start_date="2024-01-01")
        assert isinstance(p, PartitionDef)


class TestWeeklyPartition:
    def test_basic_range(self):
        p = WeeklyPartition(start_date="2024-W01", end_date="2024-W04")
        keys = p.partition_keys()
        assert len(keys) == 4
        assert keys[0] == "2024-W01"
        assert keys[-1] == "2024-W04"

    def test_override_range(self):
        p = WeeklyPartition(start_date="2024-W01", end_date="2024-W52")
        keys = p.partition_keys(start="2024-W10", end="2024-W12")
        assert len(keys) == 3

    def test_single_week(self):
        p = WeeklyPartition(start_date="2024-W05", end_date="2024-W05")
        assert len(p.partition_keys()) == 1


class TestMonthlyPartition:
    def test_basic_range(self):
        p = MonthlyPartition(start_date="2024-01", end_date="2024-06")
        keys = p.partition_keys()
        assert keys == [
            "2024-01",
            "2024-02",
            "2024-03",
            "2024-04",
            "2024-05",
            "2024-06",
        ]

    def test_cross_year_boundary(self):
        p = MonthlyPartition(start_date="2023-11", end_date="2024-02")
        keys = p.partition_keys()
        assert keys == ["2023-11", "2023-12", "2024-01", "2024-02"]

    def test_override_range(self):
        p = MonthlyPartition(start_date="2024-01", end_date="2024-12")
        keys = p.partition_keys(start="2024-03", end="2024-05")
        assert keys == ["2024-03", "2024-04", "2024-05"]

    def test_single_month(self):
        p = MonthlyPartition(start_date="2024-07", end_date="2024-07")
        keys = p.partition_keys()
        assert keys == ["2024-07"]


class TestHourlyPartition:
    def test_basic_range(self):
        p = HourlyPartition(
            start_date="2024-01-01T00",
            end_date="2024-01-01T05",
        )
        keys = p.partition_keys()
        assert len(keys) == 6
        assert keys[0] == "2024-01-01T00"
        assert keys[-1] == "2024-01-01T05"

    def test_cross_day_boundary(self):
        p = HourlyPartition(
            start_date="2024-01-01T22",
            end_date="2024-01-02T02",
        )
        keys = p.partition_keys()
        assert len(keys) == 5
        assert "2024-01-01T23" in keys
        assert "2024-01-02T00" in keys

    def test_utc_default(self):
        p = HourlyPartition(start_date="2024-01-01T00")
        assert p.tz == "UTC"

    def test_custom_timezone(self):
        p = HourlyPartition(
            start_date="2024-01-01T00",
            end_date="2024-01-01T03",
            tz="America/New_York",
        )
        keys = p.partition_keys()
        assert len(keys) == 4

    def test_override_range(self):
        p = HourlyPartition(
            start_date="2024-01-01T00",
            end_date="2024-01-01T23",
        )
        keys = p.partition_keys(start="2024-01-01T10", end="2024-01-01T12")
        assert len(keys) == 3


class TestStaticPartition:
    def test_basic(self):
        p = StaticPartition(keys=["us", "eu", "jp"])
        assert p.partition_keys() == ["us", "eu", "jp"]

    def test_start_end_ignored(self):
        p = StaticPartition(keys=["a", "b"])
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = p.partition_keys(start="x", end="y")
        assert result == ["a", "b"]
        assert len(w) == 1
        assert "ignores" in str(w[0].message)

    def test_defensive_copy(self):
        original = ["a", "b"]
        p = StaticPartition(keys=original)
        original.append("c")
        assert p.keys == ["a", "b"]

    def test_is_frozen(self):
        p = StaticPartition(keys=["a"])
        with pytest.raises(AttributeError):
            p.keys = ["b"]  # type: ignore[misc]


class TestCurrentLogicalDate:
    def test_reads_from_params(self):
        _populate_params({"logical_date": "2024-01-15T00:00:00+00:00"})
        result = current_logical_date()
        from datetime import datetime, timezone

        assert result == datetime(2024, 1, 15, tzinfo=timezone.utc)

    def test_empty_raises(self):
        _populate_params({"logical_date": ""})
        with pytest.raises(RuntimeError, match="logical_date is not set"):
            current_logical_date()

    def test_missing_raises(self):
        _populate_params({})
        with pytest.raises(RuntimeError, match="logical_date is not set"):
            current_logical_date()

    def teardown_method(self):
        _populate_params({})


class TestTimezoneAwareDefaults:
    """Tests for tz parameter on time-based partitions."""

    def test_daily_tz_utc_is_default(self):
        """DailyPartition defaults to tz='UTC'."""
        p = DailyPartition(start_date="2024-01-01", end_date="2024-01-03")
        assert p.tz == "UTC"
        keys = p.partition_keys()
        assert keys == ["2024-01-01", "2024-01-02", "2024-01-03"]

    def test_weekly_tz_default_utc(self):
        from databricks_bundle_decorators.partitions import WeeklyPartition

        p = WeeklyPartition(start_date="2024-W01", end_date="2024-W04")
        assert p.tz == "UTC"
        keys = p.partition_keys()
        assert len(keys) == 4

    def test_monthly_tz_default_utc(self):
        from databricks_bundle_decorators.partitions import MonthlyPartition

        p = MonthlyPartition(start_date="2024-01", end_date="2024-06")
        assert p.tz == "UTC"
        keys = p.partition_keys()
        assert len(keys) == 6

    def test_hourly_fold_deterministic(self):
        """HourlyPartition with a DST-ambiguous hour should not crash."""
        p = HourlyPartition(
            start_date="2024-11-03T00",
            end_date="2024-11-03T03",
            tz="America/New_York",
        )
        keys = p.partition_keys()
        assert len(keys) >= 4  # fall-back produces extra hour
