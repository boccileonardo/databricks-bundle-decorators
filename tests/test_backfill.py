"""Tests for backfill definitions and get_run_logical_date helper."""

from __future__ import annotations

import warnings
from datetime import UTC, datetime

import pytest
import whenever

from databricks_bundle_decorators.backfill import (
    BackfillDef,
    DailyBackfill,
    HourlyBackfill,
    MonthlyBackfill,
    StaticBackfill,
    WeeklyBackfill,
    _parse_logical_date_str,
    get_run_logical_date,
)
from databricks_bundle_decorators.context import _populate_params
from databricks_bundle_decorators.registry import (
    _JOB_REGISTRY,
    JobMeta,
    reset_registries,
)


class TestDailyBackfill:
    def test_basic_range(self):
        p = DailyBackfill(start_date="2024-01-01", end_date="2024-01-05")
        assert p.tz == "UTC"
        keys = p.keys()
        assert keys == [
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
        ]

    def test_override_start_end(self):
        p = DailyBackfill(start_date="2024-01-01", end_date="2024-12-31")
        keys = p.keys(start="2024-03-01", end="2024-03-03")
        assert keys == ["2024-03-01", "2024-03-02", "2024-03-03"]

    def test_single_day(self):
        p = DailyBackfill(start_date="2024-06-15", end_date="2024-06-15")
        assert p.keys() == ["2024-06-15"]

    def test_empty_range(self):
        p = DailyBackfill(start_date="2024-06-15", end_date="2024-06-14")
        assert p.keys() == []

    def test_default_end_is_today(self):
        today = whenever.ZonedDateTime.now("UTC").date()
        key = today.py_date().strftime("%Y-%m-%d")
        p = DailyBackfill(start_date=key)
        keys = p.keys()
        assert keys == [key]

    def test_is_frozen(self):
        p = DailyBackfill(start_date="2024-01-01")
        with pytest.raises(AttributeError):
            p.start_date = "2024-02-01"  # type: ignore[misc]

    def test_is_backfill_def(self):
        p = DailyBackfill(start_date="2024-01-01")
        assert isinstance(p, BackfillDef)


class TestWeeklyBackfill:
    def test_basic_range(self):
        p = WeeklyBackfill(start_date="2024-W01", end_date="2024-W04")
        assert p.tz == "UTC"
        keys = p.keys()
        assert len(keys) == 4
        assert keys[0] == "2024-W01"
        assert keys[-1] == "2024-W04"

    def test_override_range(self):
        p = WeeklyBackfill(start_date="2024-W01", end_date="2024-W52")
        keys = p.keys(start="2024-W10", end="2024-W12")
        assert len(keys) == 3

    def test_single_week(self):
        p = WeeklyBackfill(start_date="2024-W05", end_date="2024-W05")
        assert len(p.keys()) == 1


class TestMonthlyBackfill:
    def test_basic_range(self):
        p = MonthlyBackfill(start_date="2024-01-01", end_date="2024-06-01")
        assert p.tz == "UTC"
        keys = p.keys()
        assert keys == [
            "2024-01-01",
            "2024-02-01",
            "2024-03-01",
            "2024-04-01",
            "2024-05-01",
            "2024-06-01",
        ]

    def test_cross_year_boundary(self):
        p = MonthlyBackfill(start_date="2023-11-01", end_date="2024-02-01")
        keys = p.keys()
        assert keys == ["2023-11-01", "2023-12-01", "2024-01-01", "2024-02-01"]

    def test_override_range(self):
        p = MonthlyBackfill(start_date="2024-01-01", end_date="2024-12-01")
        keys = p.keys(start="2024-03-01", end="2024-05-01")
        assert keys == ["2024-03-01", "2024-04-01", "2024-05-01"]

    def test_single_month(self):
        p = MonthlyBackfill(start_date="2024-07-01", end_date="2024-07-01")
        keys = p.keys()
        assert keys == ["2024-07-01"]


class TestHourlyBackfill:
    def test_basic_range(self):
        p = HourlyBackfill(
            start_date="2024-01-01T00",
            end_date="2024-01-01T05",
        )
        keys = p.keys()
        assert len(keys) == 6
        assert keys[0] == "2024-01-01T00"
        assert keys[-1] == "2024-01-01T05"

    def test_cross_day_boundary(self):
        p = HourlyBackfill(
            start_date="2024-01-01T22",
            end_date="2024-01-02T02",
        )
        keys = p.keys()
        assert len(keys) == 5
        assert "2024-01-01T23" in keys
        assert "2024-01-02T00" in keys

    def test_utc_default(self):
        p = HourlyBackfill(start_date="2024-01-01T00")
        assert p.tz == "UTC"

    def test_custom_timezone(self):
        p = HourlyBackfill(
            start_date="2024-01-01T00",
            end_date="2024-01-01T03",
            tz="America/New_York",
        )
        keys = p.keys()
        assert len(keys) == 4

    def test_override_range(self):
        p = HourlyBackfill(
            start_date="2024-01-01T00",
            end_date="2024-01-01T23",
        )
        keys = p.keys(start="2024-01-01T10", end="2024-01-01T12")
        assert len(keys) == 3


class TestStaticBackfill:
    def test_basic(self):
        p = StaticBackfill(keys=["us", "eu", "jp"])
        assert p.keys() == ["us", "eu", "jp"]

    def test_start_end_ignored(self):
        p = StaticBackfill(keys=["a", "b"])
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = p.keys(start="x", end="y")
        assert result == ["a", "b"]
        assert len(w) == 1
        assert "ignores" in str(w[0].message)

    def test_defensive_copy(self):
        original = ["a", "b"]
        p = StaticBackfill(keys=original)
        original.append("c")
        assert p._keys == ["a", "b"]

    def test_is_frozen(self):
        p = StaticBackfill(keys=["a"])
        with pytest.raises(AttributeError):
            p._keys = ["b"]  # type: ignore[misc]


class TestGetRunLogicalDate:
    def test_reads_from_params(self):
        _populate_params({"backfill_key": "2024-01-15T00:00:00+00:00"})
        result = get_run_logical_date(validate=False)

        assert result == datetime(2024, 1, 15, tzinfo=UTC)

    def test_monthly_format_parsed(self):
        """MonthlyBackfill keys ('YYYY-MM-01') should be parseable."""
        _populate_params({"backfill_key": "2024-01-01"})
        result = get_run_logical_date(validate=False)

        assert result == datetime(2024, 1, 1, tzinfo=UTC)

    def test_weekly_format_parsed(self):
        """WeeklyBackfill keys ('YYYY-WNN') should be parseable."""
        _populate_params({"backfill_key": "2024-W03"})
        result = get_run_logical_date(validate=False)

        # 2024-W03 Monday = 2024-01-15
        assert result == datetime(2024, 1, 15, tzinfo=UTC)

    def test_daily_format_parsed(self):
        """DailyBackfill keys ('YYYY-MM-DD') should work."""
        _populate_params({"backfill_key": "2024-06-15"})
        result = get_run_logical_date(validate=False)
        assert result == datetime(2024, 6, 15, tzinfo=UTC)

    def test_unparseable_raises(self):
        _populate_params({"backfill_key": "not-a-date"})
        with pytest.raises(ValueError, match="Cannot parse"):
            get_run_logical_date(validate=False)

    def test_empty_raises(self):
        _populate_params({"backfill_key": ""})
        with pytest.raises(RuntimeError, match="backfill_key is not set"):
            get_run_logical_date()

    def test_missing_raises(self):
        _populate_params({})
        with pytest.raises(RuntimeError, match="backfill_key is not set"):
            get_run_logical_date()

    def teardown_method(self):
        _populate_params({})


class TestTimezoneAwareDefaults:
    """Tests for tz parameter edge cases."""

    def test_hourly_fold_deterministic(self):
        """HourlyBackfill with a DST-ambiguous hour should not crash."""
        p = HourlyBackfill(
            start_date="2024-11-03T00",
            end_date="2024-11-03T03",
            tz="America/New_York",
        )
        keys = p.keys()
        assert len(keys) >= 4  # fall-back produces extra hour


class TestParseLogicalDateStr:
    """Tests for the _parse_logical_date_str helper."""

    def test_iso_full_tz(self):
        dt = _parse_logical_date_str("2024-01-15T00:00:00+00:00")
        assert dt == datetime(2024, 1, 15, tzinfo=UTC)

    def test_iso_date_only(self):
        dt = _parse_logical_date_str("2024-06-15")
        assert dt == datetime(2024, 6, 15, tzinfo=UTC)

    def test_invalid_raises_value_error(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            _parse_logical_date_str("not-a-date")


class TestGetRunLogicalDateValidation:
    """Tests for boundary validation in get_run_logical_date."""

    def setup_method(self):
        reset_registries()

    def teardown_method(self):
        _populate_params({})

    def _register_job(self, backfill_def):
        """Register a minimal job with the given backfill def."""
        _JOB_REGISTRY["test_job"] = JobMeta(
            fn=lambda: None,
            name="test_job",
            backfill=backfill_def,
        )

    def test_daily_in_range_passes(self):
        self._register_job(
            DailyBackfill(start_date="2024-01-01", end_date="2024-12-31")
        )
        _populate_params({"backfill_key": "2024-06-15", "__job_name__": "test_job"})
        dt = get_run_logical_date()
        assert dt.day == 15

    def test_daily_before_start_raises(self):
        self._register_job(
            DailyBackfill(start_date="2024-03-01", end_date="2024-12-31")
        )
        _populate_params({"backfill_key": "2024-01-15", "__job_name__": "test_job"})
        with pytest.raises(ValueError, match="before the backfill start_date"):
            get_run_logical_date()

    def test_daily_after_end_raises(self):
        self._register_job(
            DailyBackfill(start_date="2024-01-01", end_date="2024-06-30")
        )
        _populate_params({"backfill_key": "2024-07-01", "__job_name__": "test_job"})
        with pytest.raises(ValueError, match="after the backfill end_date"):
            get_run_logical_date()

    def test_daily_on_boundary_passes(self):
        self._register_job(
            DailyBackfill(start_date="2024-01-01", end_date="2024-01-01")
        )
        _populate_params({"backfill_key": "2024-01-01", "__job_name__": "test_job"})
        dt = get_run_logical_date()
        assert dt.day == 1

    def test_monthly_in_range_passes(self):
        self._register_job(
            MonthlyBackfill(start_date="2024-01-01", end_date="2024-12-01")
        )
        _populate_params({"backfill_key": "2024-06-01", "__job_name__": "test_job"})
        dt = get_run_logical_date()
        assert dt.month == 6

    def test_hourly_in_range_passes(self):
        self._register_job(
            HourlyBackfill(start_date="2024-01-01T00", end_date="2024-01-01T23")
        )
        _populate_params({"backfill_key": "2024-01-01T12", "__job_name__": "test_job"})
        dt = get_run_logical_date()
        assert dt.hour == 12

    def test_hourly_before_start_raises(self):
        self._register_job(
            HourlyBackfill(start_date="2024-01-01T10", end_date="2024-01-01T23")
        )
        _populate_params({"backfill_key": "2024-01-01T05", "__job_name__": "test_job"})
        with pytest.raises(ValueError, match="before the backfill start_date"):
            get_run_logical_date()

    def test_static_valid_date_key_passes(self):
        self._register_job(StaticBackfill(keys=["2024-01-01", "2024-06-15"]))
        _populate_params({"backfill_key": "2024-06-15", "__job_name__": "test_job"})
        dt = get_run_logical_date()
        assert dt.day == 15

    def test_static_invalid_key_raises(self):
        self._register_job(StaticBackfill(keys=["2024-01-01", "2024-06-15"]))
        _populate_params({"backfill_key": "2024-03-01", "__job_name__": "test_job"})
        with pytest.raises(ValueError, match="not in the StaticBackfill"):
            get_run_logical_date()

    def test_validate_false_skips_check(self):
        self._register_job(
            DailyBackfill(start_date="2024-06-01", end_date="2024-06-30")
        )
        _populate_params({"backfill_key": "2024-01-01", "__job_name__": "test_job"})
        # Out of range but validate=False
        dt = get_run_logical_date(validate=False)
        assert dt.month == 1

    def test_no_job_context_skips_validation(self):
        """When __job_name__ is absent from params, validation is skipped."""
        _populate_params({"backfill_key": "2024-01-01"})
        dt = get_run_logical_date()
        assert dt.day == 1

    def test_no_backfill_def_skips_validation(self):
        """Jobs without backfill= skip validation."""
        _JOB_REGISTRY["test_job"] = JobMeta(fn=lambda: None, name="test_job")
        _populate_params({"backfill_key": "2024-01-01", "__job_name__": "test_job"})
        dt = get_run_logical_date()
        assert dt.day == 1

    def test_no_end_date_only_validates_start(self):
        """When end_date is None, only start boundary is checked."""
        self._register_job(DailyBackfill(start_date="2024-06-01"))
        _populate_params({"backfill_key": "2024-01-01", "__job_name__": "test_job"})
        with pytest.raises(ValueError, match="before the backfill start_date"):
            get_run_logical_date()
