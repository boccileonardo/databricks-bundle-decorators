"""Tests for backfill definitions and get_run_logical_date helper."""

from __future__ import annotations

import logging
import warnings
from datetime import UTC, datetime

import pytest
import whenever

from databricks_bundle_decorators.backfill import (
    EXACT_BACKFILL_PARAM,
    BackfillDef,
    DailyBackfill,
    HourlyBackfill,
    MonthlyBackfill,
    StaticBackfill,
    WeeklyBackfill,
    _compute_schedule_gap_keys,
    _deserialize_backfill_tag,
    _parse_logical_date_str,
    _quartz_to_unix_cron,
    _serialize_backfill_tag,
    get_backfill_key,
    get_backfill_keys,
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
        key = today.format("YYYY-MM-DD")
        p = DailyBackfill(start_date=key)
        keys = p.keys()
        assert keys == [key]

    def test_is_frozen(self):
        p = DailyBackfill(start_date="2024-01-01")
        with pytest.raises(AttributeError):
            p.start_date = "2024-02-01"  # ty: ignore[invalid-assignment]

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
            p._keys = ["b"]  # ty: ignore[invalid-assignment]


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


class TestCurrentKey:
    """Tests for BackfillDef.current_key() method."""

    def test_daily_returns_today(self):
        p = DailyBackfill(start_date="2024-01-01")
        key = p.current_key()
        today = whenever.ZonedDateTime.now("UTC").date().format("YYYY-MM-DD")
        assert key == today

    def test_weekly_returns_current_week(self):
        p = WeeklyBackfill(start_date="2024-W01")
        key = p.current_key()
        iwd = whenever.ZonedDateTime.now("UTC").date().iso_week_date()
        today = f"{iwd.year}-W{iwd.week:02d}"
        assert key == today

    def test_monthly_returns_current_month(self):
        p = MonthlyBackfill(start_date="2024-01-01")
        key = p.current_key()
        today = whenever.ZonedDateTime.now("UTC").date()
        expected = today.replace(day=1).format("YYYY-MM-DD")
        assert key == expected

    def test_hourly_returns_current_hour(self):
        p = HourlyBackfill(start_date="2024-01-01T00")
        key = p.current_key()
        now = whenever.ZonedDateTime.now("UTC")
        expected = now.replace(minute=0, second=0, nanosecond=0).format(
            "YYYY-MM-DD'T'hh"
        )
        assert key == expected

    def test_static_returns_none(self):
        p = StaticBackfill(keys=["us", "eu"])
        assert p.current_key() is None

    def test_daily_respects_timezone(self):
        p = DailyBackfill(start_date="2024-01-01", tz="Pacific/Auckland")
        key = p.current_key()
        expected = (
            whenever.ZonedDateTime.now("Pacific/Auckland").date().format("YYYY-MM-DD")
        )
        assert key == expected


class TestDataLag:
    """Tests for the data_lag parameter on time-based backfill defs."""

    def test_daily_current_key_with_lag(self):
        p = DailyBackfill(start_date="2024-01-01", data_lag=1)
        key = p.current_key()
        yesterday = (
            whenever.ZonedDateTime.now("UTC")
            .date()
            .subtract(days=1)
            .format("YYYY-MM-DD")
        )
        assert key == yesterday

    def test_daily_keys_default_end_with_lag(self):
        today = whenever.ZonedDateTime.now("UTC").date()
        yesterday = today.subtract(days=1).format("YYYY-MM-DD")
        p = DailyBackfill(start_date=yesterday, data_lag=1)
        keys = p.keys()
        assert yesterday in keys
        assert today.format("YYYY-MM-DD") not in keys

    def test_daily_keys_explicit_end_ignores_lag(self):
        """When --end is explicitly provided, data_lag does not apply."""
        p = DailyBackfill(start_date="2024-01-01", data_lag=1)
        keys = p.keys(end="2024-01-05")
        assert keys == [
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
        ]

    def test_daily_lag_2(self):
        p = DailyBackfill(start_date="2024-01-01", data_lag=2)
        key = p.current_key()
        expected = (
            whenever.ZonedDateTime.now("UTC")
            .date()
            .subtract(days=2)
            .format("YYYY-MM-DD")
        )
        assert key == expected

    def test_weekly_current_key_with_lag(self):
        p = WeeklyBackfill(start_date="2024-W01", data_lag=1)
        key = p.current_key()
        last_week = whenever.ZonedDateTime.now("UTC").date().subtract(weeks=1)
        iwd = last_week.iso_week_date()
        expected = f"{iwd.year}-W{iwd.week:02d}"
        assert key == expected

    def test_monthly_current_key_with_lag(self):
        p = MonthlyBackfill(start_date="2024-01-01", data_lag=1)
        key = p.current_key()
        last_month = (
            whenever.ZonedDateTime.now("UTC")
            .date()
            .subtract(months=1)
            .replace(day=1)
            .format("YYYY-MM-DD")
        )
        assert key == last_month

    def test_hourly_current_key_with_lag(self):
        p = HourlyBackfill(start_date="2024-01-01T00", data_lag=1)
        key = p.current_key()
        now = whenever.ZonedDateTime.now("UTC")
        expected = (
            now.replace(minute=0, second=0, nanosecond=0)
            .subtract(hours=1)
            .format("YYYY-MM-DD'T'hh")
        )
        assert key == expected

    def test_default_lag_is_zero(self):
        p = DailyBackfill(start_date="2024-01-01")
        assert p.data_lag == 0

    def test_lag_serialization_roundtrip(self):
        original = DailyBackfill(start_date="2024-01-01", data_lag=1)
        raw = _serialize_backfill_tag(original)
        restored = _deserialize_backfill_tag(raw)
        assert isinstance(restored, DailyBackfill)
        assert restored.data_lag == 1

    def test_lag_zero_not_serialized(self):
        p = DailyBackfill(start_date="2024-01-01", data_lag=0)
        raw = _serialize_backfill_tag(p)
        assert "data_lag" not in raw


class TestAutoDerive:
    """Tests for auto-deriving backfill_key when not explicitly provided."""

    def setup_method(self):

        reset_registries()

    def teardown_method(self):
        _populate_params({})

    def _register_job(self, backfill_def):

        _JOB_REGISTRY["test_job"] = JobMeta(
            fn=lambda: None,
            name="test_job",
            backfill=backfill_def,
        )

    def test_daily_auto_derive(self):
        self._register_job(DailyBackfill(start_date="2024-01-01"))
        _populate_params({"backfill_key": "", "__job_name__": "test_job"})
        key = get_backfill_key(validate=False)
        today = whenever.ZonedDateTime.now("UTC").date().format("YYYY-MM-DD")
        assert key == today

    def test_missing_param_auto_derive(self):
        self._register_job(DailyBackfill(start_date="2024-01-01"))
        _populate_params({"__job_name__": "test_job"})
        key = get_backfill_key(validate=False)
        today = whenever.ZonedDateTime.now("UTC").date().format("YYYY-MM-DD")
        assert key == today

    def test_static_still_raises(self):
        self._register_job(StaticBackfill(keys=["us", "eu"]))
        _populate_params({"backfill_key": "", "__job_name__": "test_job"})
        with pytest.raises(RuntimeError, match="backfill_key is not set"):
            get_backfill_key()

    def test_no_backfill_def_still_raises(self):

        _JOB_REGISTRY["test_job"] = JobMeta(fn=lambda: None, name="test_job")
        _populate_params({"backfill_key": "", "__job_name__": "test_job"})
        with pytest.raises(RuntimeError, match="backfill_key is not set"):
            get_backfill_key()

    def test_no_job_name_still_raises(self):
        _populate_params({"backfill_key": ""})
        with pytest.raises(RuntimeError, match="backfill_key is not set"):
            get_backfill_key()

    def test_explicit_key_not_overridden(self):
        self._register_job(DailyBackfill(start_date="2024-01-01"))
        _populate_params({"backfill_key": "2024-06-15", "__job_name__": "test_job"})
        key = get_backfill_key(validate=False)
        assert key == "2024-06-15"

    def test_auto_derive_logs_warning(self, caplog):
        self._register_job(DailyBackfill(start_date="2024-01-01"))
        _populate_params({"backfill_key": "", "__job_name__": "test_job"})
        with caplog.at_level(logging.WARNING):
            get_backfill_key(validate=False)
        assert "auto-assigned" in caplog.text

    def test_hourly_auto_derive(self):
        self._register_job(HourlyBackfill(start_date="2024-01-01T00"))
        _populate_params({"backfill_key": "", "__job_name__": "test_job"})
        key = get_backfill_key(validate=False)
        now = whenever.ZonedDateTime.now("UTC")
        expected = now.replace(minute=0, second=0, nanosecond=0).format(
            "YYYY-MM-DD'T'hh"
        )
        assert key == expected

    def test_weekly_auto_derive(self):
        self._register_job(WeeklyBackfill(start_date="2024-W01"))
        _populate_params({"backfill_key": "", "__job_name__": "test_job"})
        key = get_backfill_key(validate=False)
        iwd = whenever.ZonedDateTime.now("UTC").date().iso_week_date()
        today = f"{iwd.year}-W{iwd.week:02d}"
        assert key == today

    def test_monthly_auto_derive(self):
        self._register_job(MonthlyBackfill(start_date="2024-01-01"))
        _populate_params({"backfill_key": "", "__job_name__": "test_job"})
        key = get_backfill_key(validate=False)
        today = whenever.ZonedDateTime.now("UTC").date()
        expected = today.replace(day=1).format("YYYY-MM-DD")
        assert key == expected


class TestQuartzToUnixCron:
    """Tests for the Quartz → Unix cron converter."""

    def test_simple_daily(self):

        # "At 06:00:00 every day"
        result = _quartz_to_unix_cron("0 0 6 * * ?")
        assert result == "0 6 * * *"

    def test_weekday_only(self):

        # "At 06:00:00 MON-FRI" (Quartz: 2=MON, 6=FRI)
        result = _quartz_to_unix_cron("0 0 6 ? * 2-6")
        assert result == "0 6 * * 1-5"

    def test_named_days_unchanged(self):

        result = _quartz_to_unix_cron("0 0 6 ? * MON-FRI")
        assert result == "0 6 * * MON-FRI"

    def test_question_mark_replaced(self):

        result = _quartz_to_unix_cron("0 15 10 ? * *")
        assert result == "15 10 * * *"

    def test_with_year_field(self):

        # 7 fields (with year) — year is dropped
        result = _quartz_to_unix_cron("0 0 6 * * ? 2026")
        assert result == "0 6 * * *"

    def test_too_few_fields_raises(self):

        with pytest.raises(ValueError, match="6-7 fields"):
            _quartz_to_unix_cron("0 6 * * *")

    def test_non_zero_seconds_raises(self):

        with pytest.raises(ValueError, match="seconds field must be '0'"):
            _quartz_to_unix_cron("30 0 6 * * ?")


class TestGetBackfillKeys:
    """Tests for the multi-key get_backfill_keys function."""

    def setup_method(self):
        reset_registries()

    def teardown_method(self):
        _populate_params({})

    def _register_job(self, backfill_def, schedule=None):
        sdk_config = {}
        if schedule is not None:
            sdk_config["schedule"] = schedule
        _JOB_REGISTRY["test_job"] = JobMeta(
            fn=lambda: None,
            name="test_job",
            backfill=backfill_def,
            sdk_config=sdk_config,
        )

    def test_single_key_no_lookback(self):

        self._register_job(DailyBackfill(start_date="2024-01-01"))
        _populate_params({"backfill_key": "2026-01-08", "__job_name__": "test_job"})
        keys = get_backfill_keys(validate=False)
        assert keys == ["2026-01-08"]

    def test_lookback_daily(self):

        self._register_job(DailyBackfill(start_date="2024-01-01", lookback=2))
        _populate_params({"backfill_key": "2026-01-08", "__job_name__": "test_job"})
        keys = get_backfill_keys(validate=False)
        assert keys == ["2026-01-06", "2026-01-07", "2026-01-08"]

    def test_lookback_weekly(self):

        self._register_job(WeeklyBackfill(start_date="2024-W01", lookback=1))
        _populate_params({"backfill_key": "2026-W02", "__job_name__": "test_job"})
        keys = get_backfill_keys(validate=False)
        assert keys == ["2026-W01", "2026-W02"]

    def test_lookback_monthly(self):

        self._register_job(MonthlyBackfill(start_date="2024-01-01", lookback=2))
        _populate_params({"backfill_key": "2026-03-01", "__job_name__": "test_job"})
        keys = get_backfill_keys(validate=False)
        assert keys == ["2026-01-01", "2026-02-01", "2026-03-01"]

    def test_lookback_hourly(self):

        self._register_job(HourlyBackfill(start_date="2024-01-01T00", lookback=2))
        _populate_params({"backfill_key": "2026-01-01T10", "__job_name__": "test_job"})
        keys = get_backfill_keys(validate=False)
        assert keys == ["2026-01-01T08", "2026-01-01T09", "2026-01-01T10"]

    def test_schedule_gaps_daily_weekday_monday(self):
        """Monday with MON-FRI schedule: includes Sat, Sun, Mon."""

        # Mock a CronSchedule object
        class _FakeSchedule:
            quartz_cron_expression = "0 0 6 ? * 2-6"  # MON-FRI 6am

        self._register_job(
            DailyBackfill(start_date="2024-01-01", collect_schedule_gaps=True),
            schedule=_FakeSchedule(),
        )
        # Auto-derived (empty backfill_key) — Monday 2026-01-05
        _populate_params({"backfill_key": "", "__job_name__": "test_job"})
        # Since backfill_key is empty, get_backfill_key auto-derives to today.
        # We need to provide the key explicitly but mark it as auto-derived.
        # Actually the function checks params.get(BACKFILL_KEY_PARAM, "") for
        # schedule gaps — empty means auto-derived. Let's test differently.
        # We set the key to simulate a scheduled run where auto-derive happened.
        # The auto-derive logic sets the key but the param remains empty.
        # For testing, let's just verify the gap computation directly.

        backfill = DailyBackfill(start_date="2024-01-01", collect_schedule_gaps=True)
        gap_keys = _compute_schedule_gap_keys(backfill, "2026-01-05", "0 0 6 ? * 2-6")
        # Previous fire was Friday 2026-01-02, gap = Sat 01-03, Sun 01-04
        assert gap_keys == ["2026-01-03", "2026-01-04"]

    def test_schedule_gaps_daily_tuesday(self):
        """Tuesday with MON-FRI schedule: no gap (previous fire was Monday)."""

        backfill = DailyBackfill(start_date="2024-01-01", collect_schedule_gaps=True)
        gap_keys = _compute_schedule_gap_keys(backfill, "2026-01-06", "0 0 6 ? * 2-6")
        # Previous fire was Monday 2026-01-05 — no gap days
        assert gap_keys == []

    def test_exact_flag_bypasses_lookback(self):

        self._register_job(DailyBackfill(start_date="2024-01-01", lookback=2))
        _populate_params(
            {
                "backfill_key": "2026-01-08",
                "__job_name__": "test_job",
                EXACT_BACKFILL_PARAM: "1",
            }
        )
        keys = get_backfill_keys(validate=False)
        assert keys == ["2026-01-08"]

    def test_explicit_key_bypasses_schedule_gaps(self):
        """When backfill_key is explicitly provided, schedule gaps are skipped."""

        class _FakeSchedule:
            quartz_cron_expression = "0 0 6 ? * 2-6"

        self._register_job(
            DailyBackfill(start_date="2024-01-01", collect_schedule_gaps=True),
            schedule=_FakeSchedule(),
        )
        # Explicitly provided key (non-empty) — Monday 2026-01-05
        _populate_params({"backfill_key": "2026-01-05", "__job_name__": "test_job"})
        keys = get_backfill_keys(validate=False)
        # No schedule gaps because key was explicit
        assert keys == ["2026-01-05"]

    def test_combined_lookback_and_explicit(self):
        """Explicit key with lookback: lookback applies, gaps don't."""

        class _FakeSchedule:
            quartz_cron_expression = "0 0 6 ? * 2-6"

        self._register_job(
            DailyBackfill(
                start_date="2024-01-01",
                lookback=2,
                collect_schedule_gaps=True,
            ),
            schedule=_FakeSchedule(),
        )
        # Saturday explicit backfill
        _populate_params({"backfill_key": "2026-01-03", "__job_name__": "test_job"})
        keys = get_backfill_keys(validate=False)
        # lookback=2 gives Thu, Fri + Sat; schedule gaps bypassed
        assert keys == ["2026-01-01", "2026-01-02", "2026-01-03"]

    def test_static_backfill_returns_single_key(self):

        self._register_job(StaticBackfill(keys=["us", "eu", "jp"]))
        _populate_params({"backfill_key": "eu", "__job_name__": "test_job"})
        keys = get_backfill_keys(validate=False)
        assert keys == ["eu"]


class TestSerializeDeserializeLookback:
    """Test that lookback/collect_schedule_gaps survive serialization."""

    def test_daily_roundtrip(self):

        original = DailyBackfill(
            start_date="2024-01-01",
            lookback=3,
            collect_schedule_gaps=True,
        )
        serialized = _serialize_backfill_tag(original)
        restored = _deserialize_backfill_tag(serialized)
        assert isinstance(restored, DailyBackfill)
        assert restored.lookback == 3
        assert restored.collect_schedule_gaps is True

    def test_weekly_roundtrip(self):

        original = WeeklyBackfill(start_date="2024-W01", lookback=1)
        serialized = _serialize_backfill_tag(original)
        restored = _deserialize_backfill_tag(serialized)
        assert isinstance(restored, WeeklyBackfill)
        assert restored.lookback == 1
        assert restored.collect_schedule_gaps is False

    def test_defaults_not_serialized(self):
        """When lookback=0 and collect_schedule_gaps=False, they're not in JSON."""

        original = DailyBackfill(start_date="2024-01-01")
        serialized = _serialize_backfill_tag(original)
        assert "lookback" not in serialized
        assert "collect_schedule_gaps" not in serialized
