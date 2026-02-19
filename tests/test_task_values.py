"""Tests for set_task_value / get_task_value helpers."""

import pytest

from databricks_bundle_decorators.task_values import (
    _local_task_values,
    get_task_value,
    set_task_value,
)
import databricks_bundle_decorators.task_values as _tv


class TestSetTaskValue:
    def setup_method(self):
        _local_task_values.clear()

    def test_set_str(self):
        set_task_value("key", "hello")
        assert _local_task_values["__current__"]["key"] == "hello"

    def test_set_int(self):
        set_task_value("count", 42)
        assert _local_task_values["__current__"]["count"] == 42

    def test_set_float(self):
        set_task_value("ratio", 3.14)
        assert _local_task_values["__current__"]["ratio"] == 3.14

    def test_set_bool(self):
        set_task_value("flag", True)
        assert _local_task_values["__current__"]["flag"] is True

    def test_rejects_list(self):
        with pytest.raises(TypeError, match="primitive types"):
            set_task_value("bad", [1, 2, 3])  # type: ignore[arg-type]

    def test_rejects_dict(self):
        with pytest.raises(TypeError, match="primitive types"):
            set_task_value("bad", {"a": 1})  # type: ignore[arg-type]

    def test_rejects_none(self):
        with pytest.raises(TypeError, match="primitive types"):
            set_task_value("bad", None)  # type: ignore[arg-type]


class TestGetTaskValue:
    def setup_method(self):
        _local_task_values.clear()

    def test_get_existing_value(self):
        _local_task_values["producer"] = {"row_count": 100}
        assert get_task_value("producer", "row_count") == 100

    def test_get_missing_task(self):
        assert get_task_value("nonexistent", "key") is None

    def test_get_missing_key(self):
        _local_task_values["producer"] = {"other_key": 1}
        assert get_task_value("producer", "missing") is None


class TestLocalRoundTrip:
    """Verify set/get work together in the local fallback path."""

    def setup_method(self):
        _local_task_values.clear()
        _tv._current_task_key = None

    def teardown_method(self):
        _tv._current_task_key = None

    def test_local_store_defaults_to_current(self):
        """Without a runtime task key, stores under '__current__'."""
        set_task_value("metric", 99)
        assert _local_task_values["__current__"]["metric"] == 99

    def test_local_store_uses_task_key_when_set(self):
        """When _current_task_key is set (by runtime), stores under it."""
        _tv._current_task_key = "producer"
        set_task_value("row_count", 42)
        assert _local_task_values["producer"]["row_count"] == 42

    def test_cross_task_round_trip(self):
        """set_task_value in producer → get_task_value in consumer works locally."""
        _tv._current_task_key = "producer"
        set_task_value("row_count", 42)
        _tv._current_task_key = None

        assert get_task_value("producer", "row_count") == 42
