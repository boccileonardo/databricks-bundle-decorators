"""Tests for :mod:`databricks_bundle_decorators.context`."""

from __future__ import annotations

import types
from unittest.mock import MagicMock, patch

import pytest

from databricks_bundle_decorators.context import get_dbutils


class TestGetDbutils:
    """Tests for :func:`get_dbutils`."""

    def test_pyspark_dbutils_with_explicit_spark(self) -> None:
        """Strategy 1: ``pyspark.dbutils.DBUtils`` with an explicit session."""
        mock_spark = MagicMock()
        sentinel = object()

        fake_dbutils_cls = MagicMock(return_value=sentinel)
        fake_module = types.ModuleType("pyspark.dbutils")
        fake_module.DBUtils = fake_dbutils_cls  # type: ignore[attr-defined]

        with patch.dict(
            "sys.modules", {"pyspark.dbutils": fake_module, "pyspark": MagicMock()}
        ):
            result = get_dbutils(spark=mock_spark)

        assert result is sentinel
        fake_dbutils_cls.assert_called_once_with(mock_spark)

    def test_pyspark_dbutils_with_active_session(self) -> None:
        """Strategy 1 fallback: auto-detect the active SparkSession."""
        sentinel = object()
        mock_spark = MagicMock()

        fake_dbutils_cls = MagicMock(return_value=sentinel)
        fake_dbutils_mod = types.ModuleType("pyspark.dbutils")
        fake_dbutils_mod.DBUtils = fake_dbutils_cls  # type: ignore[attr-defined]

        fake_ss = MagicMock()
        fake_ss.getActiveSession.return_value = mock_spark
        fake_sql_mod = types.ModuleType("pyspark.sql")
        fake_sql_mod.SparkSession = fake_ss  # type: ignore[attr-defined]

        with patch.dict(
            "sys.modules",
            {
                "pyspark": MagicMock(),
                "pyspark.dbutils": fake_dbutils_mod,
                "pyspark.sql": fake_sql_mod,
            },
        ):
            result = get_dbutils()

        assert result is sentinel
        fake_dbutils_cls.assert_called_once_with(mock_spark)

    def test_ipython_fallback(self) -> None:
        """Strategy 2: retrieve dbutils from the IPython user namespace."""
        sentinel = object()

        fake_ip = MagicMock()
        fake_ip.user_ns = {"dbutils": sentinel}

        fake_ipython = MagicMock()
        fake_ipython.get_ipython.return_value = fake_ip

        # Make pyspark unavailable so strategy 1 is skipped
        with (
            patch.dict("sys.modules", {"pyspark.dbutils": None, "pyspark": None}),
            patch.dict("sys.modules", {"IPython": fake_ipython}),
        ):
            result = get_dbutils()

        assert result is sentinel

    def test_raises_when_unavailable(self) -> None:
        """RuntimeError when no strategy succeeds."""
        with (
            patch.dict(
                "sys.modules",
                {"pyspark.dbutils": None, "pyspark": None, "IPython": None},
            ),
            pytest.raises(RuntimeError, match="dbutils is not available"),
        ):
            get_dbutils()

    def test_pyspark_no_active_session_falls_through(self) -> None:
        """When pyspark is available but no active session, fall through."""
        fake_dbutils_cls = MagicMock()
        fake_dbutils_mod = types.ModuleType("pyspark.dbutils")
        fake_dbutils_mod.DBUtils = fake_dbutils_cls  # type: ignore[attr-defined]

        fake_ss = MagicMock()
        fake_ss.getActiveSession.return_value = None
        fake_sql_mod = types.ModuleType("pyspark.sql")
        fake_sql_mod.SparkSession = fake_ss  # type: ignore[attr-defined]

        with (
            patch.dict(
                "sys.modules",
                {
                    "pyspark": MagicMock(),
                    "pyspark.dbutils": fake_dbutils_mod,
                    "pyspark.sql": fake_sql_mod,
                    "IPython": None,
                },
            ),
            pytest.raises(RuntimeError, match="dbutils is not available"),
        ):
            get_dbutils()

        fake_dbutils_cls.assert_not_called()
