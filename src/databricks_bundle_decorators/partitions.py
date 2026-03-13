"""Partition definitions for time-based and static partitioning.

Partition definitions declare the universe of valid ``logical_date``
values for a job.  They are used by the ``dbxdec backfill`` CLI command
to enumerate dates for bulk run submission.

Every job automatically receives a ``logical_date`` parameter.  At
runtime, task code reads it via the convenience helper::

    from databricks_bundle_decorators.partitions import current_logical_date

    @task
    def extract() -> pl.DataFrame:
        dt = current_logical_date()  # returns datetime, raises if unset
        ...
"""

from __future__ import annotations

import logging
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

_logger = logging.getLogger(__name__)

#: The fixed job-parameter name that carries the logical date.
LOGICAL_DATE_PARAM: str = "logical_date"


class PartitionDef(ABC):
    """Base class for partition definitions.

    Subclasses declare the universe of valid partition keys for
    backfill enumeration.  The ``dbxdec backfill`` CLI uses these to
    generate ``logical_date`` values.
    """

    @abstractmethod
    def partition_keys(
        self, start: str | None = None, end: str | None = None
    ) -> list[str]:
        """Enumerate concrete partition key strings.

        Parameters
        ----------
        start:
            Override the start bound (inclusive).  Format must match the
            partition's ``fmt``.
        end:
            Override the end bound (inclusive).  Format must match the
            partition's ``fmt``.
        """
        ...


@dataclass(frozen=True)
class DailyPartition(PartitionDef):
    """One partition per calendar day.

    Keys are formatted as ``YYYY-MM-DD`` by default.

    Parameters
    ----------
    start_date:
        First partition key (inclusive), e.g. ``"2024-01-01"``.
    end_date:
        Last partition key (inclusive).  Defaults to yesterday in *tz*.
    fmt:
        ``strftime`` / ``strptime`` format string for keys.
    tz:
        IANA timezone name (e.g. ``"UTC"``, ``"Europe/Berlin"``).
        Used to determine "yesterday" when *end_date* is omitted.
    """

    start_date: str
    end_date: str | None = None
    fmt: str = "%Y-%m-%d"
    tz: str = "UTC"

    def partition_keys(
        self, start: str | None = None, end: str | None = None
    ) -> list[str]:
        s = datetime.strptime(start or self.start_date, self.fmt).date()
        if end is not None:
            e = datetime.strptime(end, self.fmt).date()
        elif self.end_date is not None:
            e = datetime.strptime(self.end_date, self.fmt).date()
        else:
            today = datetime.now(tz=ZoneInfo(self.tz)).date()
            e = today - timedelta(days=1)

        keys: list[str] = []
        while s <= e:
            keys.append(s.strftime(self.fmt))
            s += timedelta(days=1)
        return keys


@dataclass(frozen=True)
class WeeklyPartition(PartitionDef):
    """One partition per ISO week.

    Keys are formatted as ``YYYY-WNN`` by default (e.g. ``"2024-W03"``).

    The default ``end_date`` is the Monday of the most recent **completed**
    ISO week (i.e. the week whose Sunday has already passed).

    Parameters
    ----------
    start_date:
        First partition key (inclusive), using *fmt*.
    end_date:
        Last partition key (inclusive).  Defaults to the most recent
        completed ISO week.
    fmt:
        ``strftime`` format for keys.  Defaults to ``"%G-W%V"``
        (ISO year + ISO week).
    tz:
        IANA timezone name.  Used to determine "today" when
        *end_date* is omitted.
    """

    start_date: str
    end_date: str | None = None
    fmt: str = "%G-W%V"
    tz: str = "UTC"

    def _parse_iso_week(self, key: str) -> date:
        """Parse an ISO-week key into a Monday ``date``."""
        return datetime.strptime(key + "-1", self.fmt + "-%u").date()

    def partition_keys(
        self, start: str | None = None, end: str | None = None
    ) -> list[str]:
        s = self._parse_iso_week(start or self.start_date)
        if end is not None:
            e = self._parse_iso_week(end)
        elif self.end_date is not None:
            e = self._parse_iso_week(self.end_date)
        else:
            today = datetime.now(tz=ZoneInfo(self.tz)).date()
            # Most recent completed week: Monday of current week minus 7 days
            e = today - timedelta(days=today.weekday() + 7)

        keys: list[str] = []
        while s <= e:
            keys.append(s.strftime(self.fmt))
            s += timedelta(weeks=1)
        return keys


@dataclass(frozen=True)
class MonthlyPartition(PartitionDef):
    """One partition per calendar month.

    Keys are formatted as ``YYYY-MM`` by default.

    Parameters
    ----------
    start_date:
        First partition key (inclusive), e.g. ``"2024-01"``.
    end_date:
        Last partition key (inclusive).  Defaults to the previous
        completed month.
    fmt:
        ``strftime`` format for keys.
    tz:
        IANA timezone name.  Used to determine "today" when
        *end_date* is omitted.
    """

    start_date: str
    end_date: str | None = None
    fmt: str = "%Y-%m"
    tz: str = "UTC"

    def _parse_month(self, key: str) -> date:
        """Parse a month key into the first day of that month."""
        return datetime.strptime(key, self.fmt).date().replace(day=1)

    def _next_month(self, d: date) -> date:
        """Advance *d* to the first day of the next month."""
        if d.month == 12:
            return d.replace(year=d.year + 1, month=1)
        return d.replace(month=d.month + 1)

    def partition_keys(
        self, start: str | None = None, end: str | None = None
    ) -> list[str]:
        s = self._parse_month(start or self.start_date)
        if end is not None:
            e = self._parse_month(end)
        elif self.end_date is not None:
            e = self._parse_month(self.end_date)
        else:
            today = datetime.now(tz=ZoneInfo(self.tz)).date()
            # Previous completed month
            e = (today.replace(day=1) - timedelta(days=1)).replace(day=1)

        keys: list[str] = []
        while s <= e:
            keys.append(s.strftime(self.fmt))
            s = self._next_month(s)
        return keys


@dataclass(frozen=True)
class HourlyPartition(PartitionDef):
    """One partition per hour.

    Keys are formatted as ``YYYY-MM-DDTHH`` by default.

    All enumeration is performed in the specified timezone (default UTC)
    so that daylight-saving transitions are handled correctly — hours
    that don't exist are skipped, and ambiguous hours appear once.

    Parameters
    ----------
    start_date:
        First partition key (inclusive), e.g. ``"2024-01-01T00"``.
    end_date:
        Last partition key (inclusive).  Defaults to the previous
        completed hour in *tz*.
    fmt:
        ``strftime`` format for keys.
    tz:
        IANA timezone name (e.g. ``"UTC"``, ``"America/New_York"``).
        Defaults to ``"UTC"`` to sidestep daylight-saving issues.
    """

    start_date: str
    end_date: str | None = None
    fmt: str = "%Y-%m-%dT%H"
    tz: str = "UTC"

    def _parse_hour(self, key: str) -> datetime:
        """Parse an hour key into a timezone-aware ``datetime``.

        Uses ``fold=0`` so that ambiguous wall-clock times (e.g. the
        repeated hour during a fall-back DST transition) resolve
        deterministically to the *first* occurrence.
        """
        naive = datetime.strptime(key, self.fmt)
        return naive.replace(tzinfo=ZoneInfo(self.tz), fold=0)

    def partition_keys(
        self, start: str | None = None, end: str | None = None
    ) -> list[str]:
        tzinfo = ZoneInfo(self.tz)
        s = self._parse_hour(start or self.start_date)
        if end is not None:
            e = self._parse_hour(end)
        elif self.end_date is not None:
            e = self._parse_hour(self.end_date)
        else:
            now = datetime.now(tz=tzinfo)
            # Previous completed hour
            e = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

        # Step via UTC to avoid DST ambiguity, then convert back
        s_utc = s.astimezone(timezone.utc)
        e_utc = e.astimezone(timezone.utc)

        keys: list[str] = []
        seen: set[str] = set()
        cur = s_utc
        while cur <= e_utc:
            local = cur.astimezone(tzinfo)
            key = local.strftime(self.fmt)
            if key not in seen:
                keys.append(key)
                seen.add(key)
            cur += timedelta(hours=1)
        return keys


@dataclass(frozen=True)
class StaticPartition(PartitionDef):
    """A fixed set of partition keys.

    Parameters
    ----------
    keys:
        The complete list of valid partition keys.

    Example
    -------
    ::

        StaticPartition(keys=["us", "eu", "jp"])
    """

    keys: list[str] = field(default_factory=list)

    def __init__(self, keys: list[str]) -> None:
        # Defensive copy so mutations to the caller's list don't leak.
        object.__setattr__(self, "keys", list(keys))

    def partition_keys(
        self, start: str | None = None, end: str | None = None
    ) -> list[str]:
        if start is not None or end is not None:
            warnings.warn(
                "StaticPartition.partition_keys() ignores 'start' and 'end' "
                "arguments. All keys are always returned.",
                stacklevel=2,
            )
        return list(self.keys)


def current_logical_date() -> datetime:
    """Read the current logical date from ``params["logical_date"]``.

    The value is always an ISO-8601 string in *params* and is parsed
    into a timezone-aware ``datetime`` before returning.

    Raises
    ------
    RuntimeError
        If ``logical_date`` is missing or empty.  This indicates the
        job was not invoked with a ``logical_date`` parameter (e.g. it
        is not partitioned and was not started via backfill).

    Returns
    -------
    datetime
        Timezone-aware datetime representing the logical date.
    """
    from databricks_bundle_decorators.context import params

    raw = params.get(LOGICAL_DATE_PARAM, "")
    if not raw:
        raise RuntimeError(
            "logical_date is not set. "
            "This usually means the job was not invoked with a "
            "logical_date parameter. Use @job(partition=...) and "
            "the backfill CLI, or pass logical_date explicitly."
        )
    return datetime.fromisoformat(raw)
