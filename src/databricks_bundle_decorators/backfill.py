"""Backfill definitions for time-based and static key enumeration.

Backfill definitions declare the universe of valid ``logical_date``
values for a job.  They are used by the ``dbxdec backfill`` CLI command
to enumerate dates for bulk run submission.

Every job automatically receives a ``logical_date`` parameter.  At
runtime, task code reads it via the convenience helper::

    from databricks_bundle_decorators.backfill import current_logical_date

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
from datetime import datetime, timezone
from typing import ClassVar

import whenever

_logger = logging.getLogger(__name__)

#: The fixed job-parameter name that carries the logical date.
LOGICAL_DATE_PARAM: str = "logical_date"


class BackfillDef(ABC):
    """Base class for backfill definitions.

    Subclasses declare the universe of valid backfill keys for
    enumeration.  The ``dbxdec backfill`` CLI uses these to
    generate ``logical_date`` values.
    """

    @abstractmethod
    def keys(self, start: str | None = None, end: str | None = None) -> list[str]:
        """Enumerate concrete backfill key strings.

        Parameters
        ----------
        start:
            Override the start bound (inclusive).  Must use the same
            format as the definition's keys.
        end:
            Override the end bound (inclusive).  Must use the same
            format as the definition's keys.
        """
        ...


@dataclass(frozen=True)
class DailyBackfill(BackfillDef):
    """One key per calendar day.

    Keys are ISO-8601 dates: ``YYYY-MM-DD``.

    Parameters
    ----------
    start_date:
        First key (inclusive), e.g. ``"2024-01-01"``.
    end_date:
        Last key (inclusive).  Defaults to yesterday in *tz*.
    tz:
        IANA timezone name (e.g. ``"UTC"``, ``"Europe/Berlin"``).
        Used to determine "yesterday" when *end_date* is omitted.
    """

    _FMT: ClassVar[str] = "%Y-%m-%d"

    start_date: str
    end_date: str | None = None
    tz: str = "UTC"

    def _parse(self, key: str) -> whenever.Date:
        return whenever.Date.from_py_date(datetime.strptime(key, self._FMT).date())

    def keys(self, start: str | None = None, end: str | None = None) -> list[str]:
        s = self._parse(start or self.start_date)
        if end is not None:
            e = self._parse(end)
        elif self.end_date is not None:
            e = self._parse(self.end_date)
        else:
            e = whenever.ZonedDateTime.now(self.tz).date().subtract(days=1)

        keys: list[str] = []
        while s <= e:
            keys.append(s.py_date().strftime(self._FMT))
            s = s.add(days=1)
        return keys


@dataclass(frozen=True)
class WeeklyBackfill(BackfillDef):
    """One key per ISO week.

    Keys are ISO week dates: ``YYYY-WNN`` (e.g. ``"2024-W03"``).

    The default ``end_date`` is the Monday of the most recent **completed**
    ISO week (i.e. the week whose Sunday has already passed).

    Parameters
    ----------
    start_date:
        First key (inclusive), e.g. ``"2024-W01"``.
    end_date:
        Last key (inclusive).  Defaults to the most recent
        completed ISO week.
    tz:
        IANA timezone name.  Used to determine "today" when
        *end_date* is omitted.
    """

    _FMT: ClassVar[str] = "%G-W%V"

    start_date: str
    end_date: str | None = None
    tz: str = "UTC"

    def _parse_iso_week(self, key: str) -> whenever.Date:
        """Parse an ISO-week key into a Monday ``Date``."""
        return whenever.Date.from_py_date(
            datetime.strptime(key + "-1", self._FMT + "-%u").date()
        )

    def keys(self, start: str | None = None, end: str | None = None) -> list[str]:
        s = self._parse_iso_week(start or self.start_date)
        if end is not None:
            e = self._parse_iso_week(end)
        elif self.end_date is not None:
            e = self._parse_iso_week(self.end_date)
        else:
            today = whenever.ZonedDateTime.now(self.tz).date()
            # Most recent completed week: Monday of current week minus 7 days
            # day_of_week() returns Weekday enum (MONDAY=1..SUNDAY=7)
            weekday_offset = today.day_of_week().value - 1  # 0 for Monday
            e = today.subtract(days=weekday_offset + 7)

        keys: list[str] = []
        while s <= e:
            keys.append(s.py_date().strftime(self._FMT))
            s = s.add(weeks=1)
        return keys


@dataclass(frozen=True)
class MonthlyBackfill(BackfillDef):
    """One key per calendar month.

    Keys are ISO-8601 dates pinned to the first of the month:
    ``YYYY-MM-01`` (e.g. ``"2024-01-01"``).

    Parameters
    ----------
    start_date:
        First key (inclusive), e.g. ``"2024-01-01"``.
    end_date:
        Last key (inclusive).  Defaults to the previous
        completed month.
    tz:
        IANA timezone name.  Used to determine "today" when
        *end_date* is omitted.
    """

    _FMT: ClassVar[str] = "%Y-%m-01"

    start_date: str
    end_date: str | None = None
    tz: str = "UTC"

    def _parse_month(self, key: str) -> whenever.Date:
        """Parse a month key into the first day of that month."""
        d = datetime.strptime(key, self._FMT).date()
        return whenever.Date(d.year, d.month, 1)

    def keys(self, start: str | None = None, end: str | None = None) -> list[str]:
        s = self._parse_month(start or self.start_date)
        if end is not None:
            e = self._parse_month(end)
        elif self.end_date is not None:
            e = self._parse_month(self.end_date)
        else:
            today = whenever.ZonedDateTime.now(self.tz).date()
            # Previous completed month
            e = today.replace(day=1).subtract(days=1).replace(day=1)

        keys: list[str] = []
        while s <= e:
            keys.append(s.py_date().strftime(self._FMT))
            s = s.add(months=1)
        return keys


@dataclass(frozen=True)
class HourlyBackfill(BackfillDef):
    """One key per hour.

    Keys are truncated ISO-8601 timestamps: ``YYYY-MM-DDTHH``
    (e.g. ``"2024-01-01T00"``).

    All enumeration is performed in the specified timezone (default UTC)
    so that daylight-saving transitions are handled correctly — hours
    that don't exist are skipped, and ambiguous hours appear once.

    Parameters
    ----------
    start_date:
        First key (inclusive), e.g. ``"2024-01-01T00"``.
    end_date:
        Last key (inclusive).  Defaults to the previous
        completed hour in *tz*.
    tz:
        IANA timezone name (e.g. ``"UTC"``, ``"America/New_York"``).
        Defaults to ``"UTC"`` to sidestep daylight-saving issues.
    """

    _FMT: ClassVar[str] = "%Y-%m-%dT%H"

    start_date: str
    end_date: str | None = None
    tz: str = "UTC"

    def _parse_hour(self, key: str) -> whenever.ZonedDateTime:
        """Parse an hour key into a ``ZonedDateTime``.

        Ambiguous wall-clock times (e.g. the repeated hour during a
        fall-back DST transition) resolve to the *first* occurrence.
        """
        naive = datetime.strptime(key, self._FMT)
        return whenever.ZonedDateTime(
            naive.year, naive.month, naive.day, naive.hour, tz=self.tz
        )

    def keys(self, start: str | None = None, end: str | None = None) -> list[str]:
        s = self._parse_hour(start or self.start_date)
        if end is not None:
            e = self._parse_hour(end)
        elif self.end_date is not None:
            e = self._parse_hour(self.end_date)
        else:
            now = whenever.ZonedDateTime.now(self.tz)
            # Previous completed hour
            e = now.replace(minute=0, second=0, nanosecond=0).subtract(hours=1)

        keys: list[str] = []
        seen: set[str] = set()
        cur = s
        while cur <= e:
            key = cur.py_datetime().strftime(self._FMT)
            if key not in seen:
                keys.append(key)
                seen.add(key)
            cur = cur.add(hours=1)
        return keys


@dataclass(frozen=True)
class StaticBackfill(BackfillDef):
    """A fixed set of backfill keys.

    Parameters
    ----------
    keys:
        The complete list of valid backfill keys.

    Example
    -------
    ::

        StaticBackfill(keys=["us", "eu", "jp"])
    """

    _keys: list[str] = field(default_factory=list)

    def __init__(self, keys: list[str]) -> None:
        # Defensive copy so mutations to the caller's list don't leak.
        object.__setattr__(self, "_keys", list(keys))

    def keys(self, start: str | None = None, end: str | None = None) -> list[str]:
        if start is not None or end is not None:
            warnings.warn(
                "StaticBackfill.keys() ignores 'start' and 'end' "
                "arguments. All keys are always returned.",
                stacklevel=2,
            )
        return list(self._keys)


def current_logical_date() -> datetime:
    """Read the current logical date from ``params["logical_date"]``.

    The value is always an ISO-8601 string in *params* and is parsed
    into a timezone-aware ``datetime`` before returning.

    Raises
    ------
    RuntimeError
        If ``logical_date`` is missing or empty.  This indicates the
        job has no backfill definition and was not started via the
        backfill CLI.

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
            "logical_date parameter. Use @job(backfill=...) and "
            "the backfill CLI, or pass logical_date explicitly."
        )
    return _parse_logical_date_str(raw)


def _parse_logical_date_str(raw: str) -> datetime:
    """Parse a logical-date string into a timezone-aware ``datetime``.

    Uses ``datetime.fromisoformat`` which, on Python 3.12+, handles
    all built-in backfill-key formats:

    - ``DailyBackfill``: ``2024-01-15``
    - ``WeeklyBackfill``: ``2024-W03``  (ISO week date)
    - ``MonthlyBackfill``: ``2024-01-01`` (first-of-month)
    - ``HourlyBackfill``: ``2024-01-15T00``
    - Full timestamps: ``2024-01-15T00:00:00+00:00``

    The returned datetime is always timezone-aware (defaults to UTC
    when the parsed value is naïve).
    """
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        raise ValueError(
            f"Cannot parse logical_date {raw!r}. "
            f"Expected an ISO-8601 string parseable by "
            f"datetime.fromisoformat() (e.g. YYYY-MM-DD, YYYY-Www, "
            f"YYYY-MM-DDThh)."
        ) from None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
