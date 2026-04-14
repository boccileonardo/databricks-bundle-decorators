"""Backfill definitions for time-based and static key enumeration.

Backfill definitions declare the universe of valid ``backfill_key``
values for a job.  They are used by the ``dbxdec backfill`` CLI command
to enumerate keys for bulk run submission.

Every job automatically receives a ``backfill_key`` parameter.  At
runtime, task code reads the raw key via `get_backfill_key`, or
parses it as a datetime via `get_run_logical_date`::

    from databricks_bundle_decorators.backfill import get_backfill_key


    @task
    def extract() -> pl.DataFrame:
        key = get_backfill_key()  # returns str, raises if unset
        ...
"""

from __future__ import annotations

import json
import logging
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar

import whenever

from databricks_bundle_decorators.context import params
from databricks_bundle_decorators.registry import _JOB_REGISTRY

_logger = logging.getLogger(__name__)

#: The fixed job-parameter name that carries the backfill key.
BACKFILL_KEY_PARAM: str = "backfill_key"

#: Tag key used to store the serialised backfill definition on
#: the deployed Databricks job.
BACKFILL_TAG: str = "dbxdec.backfill"


def _serialize_backfill_tag(defn: BackfillDef) -> str:
    """Serialise a `BackfillDef` to a compact JSON string for a job tag.

    Only the fields relevant to the concrete type are included.
    """
    d: dict[str, Any]
    if isinstance(defn, StaticBackfill):
        d = {"type": "static", "keys": defn._keys}
    elif isinstance(
        defn, (DailyBackfill, WeeklyBackfill, MonthlyBackfill, HourlyBackfill)
    ):
        type_map = {
            DailyBackfill: "daily",
            WeeklyBackfill: "weekly",
            MonthlyBackfill: "monthly",
            HourlyBackfill: "hourly",
        }
        d = {"type": type_map[type(defn)], "start_date": defn.start_date}
        if defn.end_date is not None:
            d["end_date"] = defn.end_date
        if defn.tz != "UTC":
            d["tz"] = defn.tz
    else:
        msg = f"Unsupported BackfillDef type: {type(defn).__name__}"
        raise TypeError(msg)
    return json.dumps(d, separators=(",", ":"))


def _deserialize_backfill_tag(raw: str | dict[str, Any]) -> BackfillDef:
    """Reconstruct a `BackfillDef` from its serialised JSON form.

    Accepts either the JSON string produced by `_serialize_backfill_tag`
    or an already-parsed dictionary.
    """
    d: dict[str, Any] = json.loads(raw) if isinstance(raw, str) else raw
    kind = d.get("type", "static")
    if kind == "static":
        return StaticBackfill(keys=d["keys"])
    cls_map: dict[
        str, type[DailyBackfill | WeeklyBackfill | MonthlyBackfill | HourlyBackfill]
    ] = {
        "daily": DailyBackfill,
        "weekly": WeeklyBackfill,
        "monthly": MonthlyBackfill,
        "hourly": HourlyBackfill,
    }
    cls = cls_map.get(kind)
    if cls is None:
        msg = f"Unknown backfill type: {kind!r}"
        raise ValueError(msg)
    return cls(
        start_date=d["start_date"],
        end_date=d.get("end_date"),
        tz=d.get("tz", "UTC"),
    )


class BackfillDef(ABC):
    """Base class for backfill definitions.

    Subclasses declare the universe of valid backfill keys for
    enumeration.  The ``dbxdec backfill`` CLI uses these to
    generate ``backfill_key`` values.
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

    def current_key(self) -> str | None:
        """Return the backfill key for the current point in time.

        Used as a fallback when a job with a backfill definition is
        triggered without an explicit ``backfill_key`` (e.g. by a cron
        schedule or file-arrival trigger).

        Time-based subclasses return the key matching "now" in their
        configured timezone.  `StaticBackfill` returns ``None``
        because there is no sensible default.
        """
        return None


@dataclass(frozen=True)
class DailyBackfill(BackfillDef):
    """One key per calendar day.

    Keys are ISO-8601 dates: ``YYYY-MM-DD``.

    Parameters
    ----------
    start_date:
        First key (inclusive), e.g. ``"2024-01-01"``.
    end_date:
        Last key (inclusive).  Defaults to today in *tz*.
    tz:
        IANA timezone name (e.g. ``"UTC"``, ``"Europe/Berlin"``).
        Used to determine "yesterday" when *end_date* is omitted.
    """

    _FMT: ClassVar[str] = "YYYY-MM-DD"

    start_date: str
    end_date: str | None = None
    tz: str = "UTC"

    def _parse(self, key: str) -> whenever.Date:
        return whenever.Date.parse(key, format=self._FMT)

    def current_key(self) -> str:
        """Today's date in the configured timezone."""
        return whenever.ZonedDateTime.now(self.tz).date().format(self._FMT)

    def keys(self, start: str | None = None, end: str | None = None) -> list[str]:
        s = self._parse(start or self.start_date)
        if end is not None:
            e = self._parse(end)
        elif self.end_date is not None:
            e = self._parse(self.end_date)
        else:
            e = whenever.ZonedDateTime.now(self.tz).date()

        keys: list[str] = []
        while s <= e:
            keys.append(s.format(self._FMT))
            s = s.add(days=1)
        return keys


@dataclass(frozen=True)
class WeeklyBackfill(BackfillDef):
    """One key per ISO week.

    Keys are ISO week dates: ``YYYY-WNN`` (e.g. ``"2024-W03"``).

    The default ``end_date`` is the Monday of the current ISO week.

    Parameters
    ----------
    start_date:
        First key (inclusive), e.g. ``"2024-W01"``.
    end_date:
        Last key (inclusive).  Defaults to the current
        ISO week.
    tz:
        IANA timezone name.  Used to determine "today" when
        *end_date* is omitted.
    """

    start_date: str
    end_date: str | None = None
    tz: str = "UTC"

    @staticmethod
    def _fmt_iso_week(date: whenever.Date) -> str:
        """Format a ``Date`` as ``YYYY-WNN``."""
        iwd = date.iso_week_date()
        return f"{iwd.year}-W{iwd.week:02d}"

    def _parse_iso_week(self, key: str) -> whenever.Date:
        """Parse an ISO-week key into a Monday ``Date``."""
        return whenever.Date(
            datetime.strptime(key + "-1", "%G-W%V-%u").date()  # noqa: DTZ007
        )

    def current_key(self) -> str:
        """Current ISO week in the configured timezone."""
        today = whenever.ZonedDateTime.now(self.tz).date()
        return self._fmt_iso_week(today)

    def keys(self, start: str | None = None, end: str | None = None) -> list[str]:
        s = self._parse_iso_week(start or self.start_date)
        if end is not None:
            e = self._parse_iso_week(end)
        elif self.end_date is not None:
            e = self._parse_iso_week(self.end_date)
        else:
            today = whenever.ZonedDateTime.now(self.tz).date()
            # Monday of the current ISO week
            weekday_offset = today.day_of_week().value - 1  # 0 for Monday
            e = today.subtract(days=weekday_offset)

        keys: list[str] = []
        while s <= e:
            keys.append(self._fmt_iso_week(s))
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
        Last key (inclusive).  Defaults to the current month.
    tz:
        IANA timezone name.  Used to determine "today" when
        *end_date* is omitted.
    """

    _FMT: ClassVar[str] = "YYYY-MM-DD"

    start_date: str
    end_date: str | None = None
    tz: str = "UTC"

    def _parse_month(self, key: str) -> whenever.Date:
        """Parse a month key into the first day of that month."""
        d = whenever.Date.parse(key, format=self._FMT)
        return d.replace(day=1)

    def current_key(self) -> str:
        """First day of the current month in the configured timezone."""
        today = whenever.ZonedDateTime.now(self.tz).date()
        return today.replace(day=1).format(self._FMT)

    def keys(self, start: str | None = None, end: str | None = None) -> list[str]:
        s = self._parse_month(start or self.start_date)
        if end is not None:
            e = self._parse_month(end)
        elif self.end_date is not None:
            e = self._parse_month(self.end_date)
        else:
            today = whenever.ZonedDateTime.now(self.tz).date()
            # Current month
            e = today.replace(day=1)

        keys: list[str] = []
        while s <= e:
            keys.append(s.format(self._FMT))
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
        Last key (inclusive).  Defaults to the current
        hour in *tz*.
    tz:
        IANA timezone name (e.g. ``"UTC"``, ``"America/New_York"``).
        Defaults to ``"UTC"`` to sidestep daylight-saving issues.
    """

    _FMT: ClassVar[str] = "YYYY-MM-DD'T'hh"

    start_date: str
    end_date: str | None = None
    tz: str = "UTC"

    def _parse_hour(self, key: str) -> whenever.ZonedDateTime:
        """Parse an hour key into a ``ZonedDateTime``.

        Ambiguous wall-clock times (e.g. the repeated hour during a
        fall-back DST transition) resolve to the *first* occurrence.
        """
        naive = datetime.strptime(key, "%Y-%m-%dT%H")  # noqa: DTZ007
        return whenever.ZonedDateTime(
            naive.year, naive.month, naive.day, naive.hour, tz=self.tz
        )

    def current_key(self) -> str:
        """Current hour in the configured timezone."""
        now = whenever.ZonedDateTime.now(self.tz)
        return now.replace(minute=0, second=0, nanosecond=0).format(self._FMT)

    def keys(self, start: str | None = None, end: str | None = None) -> list[str]:
        s = self._parse_hour(start or self.start_date)
        if end is not None:
            e = self._parse_hour(end)
        elif self.end_date is not None:
            e = self._parse_hour(self.end_date)
        else:
            now = whenever.ZonedDateTime.now(self.tz)
            # Current hour
            e = now.replace(minute=0, second=0, nanosecond=0)

        keys: list[str] = []
        seen: set[str] = set()
        cur = s
        while cur <= e:
            key = cur.format(self._FMT)
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


def get_backfill_key(*, validate: bool = True) -> str:
    """Return the raw backfill key for the current job run.

    Reads the ``backfill_key`` job parameter and optionally validates
    it against the job's `BackfillDef` boundaries.

    When the parameter is missing or empty **and** the job has a
    time-based `BackfillDef`, the key is auto-derived from the
    current time (e.g. today's date for `DailyBackfill`) and a
    warning is logged.  This allows cron-triggered and file-arrival
    runs to work without explicitly supplying the key.

    For time-based backfills the key is an ISO-8601 date/time string;
    for `StaticBackfill` it is one of the declared keys (e.g.
    ``"us"``, ``"eu"``).

    Parameters
    ----------
    validate:
        When ``True`` (the default), verify that the key is valid for
        the job's `BackfillDef`.  A `ValueError` is raised if the
        key is out of range.  Ignored when the job has no backfill
        definition.

    Raises
    ------
    RuntimeError
        If ``backfill_key`` is missing or empty and no automatic
        key can be derived (e.g. the job has no backfill definition,
        or uses `StaticBackfill`).
    ValueError
        If *validate* is ``True`` and the backfill key is outside the
        backfill definition's boundaries.

    Returns
    -------
    str
        The raw backfill key string.
    """
    raw = params.get(BACKFILL_KEY_PARAM, "")
    if not raw:
        raw = _auto_derive_backfill_key()
    if validate:
        job_name: str | None = params.get("__job_name__")
        _validate_backfill_key(raw, job_name)

    return raw


def _auto_derive_backfill_key() -> str:
    """Try to derive a backfill key from the job's `BackfillDef`.

    Returns the derived key string or raises `RuntimeError` if no
    automatic derivation is possible.
    """

    job_name: str | None = params.get("__job_name__")
    backfill: BackfillDef | None = None
    if job_name is not None:
        job_meta = _JOB_REGISTRY.get(job_name)
        if job_meta is not None:
            backfill = job_meta.backfill

    if backfill is not None:
        key = backfill.current_key()
        if key is not None:
            _logger.warning(
                "backfill_key was not provided for this run; "
                "auto-assigned to %r based on the current time. "
                "To set it explicitly, use the backfill CLI or "
                "pass backfill_key as a job parameter.",
                key,
            )
            return key

    raise RuntimeError(
        "backfill_key is not set. "
        "This usually means the job was not invoked with a "
        "backfill_key parameter. Use @job(backfill=...) and "
        "the backfill CLI, or pass backfill_key explicitly."
    )


def get_run_logical_date(*, validate: bool = True) -> datetime:
    """Return the backfill key parsed as a timezone-aware ``datetime``.

    Convenience wrapper around `get_backfill_key` for time-based
    backfills (`DailyBackfill`, `WeeklyBackfill`, etc.).  Not
    suitable for `StaticBackfill` with non-date keys — use
    `get_backfill_key` instead.

    Parameters
    ----------
    validate:
        When ``True`` (the default), verify that the key is valid for
        the job's `BackfillDef`.  A `ValueError` is raised if the
        key is out of range.  Ignored when the job has no backfill
        definition.

    Raises
    ------
    RuntimeError
        If ``backfill_key`` is missing or empty.
    ValueError
        If the key cannot be parsed as an ISO-8601 date/time, or if
        *validate* is ``True`` and it falls outside the backfill
        definition's boundaries.

    Returns
    -------
    datetime
        Timezone-aware datetime representing the backfill key.
    """
    raw = get_backfill_key(validate=validate)
    return _parse_logical_date_str(raw)


def _validate_backfill_key(raw: str, job_name: str | None) -> None:
    """Check *raw* is within the current job's backfill boundaries."""
    if job_name is None:
        return
    job_meta = _JOB_REGISTRY.get(job_name)
    if job_meta is None or job_meta.backfill is None:
        return

    backfill = job_meta.backfill

    if isinstance(backfill, StaticBackfill):
        if raw not in backfill._keys:
            raise ValueError(
                f"backfill_key {raw!r} is not in the StaticBackfill "
                f"keys for job {job_name!r}. "
                f"Valid keys: {backfill._keys}"
            )
        return

    # Time-based backfill defs all have start_date and end_date
    start: str | None = getattr(backfill, "start_date", None)
    if start is None:
        return

    dt = _parse_logical_date_str(raw)
    start_dt = _parse_logical_date_str(start)

    if dt < start_dt:
        raise ValueError(
            f"backfill_key {raw!r} is before the backfill "
            f"start_date {start!r} for job {job_name!r}."
        )

    end: str | None = getattr(backfill, "end_date", None)
    if end is not None:
        end_dt = _parse_logical_date_str(end)
        if dt > end_dt:
            raise ValueError(
                f"backfill_key {raw!r} is after the backfill "
                f"end_date {end!r} for job {job_name!r}."
            )


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
            f"Cannot parse backfill_key {raw!r} as a date. "
            f"Expected an ISO-8601 string parseable by "
            f"datetime.fromisoformat() (e.g. YYYY-MM-DD, YYYY-Www, "
            f"YYYY-MM-DDThh)."
        ) from None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt
