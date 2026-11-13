"""US equity trading calendar foundation (XNYS).

Provides deterministic, injected-clock helpers for deciding whether a
requested date window contains any expected NYSE (XNYS) trading session.

Intended for the extractor integration (M2.2) to distinguish expected
no-data windows (weekends, scheduled holidays, same-day reruns) from
unexpected empty provider responses (invalid symbols, throttling, outages).

Design principles:
* Single calendar library: ``exchange_calendars`` with the ``XNYS`` calendar.
* Market timezone is ``America/New_York``; the machine local timezone is
  never used.
* All ``session_close`` values are timezone-aware UTC timestamps from
  ``exchange_calendars``; comparisons are performed in UTC.
* The data-availability buffer is applied to the *actual* session close,
  including early-close sessions.
* ``now`` is injectable so tests are deterministic and do not depend on the
  real current date or network access.

No network access is performed by this module.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional, Union
from zoneinfo import ZoneInfo

import exchange_calendars as xc
import pandas as pd

CALENDAR_NAME = "XNYS"
MARKET_TIMEZONE = "America/New_York"
MARKET_TZ = ZoneInfo(MARKET_TIMEZONE)

# Exchange calendars publishes sessions for a bounded range. The pipeline
# only needs to cover the foreseeable operating window of this project.
CALENDAR_START = "2000-01-01"
CALENDAR_END = "2050-12-31"

# Default availability buffer applied after the actual session close
# (including early closes) before a session is considered complete and its
# data expected.
DEFAULT_DATA_AVAILABILITY_BUFFER = timedelta(minutes=60)

# Acceptable start/end inputs for the public helpers.
_DateInput = Union[pd.Timestamp, datetime, date, str]


def _load_calendar() -> xc.ExchangeCalendar:
    """Load and cache the XNYS calendar over the project window."""
    return xc.get_calendar(CALENDAR_NAME, start=CALENDAR_START, end=CALENDAR_END)


_CALENDAR = _load_calendar()


def _as_session_date(value: _DateInput) -> pd.Timestamp:
    """Coerce a start/end input to a normalized market-session date.

    Datetime inputs must be timezone-aware (naive values raise
    ``ValueError``) and are converted to the market timezone before being
    normalized to midnight, so the machine local timezone never leaks in.
    Date and string inputs are treated as calendar dates.
    """
    if isinstance(value, pd.Timestamp):
        ts = value
    elif isinstance(value, datetime):
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            raise ValueError("datetime inputs must be timezone-aware")
    elif isinstance(value, str):
        ts = pd.Timestamp(value)
    else:
        ts = pd.Timestamp(value)  # date-like

    if ts.tzinfo is not None:
        ts = ts.tz_convert(MARKET_TZ)
    return ts.normalize()


def _validate_buffer(buffer: timedelta) -> None:
    """Reject negative availability buffers.

    Zero and positive buffers are preserved; negative values are meaningless
    for a data-availability window and raise ``ValueError``.
    """
    if buffer < timedelta(0):
        raise ValueError("buffer must be a non-negative timedelta")


def last_completed_session(
    now: Optional[datetime] = None,
    *,
    buffer: timedelta = DEFAULT_DATA_AVAILABILITY_BUFFER,
) -> Optional[pd.Timestamp]:
    """Return the last completed XNYS trading session on or before ``now``.

    A session counts as completed once
    ``session_close + buffer <= now``. The buffer is applied to the actual
    close, so early-close sessions (for example the day after Thanksgiving)
    become available earlier than normal-close sessions.

    Parameters
    ----------
    now:
        Timezone-aware ``datetime`` used as "now". Defaults to the current
        UTC time. Naive values raise ``ValueError``.
    buffer:
        Availability buffer added to each session's close.

    Returns
    -------
    pd.Timestamp or None
        The last completed session date, or ``None`` if no session has
        completed yet (for example before the first session in the calendar
        window plus the buffer).
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    _validate_buffer(buffer)
    now_utc = now.astimezone(timezone.utc)

    cal = _CALENDAR
    deadline = now_utc - buffer

    # session_close increases monotonically with the session date (each close
    # lands on its own session date), so the predicate
    # ``session_close <= deadline`` is monotonic. Binary search for the
    # rightmost session satisfying it instead of scanning the whole calendar
    # from 2050 backwards on every call.
    sessions = cal.sessions
    lo, hi = 0, len(sessions) - 1
    result: Optional[pd.Timestamp] = None
    while lo <= hi:
        mid = (lo + hi) // 2
        mid_close = cal.session_close(sessions[mid])
        if mid_close <= deadline:
            result = sessions[mid]
            lo = mid + 1
        else:
            hi = mid - 1
    return result


def has_expected_session(
    start: Optional[_DateInput] = None,
    end: Optional[_DateInput] = None,
    *,
    now: Optional[datetime] = None,
    buffer: timedelta = DEFAULT_DATA_AVAILABILITY_BUFFER,
    end_inclusive: bool = True,
) -> bool:
    """Return True if the interval contains at least one expected session.

    ``end`` defaults to :func:`last_completed_session` and is clamped to it,
    so only completed sessions count as expected. Returns ``False`` for
    reversed, out-of-bounds, weekend, holiday, or otherwise session-free
    intervals.

    Parameters
    ----------
    start:
        Inclusive start of the window. Defaults to the first calendar
        session.
    end:
        End of the window. Defaults to the last completed session and is
        clamped to it. Pass ``None`` to evaluate up to the last completed
        session.
    now:
        Timezone-aware ``datetime`` used to derive the last completed
        session when ``end`` is not given.
    buffer:
        Availability buffer used to derive the last completed session.
    end_inclusive:
        Whether the ``end`` boundary itself counts as containing an
        expected session.
    """
    cal = _CALENDAR
    _validate_buffer(buffer)
    last = last_completed_session(now=now, buffer=buffer)

    # No completed session at all -> nothing can be expected yet.
    if last is None:
        return False

    start_ts = cal.sessions[0] if start is None else _as_session_date(start)

    if end is None:
        end_ts = last
    else:
        end_ts = _as_session_date(end)
        # Clamp end to the last completed session: sessions after it are
        # not complete and must not be counted as expected.
        if end_ts > last:
            end_ts = last

    if not end_inclusive:
        end_ts = end_ts - pd.Timedelta(days=1)

    # Reversed interval.
    if start_ts > end_ts:
        return False

    # Out of calendar bounds: clamp to the calendar range before querying.
    first = cal.sessions[0]
    cal_last = cal.sessions[-1]
    if end_ts < first or start_ts > cal_last:
        return False
    lo = max(start_ts, first)
    hi = min(end_ts, cal_last)

    return len(cal.sessions_in_range(lo, hi)) > 0
