"""Focused tests for ingestion.market_calendar (M2.1).

All timestamps are injected and fixed. Tests do not depend on the real
current date, the machine local timezone, or network access.

XNYS reference points used (exchange_calendars 4.10.1):
* 2026-11-26 Thursday: Thanksgiving, market closed.
* 2026-11-27 Friday: early close at 18:00 UTC (13:00 ET).
* 2026-11-28 Saturday / 2026-11-29 Sunday: weekend, no sessions.
* 2026-11-30 Monday: normal close at 21:00 UTC (16:00 EST).
* 2026-10-30 Friday: normal close at 20:00 UTC (16:00 EDT).
* 2026-11-02 Monday: normal close at 21:00 UTC (16:00 EST, after DST fall-back).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import ingestion.market_calendar as market_calendar_module
from ingestion.market_calendar import (
    CALENDAR_NAME,
    DEFAULT_DATA_AVAILABILITY_BUFFER,
    MARKET_TIMEZONE,
    has_expected_session,
    last_completed_session,
)


def _utc(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    """Build a fixed, timezone-aware UTC datetime."""
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

def test_calendar_constants():
    assert CALENDAR_NAME == "XNYS"
    assert MARKET_TIMEZONE == "America/New_York"
    assert DEFAULT_DATA_AVAILABILITY_BUFFER == timedelta(minutes=60)


# ---------------------------------------------------------------------------
# last_completed_session
# ---------------------------------------------------------------------------

def test_last_completed_session_naive_now_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        last_completed_session(now=datetime(2026, 11, 30, 22, 0))


def test_last_completed_session_default_now_is_utc(monkeypatch):
    # Default clock must not rely on the machine local timezone or the real
    # current date. Monkeypatch the module-level ``datetime`` so ``now()``
    # returns a fixed UTC instant, then verify the result is derived from it.
    fixed_now = _utc(2026, 11, 30, 22, 0)

    class _FixedDateTime:
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(market_calendar_module, "datetime", _FixedDateTime)

    result = last_completed_session()
    # 2026-11-30 normal close is 21:00 UTC; +60 min buffer -> 22:00 UTC.
    assert result == pd.Timestamp("2026-11-30")


def test_last_completed_session_before_early_close_buffer():
    # 2026-11-27 is an early-close session (close 18:00 UTC). With a 60 min
    # buffer it completes at 19:00 UTC. At 18:30 UTC it is not yet complete,
    # so the last completed session is the prior normal day, 2026-11-25
    # (2026-11-26 Thanksgiving is closed).
    result = last_completed_session(now=_utc(2026, 11, 27, 18, 30))
    assert result == pd.Timestamp("2026-11-25")


def test_last_completed_session_after_early_close_buffer():
    # At 19:00 UTC exactly (close + buffer) the early-close session is complete.
    result = last_completed_session(now=_utc(2026, 11, 27, 19, 0))
    assert result == pd.Timestamp("2026-11-27")


def test_last_completed_session_normal_day_before_close_buffer():
    # 2026-11-30 normal close is 21:00 UTC; +60 min buffer -> 22:00 UTC.
    # At 21:30 UTC the session is not complete yet -> last is 2026-11-27.
    result = last_completed_session(now=_utc(2026, 11, 30, 21, 30))
    assert result == pd.Timestamp("2026-11-27")


def test_last_completed_session_normal_day_after_close_buffer():
    result = last_completed_session(now=_utc(2026, 11, 30, 22, 0))
    assert result == pd.Timestamp("2026-11-30")


def test_last_completed_session_skips_thanksgiving_holiday():
    # 2026-11-26 is Thanksgiving (closed). Even well after the prior close,
    # the last completed session remains 2026-11-25 until 2026-11-27 completes.
    result = last_completed_session(now=_utc(2026, 11, 26, 12, 0))
    assert result == pd.Timestamp("2026-11-25")


def test_last_completed_session_returns_session_date():
    result = last_completed_session(now=_utc(2026, 11, 30, 22, 0))
    assert isinstance(result, pd.Timestamp)


# ---------------------------------------------------------------------------
# has_expected_session
# ---------------------------------------------------------------------------

def test_saturday_no_expected_session():
    assert has_expected_session("2026-11-28", "2026-11-28") is False


def test_sunday_no_expected_session():
    assert has_expected_session("2026-11-29", "2026-11-29") is False


def test_weekend_range_no_expected_session():
    assert has_expected_session("2026-11-28", "2026-11-29") is False


def test_thanksgiving_holiday_no_expected_session():
    assert has_expected_session("2026-11-26", "2026-11-26") is False


def test_normal_day_before_close_no_expected_session():
    # 2026-11-30 is not complete at 21:30 UTC (buffer not yet expired),
    # so a window covering only that day has no expected completed session.
    assert has_expected_session(
        "2026-11-30", "2026-11-30", now=_utc(2026, 11, 30, 21, 30)
    ) is False


def test_normal_day_after_close_buffer_expected_session():
    assert has_expected_session(
        "2026-11-30", "2026-11-30", now=_utc(2026, 11, 30, 22, 0)
    ) is True


def test_early_close_session_expected_after_buffer():
    assert has_expected_session(
        "2026-11-27", "2026-11-27", now=_utc(2026, 11, 27, 19, 0)
    ) is True


def test_early_close_session_not_expected_before_buffer():
    assert has_expected_session(
        "2026-11-27", "2026-11-27", now=_utc(2026, 11, 27, 18, 30)
    ) is False


def test_empty_interval_returns_false():
    # Same start/end on a non-session day.
    assert has_expected_session("2026-11-26", "2026-11-26") is False


def test_reversed_interval_returns_false():
    assert has_expected_session("2026-11-30", "2026-11-27") is False


def test_start_after_last_completed_returns_false():
    # Last completed session is 2026-11-25 (11-27 not yet complete at 10:00
    # UTC). A window starting 2026-11-27 is after the last completed session.
    assert has_expected_session(
        "2026-11-27", now=_utc(2026, 11, 27, 10, 0)
    ) is False


def test_window_spanning_completed_session_is_true():
    assert has_expected_session(
        "2026-11-25", "2026-11-27", now=_utc(2026, 11, 27, 19, 0)
    ) is True


def test_end_clamped_to_last_completed_session():
    # End requested in the future is clamped to the last completed session.
    assert has_expected_session(
        "2026-11-25", "2026-12-31", now=_utc(2026, 11, 27, 19, 0)
    ) is True


def test_dst_transition_close_times_handled():
    # EDT close 2026-10-30 is 20:00 UTC; EST close 2026-11-02 is 21:00 UTC.
    # The buffer is applied to the actual close, so completion differs by DST.
    assert last_completed_session(now=_utc(2026, 10, 30, 21, 0)) == pd.Timestamp(
        "2026-10-30"
    )
    assert last_completed_session(now=_utc(2026, 10, 30, 20, 59)) == pd.Timestamp(
        "2026-10-29"
    )
    assert last_completed_session(now=_utc(2026, 11, 2, 22, 0)) == pd.Timestamp(
        "2026-11-02"
    )
    assert last_completed_session(now=_utc(2026, 11, 2, 21, 59)) == pd.Timestamp(
        "2026-10-30"
    )


def test_has_expected_session_rejects_naive_datetime():
    with pytest.raises(ValueError, match="timezone-aware"):
        has_expected_session(
            datetime(2026, 11, 30), datetime(2026, 11, 30)
        )


# ---------------------------------------------------------------------------
# Availability buffer validation
# ---------------------------------------------------------------------------

def test_last_completed_session_rejects_negative_buffer():
    with pytest.raises(ValueError, match="buffer"):
        last_completed_session(now=_utc(2026, 11, 30, 22, 0), buffer=timedelta(minutes=-1))


def test_has_expected_session_rejects_negative_buffer():
    with pytest.raises(ValueError, match="buffer"):
        has_expected_session(
            "2026-11-25",
            "2026-11-30",
            now=_utc(2026, 11, 30, 22, 0),
            buffer=timedelta(minutes=-1),
        )


def test_zero_buffer_is_accepted():
    # Zero buffer: a session completes exactly at its close.
    assert last_completed_session(
        now=_utc(2026, 11, 30, 21, 0), buffer=timedelta(0)
    ) == pd.Timestamp("2026-11-30")
    assert has_expected_session(
        "2026-11-30",
        "2026-11-30",
        now=_utc(2026, 11, 30, 21, 0),
        buffer=timedelta(0),
    ) is True


def test_positive_buffer_is_applied():
    # 30 min buffer: close 21:00 + 30 min = 21:30 <= now, so 2026-11-30
    # completes exactly at 21:30 UTC.
    assert last_completed_session(
        now=_utc(2026, 11, 30, 21, 30), buffer=timedelta(minutes=30)
    ) == pd.Timestamp("2026-11-30")
    assert last_completed_session(
        now=_utc(2026, 11, 30, 21, 29), buffer=timedelta(minutes=30)
    ) == pd.Timestamp("2026-11-27")
