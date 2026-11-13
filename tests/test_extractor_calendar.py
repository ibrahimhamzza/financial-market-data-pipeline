"""Offline tests for the XNYS calendar gate in ingestion.extractor.run_extraction.

The calendar gate calls has_expected_session(start=symbol_start, end=end)
before each provider call. All clocks are injected and fixed; no provider,
database, or real-date dependence.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import ingestion.extractor as extractor
import ingestion.market_calendar as market_calendar_module
from ingestion.extractor import run_extraction


def _utc(year, month, day, hour, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def _frame(symbol: str, trade_date="2026-11-30") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "trade_date": pd.Timestamp(trade_date),
                "open_price": 1.0,
                "high_price": 2.0,
                "low_price": 0.5,
                "close_price": 1.5,
                "volume": 10,
            }
        ]
    )


def _set_now(monkeypatch, now):
    """Inject a fixed clock into ingestion.market_calendar."""
    class _FixedDateTime:
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(market_calendar_module, "datetime", _FixedDateTime)


def _patch_run(monkeypatch, now=None, next_start="2026-11-28"):
    """Monkeypatch fetch_daily and next_start_date; return fetch call counter."""
    calls = {"fetch_daily": 0, "symbols": []}

    def fake_fetch_daily(symbol, start=None, end=None, source=None):
        calls["fetch_daily"] += 1
        calls["symbols"].append(symbol)
        return _frame(symbol).copy()

    monkeypatch.setattr(extractor, "fetch_daily", fake_fetch_daily)
    if callable(next_start):
        monkeypatch.setattr(extractor, "next_start_date", next_start)
    else:
        monkeypatch.setattr(extractor, "next_start_date", lambda s: next_start)
    if now is not None:
        _set_now(monkeypatch, now)
    return calls


_FRAME = _frame("AAPL")


def test_saturday_start_gated_out(monkeypatch):
    # now = Fri 2026-11-27 19:00 UTC -> last completed = Fri 11-27.
    # symbol_start = Sat 11-28 -> window [11-28, 11-27] reversed -> False.
    _set_now(monkeypatch, _utc(2026, 11, 27, 19, 0))
    calls = _patch_run(monkeypatch, next_start="2026-11-28")

    result = run_extraction(symbols=["AAPL"], incremental=True)

    assert calls["fetch_daily"] == 0
    assert getattr(result, "attrs", {}).get("no_new_data") is True
    assert result.attrs["symbols_checked"] == ["AAPL"]
    assert result.attrs["symbols_ok"] == []
    assert result.attrs["errors"] == {}


def test_monday_after_close_buffer_gated_in(monkeypatch):
    # now = Mon 2026-11-30 22:00 UTC -> last completed = Mon 11-30.
    # symbol_start = Sat 11-28 -> window [11-28, 11-30] contains Mon 11-30 -> True.
    _set_now(monkeypatch, _utc(2026, 11, 30, 22, 0))
    calls = _patch_run(monkeypatch, next_start="2026-11-28")

    result = run_extraction(symbols=["AAPL"], incremental=True)

    assert calls["fetch_daily"] == 1
    assert calls["symbols"] == ["AAPL"]
    assert not result.empty
    assert result.attrs["symbols_ok"] == ["AAPL"]
    assert result.attrs["symbols_checked"] == ["AAPL"]
    assert getattr(result, "attrs", {}).get("no_new_data") is not True


def test_same_day_rerun_gated_out(monkeypatch):
    # Watermark is already today (Mon 11-30); symbol_start = Tue 12-01.
    # now = Mon 11-30 22:00 UTC -> last completed = Mon 11-30.
    # Window [12-01, 11-30] reversed -> False -> skip provider call.
    _set_now(monkeypatch, _utc(2026, 11, 30, 22, 0))
    calls = _patch_run(monkeypatch, next_start="2026-12-01")

    result = run_extraction(symbols=["AAPL"], incremental=True)

    assert calls["fetch_daily"] == 0
    assert getattr(result, "attrs", {}).get("no_new_data") is True


def test_normal_day_before_close_buffer_gated_out(monkeypatch):
    # now = Mon 11-30 21:30 UTC -> last completed = Fri 11-27 (buffer not yet expired).
    # symbol_start = Sat 11-28 -> window [11-28, 11-27] reversed -> False.
    _set_now(monkeypatch, _utc(2026, 11, 30, 21, 30))
    calls = _patch_run(monkeypatch, next_start="2026-11-28")

    result = run_extraction(symbols=["AAPL"], incremental=True)

    assert calls["fetch_daily"] == 0
    assert getattr(result, "attrs", {}).get("no_new_data") is True


def test_early_close_before_buffer_gated_out(monkeypatch):
    # now = Fri 11-27 18:30 UTC -> early close 18:00 + 60 min buffer not yet expired;
    # last completed = Wed 11-25. symbol_start = Thu 11-26 (Thanksgiving).
    # Window [11-26, 11-25] reversed -> False.
    _set_now(monkeypatch, _utc(2026, 11, 27, 18, 30))
    calls = _patch_run(monkeypatch, next_start="2026-11-26")

    result = run_extraction(symbols=["AAPL"], incremental=True)

    assert calls["fetch_daily"] == 0
    assert getattr(result, "attrs", {}).get("no_new_data") is True


def test_early_close_after_buffer_gated_in(monkeypatch):
    # now = Fri 11-27 19:00 UTC -> early close complete; last completed = Fri 11-27.
    # symbol_start = Thu 11-26 -> window [11-26, 11-27] contains Fri 11-27 -> True.
    _set_now(monkeypatch, _utc(2026, 11, 27, 19, 0))
    calls = _patch_run(monkeypatch, next_start="2026-11-26")

    result = run_extraction(symbols=["AAPL"], incremental=True)

    assert calls["fetch_daily"] == 1
    assert not result.empty


def test_partial_symbol_one_gated_out_one_fetched(monkeypatch):
    # now = Mon 11-30 22:00 UTC -> last completed = Mon 11-30.
    # "GATED" starts Tue 12-01 (rerun) -> gate out. "FETCH" starts Sat 11-28 -> gate in.
    _set_now(monkeypatch, _utc(2026, 11, 30, 22, 0))

    def fake_next_start(symbol):
        return "2026-12-01" if symbol == "GATED" else "2026-11-28"

    calls = _patch_run(monkeypatch, next_start=fake_next_start)

    result = run_extraction(symbols=["GATED", "FETCH"], incremental=True)

    assert calls["fetch_daily"] == 1
    assert calls["symbols"] == ["FETCH"]
    assert result.attrs["symbols_ok"] == ["FETCH"]
    assert result.attrs["symbols_checked"] == ["GATED", "FETCH"]
    assert "GATED" not in result.attrs["errors"]


def test_explicit_backfill_range_evaluated_against_requested_window(monkeypatch):
    # incremental=False: symbol_start = explicit start, end honored.
    # now = Mon 11-30 22:00 UTC.
    # Range [11-25, 11-27] contains sessions -> gate in.
    _set_now(monkeypatch, _utc(2026, 11, 30, 22, 0))
    calls = _patch_run(monkeypatch, next_start=None)

    result = run_extraction(
        symbols=["AAPL"], incremental=False, start="2026-11-25", end="2026-11-27"
    )

    assert calls["fetch_daily"] == 1
    assert calls["symbols"] == ["AAPL"]


def test_explicit_backfill_weekend_range_gated_out(monkeypatch):
    # Range [Sat 11-28, Sun 11-29] contains no XNYS session -> gate out.
    _set_now(monkeypatch, _utc(2026, 11, 30, 22, 0))
    calls = _patch_run(monkeypatch, next_start=None)

    result = run_extraction(
        symbols=["AAPL"], incremental=False, start="2026-11-28", end="2026-11-29"
    )

    assert calls["fetch_daily"] == 0
    assert getattr(result, "attrs", {}).get("no_new_data") is True


def test_calendar_misconfiguration_proceeds_to_fetch(monkeypatch):
    # has_expected_session raising is logged as a warning; extraction still calls provider.
    _set_now(monkeypatch, _utc(2026, 11, 30, 22, 0))
    calls = _patch_run(monkeypatch, next_start="2026-11-28")

    def boom(*args, **kwargs):
        raise RuntimeError("calendar broken")

    monkeypatch.setattr(market_calendar_module, "has_expected_session", boom)

    result = run_extraction(symbols=["AAPL"], incremental=True)

    assert calls["fetch_daily"] == 1
    assert not result.empty


def test_incremental_lookup_none_bypasses_gate_full_history(monkeypatch):
    # next_start_date returns None (first run / lookup failure) -> start None -> gate bypassed.
    _set_now(monkeypatch, _utc(2026, 11, 30, 22, 0))
    calls = _patch_run(monkeypatch, next_start=None)

    result = run_extraction(symbols=["AAPL"], incremental=True)

    assert calls["fetch_daily"] == 1
    assert not result.empty


def test_error_symbol_preserved_when_gate_allows_fetch_then_provider_fails(monkeypatch):
    # Gate lets the symbol through, but the provider raises -> ExtractionError preserved.
    _set_now(monkeypatch, _utc(2026, 11, 30, 22, 0))
    calls = _patch_run(monkeypatch, next_start="2026-11-28")

    def failing_fetch(symbol, start=None, end=None, source=None):
        calls["fetch_daily"] += 1
        raise extractor.APIClientError("provider down")

    monkeypatch.setattr(extractor, "fetch_daily", failing_fetch)

    with pytest.raises(extractor.ExtractionError) as exc_info:
        run_extraction(symbols=["AAPL"], incremental=True)

    err = exc_info.value
    assert err.errors == {"AAPL": "provider down"}
    assert err.symbols_ok == []
    assert calls["fetch_daily"] == 1
