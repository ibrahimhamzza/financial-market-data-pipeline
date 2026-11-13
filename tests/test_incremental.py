"""Offline tests for ingestion.incremental watermark behavior.

The database layer is mocked; no live PostgreSQL connection is made.
"""

from __future__ import annotations

from datetime import date

import ingestion.incremental as incremental


class _FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _FakeConnection:
    def __init__(self, value):
        self._value = value

    def execute(self, sql, params=None):
        return _FakeResult(self._value)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _FakeEngine:
    def __init__(self, value):
        self._value = value

    def connect(self):
        return _FakeConnection(self._value)


def _captured_params(monkeypatch, value):
    captured = {}

    class _TrackingConnection(_FakeConnection):
        def execute(self, sql, params=None):
            captured["params"] = params
            return super().execute(sql, params)

    class _TrackingEngine(_FakeEngine):
        def connect(self):
            return _TrackingConnection(self._value)

    monkeypatch.setattr(incremental, "get_engine", lambda: _TrackingEngine(value))
    return captured


def test_next_start_date_returns_day_after_watermark(monkeypatch):
    monkeypatch.setattr(
        incremental, "get_engine", lambda: _FakeEngine(date(2026, 11, 30))
    )
    assert incremental.next_start_date("AAPL") == "2026-12-01"


def test_next_start_date_returns_none_when_no_watermark(monkeypatch):
    # First run: no rows in raw_market_data -> full history (None start).
    monkeypatch.setattr(incremental, "get_engine", lambda: _FakeEngine(None))
    assert incremental.next_start_date("AAPL") is None


def test_get_last_loaded_date_returns_date(monkeypatch):
    monkeypatch.setattr(
        incremental, "get_engine", lambda: _FakeEngine(date(2026, 11, 27))
    )
    assert incremental.get_last_loaded_date("AAPL") == date(2026, 11, 27)


def test_get_last_loaded_date_symbol_uppercased_in_params(monkeypatch):
    captured = _captured_params(monkeypatch, date(2026, 11, 27))
    incremental.get_last_loaded_date("aapl")
    assert captured["params"] == {"symbol": "AAPL"}


def test_get_last_loaded_date_none_on_connection_error(monkeypatch):
    # A failed watermark lookup is logged and falls back to full history.
    class _BadEngine:
        def connect(self):
            raise RuntimeError("db down")

    monkeypatch.setattr(incremental, "get_engine", lambda: _BadEngine())
    assert incremental.get_last_loaded_date("AAPL") is None


def test_next_start_date_none_on_connection_error(monkeypatch):
    class _BadEngine:
        def connect(self):
            raise RuntimeError("db down")

    monkeypatch.setattr(incremental, "get_engine", lambda: _BadEngine())
    assert incremental.next_start_date("AAPL") is None
