from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from ingestion import extractor
from ingestion.api_client import APIClientError
from ingestion.extractor import ExtractionError, run_extraction
import pipeline


def _make_frame(symbol: str):
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "trade_date": pd.Timestamp("2024-01-02"),
                "open_price": 1.0,
                "high_price": 2.0,
                "low_price": 0.5,
                "close_price": 1.5,
                "volume": 10,
            }
        ]
    )


def test_run_extraction_all_symbols_fail_raises_extraction_error(monkeypatch):
    def fake_fetch_daily(symbol, start=None, end=None, source=None):
        raise APIClientError(f"fail {symbol}")

    monkeypatch.setattr(extractor, "fetch_daily", fake_fetch_daily)
    monkeypatch.setattr(extractor, "next_start_date", lambda s: None)

    with pytest.raises(ExtractionError) as exc_info:
        run_extraction(symbols=["AAPL", "MSFT"], incremental=False)

    err = exc_info.value
    assert set(err.errors.keys()) == {"AAPL", "MSFT"}
    assert err.symbols_ok == []


def test_run_extraction_partial_failure_raises_extraction_error(monkeypatch):
    def fake_fetch_daily(symbol, start=None, end=None, source=None):
        if symbol == "AAPL":
            return _make_frame("AAPL")
        raise APIClientError("boom")

    monkeypatch.setattr(extractor, "fetch_daily", fake_fetch_daily)
    monkeypatch.setattr(extractor, "next_start_date", lambda s: None)

    with pytest.raises(ExtractionError) as exc_info:
        run_extraction(symbols=["AAPL", "MSFT"], incremental=False)

    err = exc_info.value
    assert err.errors == {"MSFT": "boom"}
    assert err.symbols_ok == ["AAPL"]


def test_run_extraction_empty_no_errors_returns_no_new_data(monkeypatch):
    def fake_fetch_daily(symbol, start=None, end=None, source=None):
        return pd.DataFrame()

    monkeypatch.setattr(extractor, "fetch_daily", fake_fetch_daily)
    monkeypatch.setattr(extractor, "next_start_date", lambda s: None)

    result = run_extraction(symbols=["AAPL"], incremental=False)
    assert result.empty
    assert getattr(result, "attrs", {}).get("no_new_data") is True


def test_pipeline_records_failed_run_on_extraction_error(monkeypatch, tmp_path):
    # Redirect paths
    monkeypatch.setattr(pipeline, "RAW_DATA_DIR", tmp_path)
    monkeypatch.setattr(pipeline, "STAGING_PATH", tmp_path / "_staging.csv")
    monkeypatch.setattr(pipeline, "METRICS_PATH", tmp_path / "_metrics.csv")

    calls = {}

    def fake_start_pipeline_run():
        calls["start"] = True
        return 123

    def fake_finish_pipeline_run(run_id, status, symbols_processed, rows_inserted, duration_seconds, error_message=None):
        calls["finish"] = {
            "run_id": run_id,
            "status": status,
            "symbols_processed": symbols_processed,
            "rows_inserted": rows_inserted,
            "duration_seconds": duration_seconds,
            "error_message": error_message,
        }

    def fake_run_extraction(incremental=True):
        raise ExtractionError("all failed", errors={"AAPL": "boom"}, symbols_ok=[])

    monkeypatch.setattr(pipeline, "start_pipeline_run", fake_start_pipeline_run)
    monkeypatch.setattr(pipeline, "finish_pipeline_run", fake_finish_pipeline_run)
    monkeypatch.setattr(pipeline, "run_extraction", fake_run_extraction)
    # Ensure no downstream calls happen
    monkeypatch.setattr(pipeline, "fetch_raw_for_symbols", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(pipeline, "load_raw", lambda *a, **k: 0)
    monkeypatch.setattr(pipeline, "load_transformed", lambda *a, **k: 0)
    monkeypatch.setattr(pipeline, "run_quality_checks", lambda *a, **k: {})

    with pytest.raises(ExtractionError):
        pipeline.run_pipeline(record_run=True)

    assert calls.get("start") is True
    assert calls.get("finish") is not None
    assert calls["finish"]["run_id"] == 123
    assert calls["finish"]["status"] == "failed"
    assert calls["finish"]["symbols_processed"] == 0
    assert calls["finish"]["rows_inserted"] == 0
    assert calls["finish"]["error_message"] is not None
    # Ensure staging/metrics not written in success path
    assert not (tmp_path / "_staging.csv").exists()
    assert not (tmp_path / "_metrics.csv").exists()


def test_pipeline_handles_no_new_data_without_loading_stale_staging(monkeypatch, tmp_path):
    # Redirect paths
    monkeypatch.setattr(pipeline, "RAW_DATA_DIR", tmp_path)
    monkeypatch.setattr(pipeline, "STAGING_PATH", tmp_path / "_staging.csv")
    monkeypatch.setattr(pipeline, "METRICS_PATH", tmp_path / "_metrics.csv")

    # Create stale files to ensure they are removed
    (tmp_path / "_staging.csv").write_text("stale")
    (tmp_path / "_metrics.csv").write_text("stale")

    calls = {"load_raw": 0, "load_transformed": 0, "quality": 0}

    def fake_start_pipeline_run():
        return 456

    def fake_finish_pipeline_run(*a, **k):
        calls["finish"] = k

    def fake_run_extraction(incremental=True):
        empty = pd.DataFrame(
            columns=["symbol", "trade_date", "open_price", "high_price", "low_price", "close_price", "volume"]
        )
        empty.attrs["no_new_data"] = True
        empty.attrs["errors"] = {}
        empty.attrs["symbols_ok"] = []
        return empty

    monkeypatch.setattr(pipeline, "start_pipeline_run", fake_start_pipeline_run)
    monkeypatch.setattr(pipeline, "finish_pipeline_run", fake_finish_pipeline_run)
    monkeypatch.setattr(pipeline, "run_extraction", fake_run_extraction)
    monkeypatch.setattr(pipeline, "fetch_raw_for_symbols", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(pipeline, "load_raw", lambda *a, **k: calls.__setitem__("load_raw", calls["load_raw"] + 1) or 0)
    monkeypatch.setattr(pipeline, "load_transformed", lambda *a, **k: calls.__setitem__("load_transformed", calls["load_transformed"] + 1) or 0)
    monkeypatch.setattr(pipeline, "run_quality_checks", lambda *a, **k: calls.__setitem__("quality", calls["quality"] + 1) or {})

    result = pipeline.run_pipeline(record_run=True)

    # Should not have loaded anything
    assert calls["load_raw"] == 0
    assert calls["load_transformed"] == 0
    assert calls["quality"] == 0
    # Stale files removed
    assert not (tmp_path / "_staging.csv").exists()
    assert not (tmp_path / "_metrics.csv").exists()
    # Pipeline should be marked success with no_new_data
    assert calls.get("finish") is not None
    assert calls["finish"].get("status") == "success"
    assert result.get("status") == "success"
    assert result.get("no_new_data") is True


def test_calendar_gate_skips_provider_on_weekend(monkeypatch):
    # now = Fri 2026-11-27 19:00 UTC -> last completed XNYS session = Fri 11-27.
    # next_start_date returns Sat 11-28 -> window [11-28, 11-27] reversed -> gate out.
    import ingestion.market_calendar as market_calendar_module

    class _FixedDateTime:
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 27, 19, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(market_calendar_module, "datetime", _FixedDateTime)

    calls = {"fetch_daily": 0}

    def fake_fetch_daily(symbol, start=None, end=None, source=None):
        calls["fetch_daily"] += 1
        raise AssertionError("provider must not be called on a gated-out weekend")

    monkeypatch.setattr(extractor, "fetch_daily", fake_fetch_daily)
    monkeypatch.setattr(extractor, "next_start_date", lambda s: "2026-11-28")

    result = run_extraction(symbols=["AAPL"], incremental=True)

    assert calls["fetch_daily"] == 0
    assert getattr(result, "attrs", {}).get("no_new_data") is True
    assert result.attrs["symbols_checked"] == ["AAPL"]
    assert result.attrs["symbols_ok"] == []
    assert result.attrs["errors"] == {}
