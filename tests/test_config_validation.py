"""Focused configuration validation tests (no real database or network access)."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from config import settings
from database import db_connector

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run_python(code: str, **env_overrides: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.update({name: str(value) for name, value in env_overrides.items()})
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )


def _set_valid_database(monkeypatch, **overrides) -> None:
    values = {
        "DB_HOST": "localhost",
        "DB_PORT": 5432,
        "DB_NAME": "financial_db",
        "DB_USER": "postgres",
        "DB_PASSWORD": "unit-test-password",
    }
    values.update(overrides)
    for name, value in values.items():
        monkeypatch.setattr(settings, name, value)


@pytest.fixture(autouse=True)
def _clear_engine_cache():
    db_connector.get_engine.cache_clear()
    yield
    db_connector.get_engine.cache_clear()


def test_valid_yfinance_config_without_alpha_vantage_key():
    result = _run_python(
        """
        import config.settings as settings
        print(settings.DATA_SOURCE)
        print(repr(settings.ALPHA_VANTAGE_API_KEY))
        """,
        DATA_SOURCE=" yfinance ",
        ALPHA_VANTAGE_API_KEY="",
        DB_PASSWORD="",
    )

    assert result.returncode == 0, result.stderr
    source, api_key = result.stdout.splitlines()
    assert source == "yfinance"
    assert api_key == "''"


def test_settings_import_and_extraction_without_database_credentials():
    result = _run_python(
        """
        import pandas as pd

        import config.settings as settings
        from ingestion import extractor

        assert settings.DB_PASSWORD == ""

        def fake_fetch_daily(symbol, start=None, end=None, source=None):
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

        extractor.fetch_daily = fake_fetch_daily
        frame = extractor.run_extraction(
            symbols=["AAPL"], incremental=False, source="yfinance"
        )
        assert frame["symbol"].tolist() == ["AAPL"]
        print("extraction-ok")
        """,
        DATA_SOURCE="yfinance",
        ALPHA_VANTAGE_API_KEY="",
        DB_PASSWORD="",
    )

    assert result.returncode == 0, result.stderr
    assert "extraction-ok" in result.stdout


def test_get_engine_rejects_empty_db_password(monkeypatch):
    _set_valid_database(monkeypatch, DB_PASSWORD="   ")

    with pytest.raises(ValueError, match="DB_PASSWORD"):
        db_connector.get_engine()


def test_get_engine_rejects_placeholder_db_password(monkeypatch):
    _set_valid_database(monkeypatch, DB_PASSWORD="your_password_here")

    with pytest.raises(ValueError, match="DB_PASSWORD") as exc_info:
        db_connector.get_engine()

    assert "your_password_here" not in str(exc_info.value)


def test_valid_database_config_passes_validation(monkeypatch):
    _set_valid_database(monkeypatch)

    settings.require_database_config()


def test_unsupported_data_source_is_rejected():
    with pytest.raises(ValueError, match="DATA_SOURCE") as exc_info:
        settings.validate_data_source("not-a-real-source")

    message = str(exc_info.value)
    assert "yfinance" in message
    assert "alphavantage" in message


def test_unsupported_data_source_is_rejected_at_import():
    result = _run_python("import config.settings", DATA_SOURCE="not-a-real-source")

    assert result.returncode != 0
    assert "DATA_SOURCE" in result.stderr
    assert "yfinance" in result.stderr
    assert "alphavantage" in result.stderr


@pytest.mark.parametrize("api_key", ["", "   ", "your_api_key_here"])
def test_alpha_vantage_rejects_missing_or_placeholder_key(monkeypatch, api_key):
    monkeypatch.setattr(settings, "ALPHA_VANTAGE_API_KEY", api_key)

    with pytest.raises(RuntimeError, match="ALPHA_VANTAGE_API_KEY") as exc_info:
        settings.require_alpha_vantage_key()

    if api_key.strip():
        assert api_key.strip() not in str(exc_info.value)


def test_alpha_vantage_accepts_real_key_without_exposing_it(monkeypatch, caplog):
    api_key = "real-looking-key-12345"
    monkeypatch.setattr(settings, "ALPHA_VANTAGE_API_KEY", api_key)

    assert settings.require_alpha_vantage_key() == api_key
    assert api_key not in caplog.text


@pytest.mark.parametrize("port", [0, -1, 65536, "not-a-port"])
def test_invalid_db_port_is_rejected(monkeypatch, port):
    _set_valid_database(monkeypatch, DB_PORT=port)

    with pytest.raises(ValueError, match="DB_PORT"):
        settings.require_database_config()


@pytest.mark.parametrize("timeout", ["0", "-1"])
def test_zero_or_negative_http_timeout_is_rejected(timeout):
    result = _run_python("import config.settings", HTTP_TIMEOUT_SECONDS=timeout)

    assert result.returncode != 0
    assert "HTTP_TIMEOUT_SECONDS" in result.stderr


def test_negative_max_retries_is_rejected():
    result = _run_python("import config.settings", MAX_RETRIES="-1")

    assert result.returncode != 0
    assert "MAX_RETRIES" in result.stderr


def test_negative_retry_base_delay_is_rejected():
    result = _run_python("import config.settings", RETRY_BASE_DELAY_SECONDS="-0.1")

    assert result.returncode != 0
    assert "RETRY_BASE_DELAY_SECONDS" in result.stderr
