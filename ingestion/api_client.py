"""Fetch daily OHLCV bars from yfinance or Alpha Vantage."""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests

from config.settings import (
    ALPHA_VANTAGE_OUTPUT_SIZE,
    DATA_SOURCE,
    HTTP_TIMEOUT_SECONDS,
    MAX_RETRIES,
    RAW_DATA_DIR,
    RETRY_BASE_DELAY_SECONDS,
    require_alpha_vantage_key,
)
from utils.logger import get_logger

logger = get_logger("api_client")

REQUIRED_OHLCV = ("open_price", "high_price", "low_price", "close_price", "volume")
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"


class APIClientError(Exception):
    """Raised when an API call cannot be completed."""


class PayloadValidationError(APIClientError):
    """Raised when the HTTP body is present but not usable OHLCV data."""


def validate_http_response(response: requests.Response) -> dict[str, Any]:
    """Check status code and non-empty JSON body."""
    if response.status_code != 200:
        raise APIClientError(f"HTTP {response.status_code}: {response.text[:300]}")
    if not response.content:
        raise APIClientError("Empty HTTP body")
    try:
        payload = response.json()
    except ValueError as exc:
        raise PayloadValidationError("Response is not JSON") from exc
    if not payload:
        raise PayloadValidationError("JSON payload is empty")
    return payload


def _standardize_frame(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["symbol"] = symbol.upper()
    out["trade_date"] = pd.to_datetime(out["trade_date"], utc=True).dt.tz_localize(None)
    for col in REQUIRED_OHLCV:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["volume"] = out["volume"].round().astype("Int64")
    out.sort_values("trade_date", inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out[["symbol", "trade_date", *REQUIRED_OHLCV]]


def save_raw_json(symbol: str, payload: Any, raw_dir: Optional[Path] = None) -> Path:
    """Persist the vendor payload (data-lake landing file)."""
    directory = raw_dir or RAW_DATA_DIR
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = directory / f"{symbol.upper()}_{stamp}.json"
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    logger.info("Wrote raw JSON %s", path.name)
    return path


def _retry_call(fn, *, attempts: int = MAX_RETRIES):
    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except (requests.ConnectionError, requests.Timeout, APIClientError) as exc:
            last_error = exc
            if attempt == attempts:
                break
            delay = RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "Attempt %s/%s failed (%s). Retrying in %.1fs",
                attempt,
                attempts,
                exc,
                delay,
            )
            time.sleep(delay)
    raise APIClientError(f"Failed after {attempts} attempts: {last_error}") from last_error


def fetch_yfinance(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    import yfinance as yf

    kwargs: dict[str, Any] = {
        "tickers": symbol,
        "interval": "1d",
        "auto_adjust": False,
        "progress": False,
        "threads": False,
    }
    if start:
        kwargs["start"] = start
    if end:
        kwargs["end"] = end
    if not start and not end:
        kwargs["period"] = "1y"

    def _download():
        ticker = yf.Ticker(symbol)
        hist_kwargs: dict[str, Any] = {"interval": "1d", "auto_adjust": False}
        if start:
            hist_kwargs["start"] = start
        if end:
            hist_kwargs["end"] = end
        if not start and not end:
            hist_kwargs["period"] = "1y"
        frame = ticker.history(**hist_kwargs)
        if frame is None or frame.empty:
            frame = yf.download(**kwargs)
        if frame is None or frame.empty:
            raise APIClientError(f"yfinance returned no rows for {symbol}")
        return frame

    raw = _retry_call(_download)
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [col[0] for col in raw.columns]

    rename = {
        "Open": "open_price",
        "High": "high_price",
        "Low": "low_price",
        "Close": "close_price",
        "Volume": "volume",
    }
    missing = [col for col in rename if col not in raw.columns]
    if missing:
        raise PayloadValidationError(f"yfinance missing columns {missing} for {symbol}")

    frame = raw.reset_index().rename(columns={"Date": "trade_date", **rename})
    payload = json.loads(frame.to_json(orient="records", date_format="iso"))
    save_raw_json(symbol, {"source": "yfinance", "symbol": symbol, "bars": payload})
    return _standardize_frame(frame, symbol)


def parse_alpha_vantage_payload(payload: dict[str, Any], symbol: str) -> pd.DataFrame:
    for bad_key in ("Error Message", "Note", "Information"):
        if bad_key in payload:
            raise PayloadValidationError(f"Alpha Vantage {bad_key}: {payload[bad_key]}")

    series = payload.get("Time Series (Daily)")
    if not series:
        raise PayloadValidationError("Missing 'Time Series (Daily)' in Alpha Vantage payload")

    rows = []
    for day, values in series.items():
        if not isinstance(values, dict):
            raise PayloadValidationError(f"Bad bar payload for {day}")
        required = {
            "1. open": "open_price",
            "2. high": "high_price",
            "3. low": "low_price",
            "4. close": "close_price",
            "5. volume": "volume",
        }
        missing = [key for key in required if key not in values]
        if missing:
            raise PayloadValidationError(f"Bar {day} missing {missing}")
        rows.append(
            {
                "trade_date": day,
                "open_price": values["1. open"],
                "high_price": values["2. high"],
                "low_price": values["3. low"],
                "close_price": values["4. close"],
                "volume": values["5. volume"],
            }
        )
    return _standardize_frame(pd.DataFrame(rows), symbol)


def fetch_alpha_vantage(symbol: str) -> pd.DataFrame:
    api_key = require_alpha_vantage_key()
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": ALPHA_VANTAGE_OUTPUT_SIZE,
        "apikey": api_key,
        "datatype": "json",
    }

    def _get():
        response = requests.get(
            ALPHA_VANTAGE_URL,
            params=params,
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        payload = validate_http_response(response)
        save_raw_json(symbol, payload)
        return parse_alpha_vantage_payload(payload, symbol)

    return _retry_call(_get)


def fetch_daily(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    source: Optional[str] = None,
) -> pd.DataFrame:
    """Return a standardized OHLCV DataFrame for one symbol."""
    provider = (source or DATA_SOURCE).lower()
    logger.info("Fetching %s from %s (start=%s end=%s)", symbol, provider, start, end)
    if provider == "alphavantage":
        frame = fetch_alpha_vantage(symbol)
        if start:
            frame = frame[frame["trade_date"] >= pd.Timestamp(start)]
        if end:
            frame = frame[frame["trade_date"] <= pd.Timestamp(end)]
        return frame.reset_index(drop=True)
    if provider == "yfinance":
        return fetch_yfinance(symbol, start=start, end=end)
    raise APIClientError(f"Unknown DATA_SOURCE={provider}")


def iso_utc(value: datetime | date | pd.Timestamp) -> str:
    """Standardize timestamps to UTC ISO-8601 (date-only values stay calendar dates)."""
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat()
