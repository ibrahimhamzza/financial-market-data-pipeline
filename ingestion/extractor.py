"""Loop configured symbols and collect a single OHLCV DataFrame."""

from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd

from config.settings import SYMBOLS
from ingestion.api_client import APIClientError, fetch_daily
from ingestion.incremental import next_start_date
from utils.logger import get_logger

logger = get_logger("extractor")


def run_extraction(
    symbols: Optional[Iterable[str]] = None,
    incremental: bool = True,
    start: Optional[str] = None,
    end: Optional[str] = None,
    source: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch all symbols. Failed symbols are logged and skipped."""
    tickers = [s.upper() for s in (symbols or SYMBOLS)]
    frames: list[pd.DataFrame] = []
    errors: dict[str, str] = {}

    for symbol in tickers:
        symbol_start = start
        if incremental and symbol_start is None:
            try:
                symbol_start = next_start_date(symbol)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Incremental lookup failed for %s: %s", symbol, exc)
                symbol_start = None
        try:
            frame = fetch_daily(symbol, start=symbol_start, end=end, source=source)
            if frame.empty:
                logger.info("No new rows for %s", symbol)
                continue
            frames.append(frame)
            logger.info("Extracted %s rows for %s", len(frame), symbol)
        except APIClientError as exc:
            errors[symbol] = str(exc)
            logger.error("Extraction failed for %s: %s", symbol, exc)

    if not frames:
        logger.warning("Extraction produced no rows. errors=%s", errors)
        return pd.DataFrame(
            columns=[
                "symbol",
                "trade_date",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
            ]
        )

    combined = pd.concat(frames, ignore_index=True)
    combined.attrs["errors"] = errors
    combined.attrs["symbols_ok"] = sorted(combined["symbol"].unique().tolist())
    return combined
