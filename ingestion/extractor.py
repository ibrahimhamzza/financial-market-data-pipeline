"""Loop configured symbols and collect a single OHLCV DataFrame."""

from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd

from config.settings import SYMBOLS
from ingestion.api_client import APIClientError, fetch_daily
from ingestion.incremental import resolve_start_dates
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

    # One watermark query for all symbols. Up-to-date symbols are absent from the plan.
    plan: dict[str, Optional[str]] = {}
    use_plan = incremental and start is None
    if use_plan:
        try:
            plan = resolve_start_dates(tickers)
        except Exception as exc:  # noqa: BLE001 — DB unreachable: fall back to full fetch
            logger.warning("Incremental lookup failed (%s); fetching full history", exc)
            plan = {t: None for t in tickers}

    for symbol in tickers:
        if use_plan and symbol not in plan:
            logger.info("Skipping %s: already up to date", symbol)
            continue
        symbol_start = plan.get(symbol) if use_plan else start
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
