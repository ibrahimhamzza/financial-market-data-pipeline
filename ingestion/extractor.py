"""Loop configured symbols and collect a single OHLCV DataFrame."""

from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd

from config.settings import SYMBOLS
from ingestion.api_client import APIClientError, fetch_daily
from ingestion.incremental import next_start_date
from ingestion.market_calendar import has_expected_session
from utils.logger import get_logger

logger = get_logger("extractor")


class ExtractionError(Exception):
    """Raised when extraction fails for all symbols or has partial failures.

    Attributes:
        errors: Mapping of symbol -> error message for failed symbols.
        symbols_ok: List of symbols that were successfully extracted.
    """

    def __init__(self, message: str, errors: Optional[dict[str, str]] = None, symbols_ok: Optional[list[str]] = None):
        super().__init__(message)
        self.errors = errors or {}
        self.symbols_ok = symbols_ok or []


def run_extraction(
    symbols: Optional[Iterable[str]] = None,
    incremental: bool = True,
    start: Optional[str] = None,
    end: Optional[str] = None,
    source: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch all symbols.

    Policy (P0 fix):
    - If all symbols fail OR extraction returns no usable rows when errors exist
      (or when there are no successful extractions), raise ExtractionError.
    - If only some symbols fail, do NOT silently report full success: raise
      ExtractionError with failed symbols recorded. This ensures pipeline
      status becomes failed per the existing pipeline status model.
    - Error details are preserved in the exception (and also attached to attrs
      when a DataFrame is returned, though partial/all-failure paths raise).
    """
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
        # Calendar gate: skip the provider call when the requested window
        # contains no expected XNYS session (weekend, scheduled holiday,
        # same-day rerun, or a session not yet complete after the availability
        # buffer). A None start means full history and is not gated.
        if symbol_start is not None:
            try:
                if not has_expected_session(start=symbol_start, end=end):
                    logger.info(
                        "No expected XNYS session for %s from %s%s; skipping provider call",
                        symbol,
                        symbol_start,
                        f" to {end}" if end else "",
                    )
                    continue
            except Exception as exc:  # noqa: BLE001 — calendar misconfiguration is not a symbol failure
                logger.warning("Calendar check failed for %s: %s; proceeding to fetch", symbol, exc)
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
        except Exception as exc:  # noqa: BLE001
            errors[symbol] = str(exc)
            logger.error("Extraction failed for %s: %s", symbol, exc)

    symbols_ok = sorted({s for f in frames for s in f.get("symbol", pd.Series(dtype=str)).unique().tolist()}) if frames else []

    if not frames:
        logger.warning("Extraction produced no rows. errors=%s", errors)
        if errors:
            failed = ", ".join(sorted(errors.keys()))
            raise ExtractionError(
                f"Extraction failed for all symbols or returned no usable rows. Failed: {failed}. Errors: {errors}",
                errors=errors,
                symbols_ok=symbols_ok,
            )
        # No errors but also no frames (e.g., incremental up to date for all symbols)
        # Legitimate no_new_data outcome - every requested symbol was successfully checked.
        empty = pd.DataFrame(
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
        empty.attrs["errors"] = {}
        empty.attrs["symbols_ok"] = []
        empty.attrs["no_new_data"] = True
        empty.attrs["symbols_checked"] = tickers
        return empty

    if errors:
        failed = ", ".join(sorted(errors.keys()))
        combined = pd.concat(frames, ignore_index=True)
        combined.attrs["errors"] = errors
        combined.attrs["symbols_ok"] = symbols_ok
        combined.attrs["symbols_checked"] = tickers
        raise ExtractionError(
            f"Partial extraction failure: {len(symbols_ok)} symbol(s) succeeded, {len(errors)} failed ({failed}). Errors: {errors}",
            errors=errors,
            symbols_ok=symbols_ok,
        )

    combined = pd.concat(frames, ignore_index=True)
    combined.attrs["errors"] = errors
    combined.attrs["symbols_ok"] = symbols_ok
    combined.attrs["symbols_checked"] = tickers
    return combined
