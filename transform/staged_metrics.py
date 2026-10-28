"""Compute metrics for a staging batch using full in-memory history.

``cumulative_return`` and ``max_drawdown`` compound from the first row of the
group passed to ``calculate_metrics``. A 50-day lookback would change those
values, so callers must supply all known raw rows for each staged symbol.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from transform.calculator import calculate_metrics
from transform.cleaner import clean_data
from utils.logger import get_logger

logger = get_logger("staged_metrics")

OHLCV_COLUMNS = [
    "symbol",
    "trade_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
]


def calendar_date(series: pd.Series) -> pd.Series:
    """UTC-normalized calendar date (matches loader ``.dt.date`` intent)."""
    return pd.to_datetime(series, utc=True).dt.tz_localize(None).dt.normalize()


def staging_keys(staging: pd.DataFrame) -> pd.DataFrame:
    """Unique ``(symbol, trade_date)`` keys from the extract batch."""
    if staging is None or staging.empty:
        return pd.DataFrame(columns=["symbol", "trade_date"])
    keys = staging.loc[:, ["symbol", "trade_date"]].copy()
    keys["symbol"] = keys["symbol"].astype(str).str.upper().str.strip()
    keys["trade_date"] = calendar_date(keys["trade_date"])
    return keys.drop_duplicates(subset=["symbol", "trade_date"], keep="last")


def combine_history_and_staging(
    history: Optional[pd.DataFrame],
    staging: pd.DataFrame,
) -> pd.DataFrame:
    """History first, staging last; last row wins for the same business key."""
    frames: list[pd.DataFrame] = []
    if history is not None and not history.empty:
        frames.append(history.copy())
    if staging is not None and not staging.empty:
        frames.append(staging.copy())
    if not frames:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    present = [col for col in OHLCV_COLUMNS if col in combined.columns]
    combined = combined.loc[:, present].copy()
    combined["symbol"] = combined["symbol"].astype(str).str.upper().str.strip()
    combined["trade_date"] = calendar_date(combined["trade_date"])
    combined = combined.drop_duplicates(subset=["symbol", "trade_date"], keep="last")
    combined = combined.sort_values(["symbol", "trade_date"], kind="mergesort")
    combined.reset_index(drop=True, inplace=True)
    return combined


def compute_metrics_for_staging(
    staging: pd.DataFrame,
    history: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Return metric rows only for staging keys, computed on full combined history."""
    if staging is None or staging.empty:
        logger.info("compute_metrics_for_staging: empty staging")
        return calculate_metrics(clean_data(staging))

    combined = combine_history_and_staging(history, staging)
    cleaned = clean_data(combined)
    metrics = calculate_metrics(cleaned)
    if metrics.empty:
        return metrics

    keys = staging_keys(staging)
    keyed = metrics.copy()
    keyed["_trade_date_key"] = calendar_date(keyed["trade_date"])
    keys = keys.rename(columns={"trade_date": "_trade_date_key"})
    filtered = keyed.merge(keys, on=["symbol", "_trade_date_key"], how="inner")
    filtered = filtered.drop(columns=["_trade_date_key"])
    filtered = filtered.sort_values(["symbol", "trade_date"], kind="mergesort")
    filtered.reset_index(drop=True, inplace=True)
    logger.info(
        "compute_metrics_for_staging: history=%s staging=%s combined=%s output=%s",
        0 if history is None or history.empty else len(history),
        len(staging),
        len(combined),
        len(filtered),
    )
    return filtered
