"""Null handling, deduplication, and type casting for OHLCV frames."""

from __future__ import annotations

import pandas as pd

from utils.logger import get_logger

logger = get_logger("cleaner")

COLUMN_MAP = {
    "open": "open_price",
    "high": "high_price",
    "low": "low_price",
    "close": "close_price",
    "Open": "open_price",
    "High": "high_price",
    "Low": "low_price",
    "Close": "close_price",
    "Volume": "volume",
    "Date": "trade_date",
    "date": "trade_date",
}

PRICE_COLS = ["open_price", "high_price", "low_price", "close_price"]


def clean_data(raw: pd.DataFrame) -> pd.DataFrame:
    """Return a typed, de-duplicated OHLCV DataFrame."""
    if raw is None or raw.empty:
        logger.info("clean_data received an empty frame")
        return pd.DataFrame(
            columns=["symbol", "trade_date", *PRICE_COLS, "volume"]
        )

    df = raw.copy()
    df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns}, inplace=True)

    required = ["symbol", "trade_date", *PRICE_COLS, "volume"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Cannot clean data; missing columns: {missing}")

    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["trade_date"] = pd.to_datetime(df["trade_date"], utc=True).dt.tz_localize(None)

    for col in PRICE_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    before = len(df)
    df.sort_values(["symbol", "trade_date"], inplace=True)
    # Prices: forward-fill within each symbol; do not drop yet.
    df[PRICE_COLS] = df.groupby("symbol", group_keys=False)[PRICE_COLS].ffill()
    df = df.drop_duplicates(subset=["symbol", "trade_date"], keep="last")

    # Drop rows that still have no close (cannot compute returns).
    df = df.dropna(subset=["close_price", "symbol", "trade_date"])
    df["volume"] = df["volume"].fillna(0).clip(lower=0).round().astype("int64")

    invalid_hl = df["high_price"] < df["low_price"]
    if invalid_hl.any():
        logger.warning("Dropping %s rows where high < low", int(invalid_hl.sum()))
        df = df.loc[~invalid_hl]

    df.reset_index(drop=True, inplace=True)
    logger.info("clean_data: %s → %s rows", before, len(df))
    return df[required]
