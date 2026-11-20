
"""Null handling, deduplication, and validation for OHLCV frames."""

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

REQUIRED_COLUMNS = [
    "symbol",
    "trade_date",
    *PRICE_COLS,
    "volume",
]


def clean_data(raw: pd.DataFrame) -> pd.DataFrame:
    """Return validated, typed, and deduplicated OHLCV data."""

    if raw is None or raw.empty:
        logger.info("clean_data received an empty frame")
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    df = raw.copy()

    # Standardize column names.
    df.rename(
        columns={
            key: value
            for key, value in COLUMN_MAP.items()
            if key in df.columns
        },
        inplace=True,
    )

    # Validate required columns.
    missing = [
        col for col in REQUIRED_COLUMNS
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Cannot clean data; missing columns: {missing}"
        )

    before = len(df)

    # Normalize symbols.
    df["symbol"] = (
        df["symbol"]
        .astype("string")
        .str.upper()
        .str.strip()
    )

    # Parse dates and normalize to calendar dates.
    df["trade_date"] = (
        pd.to_datetime(
            df["trade_date"],
            errors="coerce",
            utc=True,
        )
        .dt.tz_localize(None)
        .dt.normalize()
    )

    # Convert financial columns to numeric values.
    for col in PRICE_COLS + ["volume"]:
        df[col] = pd.to_numeric(
            df[col],
            errors="coerce",
        )

    # Remove invalid symbols and missing essential values.
    valid_symbol = (
        df["symbol"].notna()
        & df["symbol"].ne("")
    )

    valid_required = (
        df["trade_date"].notna()
        & df[PRICE_COLS].notna().all(axis=1)
        & df["volume"].notna()
    )

    invalid_required = ~(valid_symbol & valid_required)

    if invalid_required.any():
        logger.warning(
            "Dropping %s rows with missing or invalid required values",
            int(invalid_required.sum()),
        )

    df = df.loc[valid_symbol & valid_required].copy()

    if df.empty:
        logger.warning("No valid rows remain after required-value validation")
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    # Reject non-positive prices.
    positive_prices = (df[PRICE_COLS] > 0).all(axis=1)

    # Reject negative or fractional volume.
    valid_volume = (
        (df["volume"] >= 0)
        & (df["volume"] % 1 == 0)
    )

    # Validate OHLC price relationships.
    valid_high_low = df["high_price"] >= df["low_price"]

    valid_open = (
        (df["open_price"] >= df["low_price"])
        & (df["open_price"] <= df["high_price"])
    )

    valid_close = (
        (df["close_price"] >= df["low_price"])
        & (df["close_price"] <= df["high_price"])
    )

    valid_rows = (
        positive_prices
        & valid_volume
        & valid_high_low
        & valid_open
        & valid_close
    )

    invalid_count = int((~valid_rows).sum())

    if invalid_count:
        logger.warning(
            "Dropping %s rows with invalid prices, OHLC relationships, "
            "or volume",
            invalid_count,
        )

    df = df.loc[valid_rows].copy()

    if df.empty:
        logger.warning("No valid rows remain after OHLCV validation")
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    # Stable sorting ensures staging rows (appended later) win duplicates.
    df.sort_values(
        ["symbol", "trade_date"],
        kind="mergesort",
        inplace=True,
    )

    duplicate_count = int(
        df.duplicated(
            subset=["symbol", "trade_date"],
            keep="last",
        ).sum()
    )

    if duplicate_count:
        logger.info(
            "Removing %s duplicate symbol/date rows",
            duplicate_count,
        )

    df.drop_duplicates(
        subset=["symbol", "trade_date"],
        keep="last",
        inplace=True,
    )

    # Convert volume to PostgreSQL-compatible integer values.
    df["volume"] = df["volume"].astype("int64")

    df.reset_index(drop=True, inplace=True)

    logger.info(
        "clean_data: %s input rows → %s valid rows",
        before,
        len(df),
    )

    return df[REQUIRED_COLUMNS]
