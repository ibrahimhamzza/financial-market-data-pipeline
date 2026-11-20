
"""Compute metrics for a staging batch using full in-memory history."""

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
    """Normalize timestamps to UTC calendar dates."""
    return (
        pd.to_datetime(series, utc=True)
        .dt.tz_localize(None)
        .dt.normalize()
    )


def staging_keys(staging: pd.DataFrame) -> pd.DataFrame:
    """Return unique symbol/date keys from the staging batch."""
    if staging is None or staging.empty:
        return pd.DataFrame(columns=["symbol", "trade_date"])

    keys = staging.loc[:, ["symbol", "trade_date"]].copy()
    keys["symbol"] = keys["symbol"].astype(str).str.upper().str.strip()
    keys["trade_date"] = calendar_date(keys["trade_date"])

    return keys.drop_duplicates(
        subset=["symbol", "trade_date"],
        keep="last",
    )


def combine_history_and_staging(
    history: Optional[pd.DataFrame],
    staging: pd.DataFrame,
) -> pd.DataFrame:
    """Combine history and staging; staging wins for duplicate keys."""
    frames = []

    if history is not None and not history.empty:
        frames.append(history.copy())

    if staging is not None and not staging.empty:
        frames.append(staging.copy())

    if not frames:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)

    missing = [col for col in OHLCV_COLUMNS if col not in combined.columns]
    if missing:
        raise ValueError(
            f"Cannot combine history and staging; missing columns: {missing}"
        )

    combined = combined.loc[:, OHLCV_COLUMNS].copy()
    combined["symbol"] = (
        combined["symbol"].astype(str).str.upper().str.strip()
    )
    combined["trade_date"] = calendar_date(combined["trade_date"])

    combined.sort_values(
        ["symbol", "trade_date"],
        kind="mergesort",
        inplace=True,
    )

    combined.drop_duplicates(
        subset=["symbol", "trade_date"],
        keep="last",
        inplace=True,
    )

    combined.reset_index(drop=True, inplace=True)
    return combined


def compute_metrics_for_staging(
    staging: pd.DataFrame,
    history: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Compute metrics on full history, returning only staging dates."""
    if staging is None or staging.empty:
        logger.info("compute_metrics_for_staging: empty staging")
        return calculate_metrics(clean_data(staging))

    keys = staging_keys(staging)
    combined = combine_history_and_staging(history, staging)

    cleaned = clean_data(combined)

    # Do not silently omit invalid staging rows.
    cleaned_keys = cleaned.loc[:, ["symbol", "trade_date"]].copy()
    cleaned_keys["trade_date"] = calendar_date(cleaned_keys["trade_date"])

    expected = set(
        zip(keys["symbol"], keys["trade_date"])
    )
    available = set(
        zip(cleaned_keys["symbol"], cleaned_keys["trade_date"])
    )

    missing_keys = expected - available
    if missing_keys:
        examples = list(missing_keys)[:5]
        raise ValueError(
            f"{len(missing_keys)} staging rows were removed during cleaning. "
            f"Example keys: {examples}"
        )

    metrics = calculate_metrics(cleaned)

    if metrics.empty:
        return metrics

    keyed = metrics.copy()
    keyed["_trade_date_key"] = calendar_date(keyed["trade_date"])

    keys = keys.rename(
        columns={"trade_date": "_trade_date_key"}
    )

    filtered = keyed.merge(
        keys,
        on=["symbol", "_trade_date_key"],
        how="inner",
        validate="one_to_one",
    )

    filtered.drop(columns=["_trade_date_key"], inplace=True)
    filtered.sort_values(
        ["symbol", "trade_date"],
        kind="mergesort",
        inplace=True,
    )
    filtered.reset_index(drop=True, inplace=True)

    logger.info(
        "compute_metrics_for_staging: history=%s staging=%s "
        "combined=%s output=%s",
        0 if history is None else len(history),
        len(staging),
        len(combined),
        len(filtered),
    )

    return filtered
