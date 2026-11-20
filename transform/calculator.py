
"""Financial metrics: returns, moving averages, volume stats, and risk."""

from __future__ import annotations

import numpy as np
import pandas as pd

from utils.logger import get_logger

logger = get_logger("calculator")


METRIC_COLUMNS = [
    "daily_return",
    "ma_7",
    "ma_20",
    "ma_50",
    "avg_volume_7d",
    "volume_ratio",
    "cumulative_return",
    "volatility_20d",
    "max_drawdown",
]


def _per_symbol(group: pd.DataFrame) -> pd.DataFrame:
    """Calculate financial metrics independently for one symbol."""

    g = group.sort_values(
        "trade_date",
        kind="mergesort",
    ).copy()

    close = g["close_price"]
    volume = g["volume"]

    # Daily percentage return.
    g["daily_return"] = close.pct_change(fill_method=None)

    # Simple moving averages.
    g["ma_7"] = close.rolling(
        window=7,
        min_periods=7,
    ).mean()

    g["ma_20"] = close.rolling(
        window=20,
        min_periods=20,
    ).mean()

    g["ma_50"] = close.rolling(
        window=50,
        min_periods=50,
    ).mean()

    # Volume indicators.
    g["avg_volume_7d"] = volume.rolling(
        window=7,
        min_periods=7,
    ).mean()

    # Avoid division by zero.
    valid_avg_volume = g["avg_volume_7d"].where(
        g["avg_volume_7d"] > 0
    )

    g["volume_ratio"] = volume / valid_avg_volume

    # Cumulative compounded return.
    returns = g["daily_return"].fillna(0)

    wealth_index = (1 + returns).cumprod()

    g["cumulative_return"] = wealth_index - 1

    # 20-day rolling volatility.
    g["volatility_20d"] = g["daily_return"].rolling(
        window=20,
        min_periods=20,
    ).std()

    # Drawdown from the historical peak.
    running_peak = wealth_index.cummax()

    drawdown = (wealth_index / running_peak) - 1

    # Worst drawdown observed up to each date.
    g["max_drawdown"] = drawdown.cummin()

    # Replace infinite results with missing values.
    g[METRIC_COLUMNS] = g[METRIC_COLUMNS].replace(
        [np.inf, -np.inf],
        np.nan,
    )

    return g


def calculate_metrics(cleaned: pd.DataFrame) -> pd.DataFrame:
    """Add financial metrics. Rolling windows remain null until complete."""

    if cleaned is None or cleaned.empty:
        logger.info("calculate_metrics received an empty frame")

        empty = (
            cleaned.copy()
            if cleaned is not None
            else pd.DataFrame()
        )

        for col in METRIC_COLUMNS:
            if col not in empty.columns:
                empty[col] = pd.Series(dtype="float64")

        return empty

    required = {
        "symbol",
        "trade_date",
        "close_price",
        "volume",
    }

    missing = required - set(cleaned.columns)

    if missing:
        raise ValueError(
            f"Cannot calculate metrics; missing {missing}"
        )

    df = cleaned.copy()

    df["symbol"] = (
        df["symbol"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    df["trade_date"] = pd.to_datetime(
        df["trade_date"],
        errors="coerce",
    )

    if df["trade_date"].isna().any():
        raise ValueError(
            "Cannot calculate metrics; invalid trade_date values found"
        )

    if df["close_price"].isna().any() or (df["close_price"] <= 0).any():
        raise ValueError(
            "Cannot calculate metrics; close_price must be positive and non-null"
        )

    if df["volume"].isna().any() or (df["volume"] < 0).any():
        raise ValueError(
            "Cannot calculate metrics; volume must be non-negative and non-null"
        )

    # Ensure one row per symbol/date before calculating metrics.
    duplicates = df.duplicated(
        subset=["symbol", "trade_date"],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            f"Cannot calculate metrics; found {int(duplicates.sum())} "
            "duplicate symbol/date rows"
        )

    parts = [
        _per_symbol(group)
        for _, group in df.groupby("symbol", sort=False)
    ]

    metrics = pd.concat(
        parts,
        ignore_index=True,
    )

    logger.info(
        "calculate_metrics: %s rows, %s symbols",
        len(metrics),
        metrics["symbol"].nunique(),
    )

    return metrics
