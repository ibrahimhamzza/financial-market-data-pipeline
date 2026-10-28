"""Financial metrics: returns, SMAs, volume stats, volatility."""

from __future__ import annotations

import pandas as pd

from utils.logger import get_logger

logger = get_logger("calculator")


def _per_symbol(group: pd.DataFrame) -> pd.DataFrame:
    g = group.sort_values("trade_date").copy()
    close = g["close_price"]
    volume = g["volume"]

    g["daily_return"] = close.pct_change()
    g["ma_7"] = close.rolling(7, min_periods=7).mean()
    g["ma_20"] = close.rolling(20, min_periods=20).mean()
    g["ma_50"] = close.rolling(50, min_periods=50).mean()
    g["avg_volume_7d"] = volume.rolling(7, min_periods=7).mean()
    g["volume_ratio"] = volume / g["avg_volume_7d"]
    g["cumulative_return"] = (1 + g["daily_return"].fillna(0)).cumprod() - 1
    g["volatility_20d"] = g["daily_return"].rolling(20, min_periods=20).std()
    g["max_drawdown"] = g["cumulative_return"] - g["cumulative_return"].cummax()
    return g


def calculate_metrics(cleaned: pd.DataFrame) -> pd.DataFrame:
    """Add metric columns. Early windows stay null until enough history exists."""
    if cleaned is None or cleaned.empty:
        logger.info("calculate_metrics received an empty frame")
        empty = cleaned.copy() if cleaned is not None else pd.DataFrame()
        for col in [
            "daily_return",
            "ma_7",
            "ma_20",
            "ma_50",
            "avg_volume_7d",
            "volume_ratio",
            "cumulative_return",
            "volatility_20d",
            "max_drawdown",
        ]:
            if col not in empty.columns:
                empty[col] = pd.Series(dtype="float64")
        return empty

    required = {"symbol", "trade_date", "close_price", "volume"}
    missing = required - set(cleaned.columns)
    if missing:
        raise ValueError(f"Cannot calculate metrics; missing {missing}")

    df = cleaned.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    parts = [_per_symbol(group) for _, group in df.groupby("symbol", sort=False)]
    metrics = pd.concat(parts, ignore_index=True)
    logger.info("calculate_metrics: %s rows, %s symbols", len(metrics), metrics["symbol"].nunique())
    return metrics
