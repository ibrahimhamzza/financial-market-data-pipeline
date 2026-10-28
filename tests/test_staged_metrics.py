"""Incremental staging metrics must match a full-history calculate_metrics run."""

import numpy as np
import pandas as pd

from transform.calculator import calculate_metrics
from transform.cleaner import clean_data
from transform.staged_metrics import (
    combine_history_and_staging,
    compute_metrics_for_staging,
)


def _series(n: int = 60, start: float = 100.0, symbol: str = "AAPL") -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=n)
    close = start * (1.01 ** np.arange(n))
    return pd.DataFrame(
        {
            "symbol": [symbol] * n,
            "trade_date": dates,
            "open_price": close,
            "high_price": close * 1.01,
            "low_price": close * 0.99,
            "close_price": close,
            "volume": np.full(n, 1_000_000),
        }
    )


def _full_metrics(history: pd.DataFrame) -> pd.DataFrame:
    return calculate_metrics(clean_data(history))


def _row_for(metrics: pd.DataFrame, symbol: str, trade_date) -> pd.Series:
    date = pd.Timestamp(trade_date).normalize()
    match = metrics[
        (metrics["symbol"] == symbol)
        & (pd.to_datetime(metrics["trade_date"]).dt.normalize() == date)
    ]
    assert len(match) == 1, f"expected one row for {symbol} {date}, got {len(match)}"
    return match.iloc[0]


METRIC_COLS = [
    "close_price",
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


def _assert_row_matches(actual: pd.Series, expected: pd.Series) -> None:
    for col in METRIC_COLS:
        a, e = actual[col], expected[col]
        if pd.isna(e):
            assert pd.isna(a), f"{col}: expected NA, got {a}"
        else:
            assert a == e or np.isclose(a, e, rtol=1e-12, atol=1e-12), f"{col}: {a} != {e}"


def test_one_row_incremental_with_50_plus_history():
    history = _series(60)
    staging = history.tail(1).copy()
    prior = history.iloc[:-1]
    out = compute_metrics_for_staging(staging, prior)
    assert len(out) == 1
    expected = _row_for(_full_metrics(history), "AAPL", staging["trade_date"].iloc[0])
    _assert_row_matches(out.iloc[0], expected)
    assert pd.notna(out.iloc[0]["ma_50"])
    assert pd.notna(out.iloc[0]["daily_return"])
    assert pd.notna(out.iloc[0]["volatility_20d"])


def test_genuinely_new_trading_date():
    history = _series(60)
    last = history.iloc[-1]
    new_date = last["trade_date"] + pd.offsets.BDay(1)
    new_close = last["close_price"] * 1.01
    staging = pd.DataFrame(
        {
            "symbol": ["AAPL"],
            "trade_date": [new_date],
            "open_price": [new_close],
            "high_price": [new_close * 1.01],
            "low_price": [new_close * 0.99],
            "close_price": [new_close],
            "volume": [1_000_000],
        }
    )
    combined = pd.concat([history, staging], ignore_index=True)
    out = compute_metrics_for_staging(staging, history)
    assert len(out) == 1
    expected = _row_for(_full_metrics(combined), "AAPL", new_date)
    _assert_row_matches(out.iloc[0], expected)
    assert pd.notna(out.iloc[0]["ma_7"])
    assert pd.notna(out.iloc[0]["ma_20"])
    assert pd.notna(out.iloc[0]["ma_50"])
    assert np.isclose(out.iloc[0]["daily_return"], 0.01)


def test_duplicate_date_in_staging_does_not_starve_windows():
    history = _series(60)
    staging = history.tail(1).copy()
    out = compute_metrics_for_staging(staging, history)
    assert len(out) == 1
    expected = _row_for(_full_metrics(history), "AAPL", staging["trade_date"].iloc[0])
    _assert_row_matches(out.iloc[0], expected)
    assert pd.notna(out.iloc[0]["ma_50"])
    assert out.iloc[0]["cumulative_return"] != 0 or history["close_price"].nunique() == 1


def test_duplicate_staging_ohlcv_wins_deterministically():
    history = _series(60)
    staging = history.tail(1).copy()
    staging["close_price"] = staging["close_price"] * 1.05
    staging["open_price"] = staging["close_price"]
    staging["high_price"] = staging["close_price"] * 1.01
    staging["low_price"] = staging["close_price"] * 0.99
    combined = combine_history_and_staging(history, staging)
    last_combined = combined.iloc[-1]
    assert np.isclose(last_combined["close_price"], staging["close_price"].iloc[0])
    out = compute_metrics_for_staging(staging, history)
    replaced = history.copy()
    replaced.iloc[-1, replaced.columns.get_loc("close_price")] = staging["close_price"].iloc[0]
    replaced.iloc[-1, replaced.columns.get_loc("open_price")] = staging["open_price"].iloc[0]
    replaced.iloc[-1, replaced.columns.get_loc("high_price")] = staging["high_price"].iloc[0]
    replaced.iloc[-1, replaced.columns.get_loc("low_price")] = staging["low_price"].iloc[0]
    expected = _row_for(_full_metrics(replaced), "AAPL", staging["trade_date"].iloc[0])
    _assert_row_matches(out.iloc[0], expected)


def test_output_contains_only_staging_keys():
    history = _series(60)
    staging = history.tail(2).copy()
    out = compute_metrics_for_staging(staging, history.iloc[:-2])
    assert len(out) == 2
    full = _full_metrics(history)
    assert len(out) < len(full)


def test_cumulative_return_and_max_drawdown_match_full_history_definition():
    history = _series(60)
    staging = history.tail(1)
    out = compute_metrics_for_staging(staging, history.iloc[:-1])
    full = _full_metrics(history)
    expected = _row_for(full, "AAPL", staging["trade_date"].iloc[0])
    assert np.isclose(out.iloc[0]["cumulative_return"], expected["cumulative_return"])
    assert np.isclose(out.iloc[0]["max_drawdown"], expected["max_drawdown"])
    # Existing definition: fillna(0) on first daily_return then cumprod - 1.
    rebuilt = (1 + full["daily_return"].fillna(0)).cumprod() - 1
    assert np.isclose(expected["cumulative_return"], rebuilt.iloc[-1])
    rebuilt_dd = rebuilt - rebuilt.cummax()
    assert np.isclose(expected["max_drawdown"], rebuilt_dd.iloc[-1])


def test_staging_only_backfill_still_uses_full_staging_frame():
    """First run: empty DB history, staging holds the whole extract."""
    staging = _series(60)
    out = compute_metrics_for_staging(staging, pd.DataFrame())
    expected = _full_metrics(staging)
    assert len(out) == len(expected)
    _assert_row_matches(out.iloc[-1], expected.iloc[-1])
