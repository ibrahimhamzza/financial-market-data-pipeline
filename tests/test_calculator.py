import numpy as np
import pandas as pd

from transform.calculator import calculate_metrics


def _series(n: int = 60, start: float = 100.0) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=n)
    close = start * (1.01 ** np.arange(n))
    return pd.DataFrame(
        {
            "symbol": ["AAPL"] * n,
            "trade_date": dates,
            "open_price": close,
            "high_price": close * 1.01,
            "low_price": close * 0.99,
            "close_price": close,
            "volume": np.full(n, 1_000_000),
        }
    )


def test_daily_return_matches_pct_change():
    cleaned = _series(10)
    metrics = calculate_metrics(cleaned)
    expected = cleaned["close_price"].pct_change()
    pd.testing.assert_series_equal(
        metrics["daily_return"].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_names=False,
    )


def test_moving_averages_need_full_window():
    metrics = calculate_metrics(_series(60))
    assert pd.isna(metrics.loc[5, "ma_7"])
    assert pd.notna(metrics.loc[6, "ma_7"])
    assert pd.isna(metrics.loc[18, "ma_20"])
    assert pd.notna(metrics.loc[19, "ma_20"])
    assert pd.isna(metrics.loc[48, "ma_50"])
    assert pd.notna(metrics.loc[49, "ma_50"])


def test_metrics_are_grouped_by_symbol():
    aapl = _series(15, start=100)
    msft = _series(15, start=200)
    msft["symbol"] = "MSFT"
    metrics = calculate_metrics(pd.concat([aapl, msft], ignore_index=True))
    assert set(metrics["symbol"]) == {"AAPL", "MSFT"}
    aapl_first_return = metrics.loc[metrics["symbol"] == "AAPL", "daily_return"].iloc[1]
    msft_first_return = metrics.loc[metrics["symbol"] == "MSFT", "daily_return"].iloc[1]
    assert abs(aapl_first_return - 0.01) < 1e-9
    assert abs(msft_first_return - 0.01) < 1e-9
