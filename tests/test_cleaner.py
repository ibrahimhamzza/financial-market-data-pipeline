import pandas as pd

from transform.cleaner import clean_data


def test_clean_removes_duplicate_symbol_dates():
    raw = pd.DataFrame(
        {
            "symbol": ["aapl", "AAPL"],
            "trade_date": ["2024-01-02", "2024-01-02"],
            "open_price": [100.0, 101.0],
            "high_price": [102.0, 103.0],
            "low_price": [99.0, 100.0],
            "close_price": [101.0, 102.0],
            "volume": [1000, 1100],
        }
    )
    cleaned = clean_data(raw)
    assert len(cleaned) == 1
    assert cleaned.iloc[0]["symbol"] == "AAPL"
    assert cleaned.iloc[0]["close_price"] == 102.0


def test_clean_forward_fills_price_nulls():
    raw = pd.DataFrame(
        {
            "symbol": ["MSFT", "MSFT"],
            "trade_date": ["2024-01-02", "2024-01-03"],
            "open_price": [10.0, None],
            "high_price": [11.0, None],
            "low_price": [9.0, None],
            "close_price": [10.5, None],
            "volume": [500, 600],
        }
    )
    cleaned = clean_data(raw)
    assert cleaned.iloc[1]["close_price"] == 10.5
    assert cleaned.iloc[1]["volume"] == 600


def test_clean_standardizes_column_names():
    raw = pd.DataFrame(
        {
            "symbol": ["GOOGL"],
            "Date": ["2024-01-02"],
            "Open": [1.0],
            "High": [2.0],
            "Low": [0.5],
            "Close": [1.5],
            "Volume": [10],
        }
    )
    cleaned = clean_data(raw)
    assert set(cleaned.columns) >= {
        "symbol",
        "trade_date",
        "open_price",
        "close_price",
        "volume",
    }
