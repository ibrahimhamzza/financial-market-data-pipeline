# API research notes

**Owner:** Ali Khaled Fawzy Mansour (Member B)
**Date:** 28 October 2026

The pipeline supports two sources. **yfinance** is the default for development (`DATA_SOURCE=yfinance`). **Alpha Vantage** is the official REST API for the DEPI “consume an API” requirement (`DATA_SOURCE=alphavantage`).

---

## 1. Alpha Vantage — `TIME_SERIES_DAILY`

- **Endpoint:** `GET https://www.alphavantage.co/query`
- **Params:** `function=TIME_SERIES_DAILY`, `symbol`, `outputsize=compact|full`, `apikey`, `datatype=json`
- **Auth:** query-string API key (store in `.env` only)
- **Quota:** free tier ~25 requests/day — eight symbols consume 8 calls per full refresh
- **Docs:** https://www.alphavantage.co/documentation/

### Success payload (fields we require)

```json
{
  "Meta Data": {
    "2. Symbol": "AAPL",
    "3. Last Refreshed": "2024-01-03"
  },
  "Time Series (Daily)": {
    "2024-01-03": {
      "1. open": "185.6400",
      "2. high": "186.9500",
      "3. low": "185.0600",
      "4. close": "185.9200",
      "5. volume": "40432300"
    }
  }
}
```

| Vendor key | Our column | Type after parse |
|------------|------------|------------------|
| object key (date) | `trade_date` | datetime / DATE |
| `1. open` | `open_price` | float |
| `2. high` | `high_price` | float |
| `3. low` | `low_price` | float |
| `4. close` | `close_price` | float |
| `5. volume` | `volume` | int |

Unadjusted daily series (not `TIME_SERIES_DAILY_ADJUSTED`). Splits can create gaps in returns; documented as a known limitation.

### Error / throttle shapes (must not be parsed as bars)

- `"Error Message"` — bad symbol or key
- `"Note"` — rate limit
- `"Information"` — premium / daily cap messages

`parse_alpha_vantage_payload()` raises `PayloadValidationError` on these.

`compact` ≈ last 100 sessions; `full` = full history (heavier, still 1 request).

---

## 2. yfinance

- **Library:** `yfinance.download(ticker, start, end, interval="1d")`
- **Auth:** none
- **Use:** local development, unit-test-like backfills, demos when Alpha Vantage is exhausted

Returned columns: `Open`, `High`, `Low`, `Close`, `Adj Close`, `Volume` (sometimes MultiIndex). Mapped to the same internal names as Alpha Vantage. Raw JSON landing file stores a record-oriented dump of the frame.

**Edge cases:** empty frame for a bad ticker; timezone-aware index (normalized to naive UTC calendar date); weekend/holiday gaps (expected, not a defect).

---

## 3. Validation rules (HTTP + payload)

1. HTTP status must be 200.
2. Body must be non-empty JSON.
3. Required OHLCV keys must exist on every bar.
4. Retries: 3 attempts, exponential backoff, on timeout / connection / `APIClientError`.
5. One symbol failing does not abort the other seven (`extractor` logs and continues).

---

## 4. Manual test (development)

```bash
cp env.example .env
# DATA_SOURCE=yfinance
python -c "from ingestion.api_client import fetch_daily; print(fetch_daily('AAPL').head())"
```

JSON files appear under `raw_data/`. Do not commit them.

---

## 5. Decision

| Environment | Source |
|-------------|--------|
| Coding / pytest / mentor laptop | yfinance |
| Official API demonstration | alphavantage, `compact`, few symbols if quota is low |
