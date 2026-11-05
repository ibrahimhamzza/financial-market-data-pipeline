# API research notes

**Owner:** Ali Khaled Fawzy Mansour (Member B)
**First written:** 28 October 2026
**Last updated:** 5 November 2026
**Status:** Phase 1 research note. Documentation only; no runtime behaviour was changed.

This note separates three kinds of evidence:

- **[Implemented]** — verified by reading the repository code and tests.
- **[Provider docs]** — stated in an identifiable external source, with URL and access date.
- **[Unverified]** — not established by the repository or the sources reviewed; do not treat it as a guarantee.

Provider facts were checked on **5 November 2026**. API quotas, plans, and terms can change; re-check the linked sources before a demo or any production use.

---

## 1. Summary and data-source decision

The pipeline supports exactly two providers, defined by `SUPPORTED_DATA_SOURCES` in `config/settings.py`:

| Provider | Role in this project | Runtime default? |
|----------|----------------------|------------------|
| **yfinance** | Development, tests, and convenient backfills; no Alpha Vantage key needed | **Yes** |
| **Alpha Vantage** | Official REST API path used for the DEPI “consume an API” requirement and API demonstrations | No |

**[Implemented]** The actual default is set in `config/settings.py`:

```python
DATA_SOURCE = validate_data_source(_env("DATA_SOURCE", "yfinance") or "yfinance")
```

If `DATA_SOURCE` is absent from the environment, `fetch_daily()` uses **yfinance**. `.env.example` also ships with `DATA_SOURCE=yfinance`. Alpha Vantage is selected only by setting `DATA_SOURCE=alphavantage`.

**Wording rule used in this repository:** “runtime default” means yfinance. “Official API” means Alpha Vantage. Alpha Vantage is *not* the runtime default, and yfinance is *not* an official Yahoo Finance API or a supported production service.

**Polygon.io is not supported by the code.** It appeared in an older README comparison table but is not in `SUPPORTED_DATA_SOURCES`, is not fetched anywhere, and is not part of this decision.

---

## 2. Alpha Vantage — official REST API

### 2.1 Endpoint and request

**[Implemented]** The project calls the documented daily endpoint directly with `requests`:

- **Method / URL:** `GET https://www.alphavantage.co/query`
- **Parameters sent by `fetch_alpha_vantage()`:**
  - `function=TIME_SERIES_DAILY`
  - `symbol=<ticker>`
  - `outputsize=<ALPHA_VANTAGE_OUTPUT_SIZE>` (default `compact`)
  - `apikey=<ALPHA_VANTAGE_API_KEY>`
  - `datatype=json`
- **Timeout:** `HTTP_TIMEOUT_SECONDS` (default 30 seconds)
- **Library:** the project uses `requests` directly. The `alpha-vantage` Python package is listed in `requirements.txt` but is **not imported** by the application code.

**[Provider docs]** `TIME_SERIES_DAILY` returns raw (as-traded) daily OHLCV data for the requested equity. `outputsize=compact` returns the latest 100 data points. `outputsize=full` returns the full 25+ year history, but the documentation marks `full` as available to premium keys. The endpoint also accepts `datatype=json` or `csv`; the project requests JSON.

Source: Alpha Vantage API documentation, <https://www.alphavantage.co/documentation/> (accessed 5 November 2026).

### 2.2 API key and configuration

**[Implemented]** Alpha Vantage requires an API key. The project reads it from the environment through `.env`:

- Variable: `ALPHA_VANTAGE_API_KEY`
- Loaded by `python-dotenv` in `config/settings.py`
- Validated by `require_alpha_vantage_key()`
- A missing key, an empty key, or a known placeholder such as `your_api_key_here` raises `RuntimeError` **before** any HTTP request is made
- The key is sent as the `apikey` query-string parameter, as required by the provider

**[Provider docs]** A free key can be claimed at <https://www.alphavantage.co/support/#api-key>. Claiming a key means agreeing to Alpha Vantage’s Terms of Service and Privacy Policy.

Never commit a real key. Keep it in `.env`; `.env.example` contains only a placeholder.

### 2.3 Success payload and column mapping

**[Implemented]** The parser expects the documented envelope:

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

| Vendor key | Internal column | Type after parsing |
|------------|-----------------|--------------------|
| object key (date) | `trade_date` | datetime (naive UTC) / `DATE` |
| `1. open` | `open_price` | float / `NUMERIC` |
| `2. high` | `high_price` | float / `NUMERIC` |
| `3. low` | `low_price` | float / `NUMERIC` |
| `4. close` | `close_price` | float / `NUMERIC` |
| `5. volume` | `volume` | `Int64` / `BIGINT` |

`parse_alpha_vantage_payload()` requires every one of the five numbered OHLCV keys on every bar. A bar that is not an object, or that is missing a key, raises `PayloadValidationError`; the frame is not returned.

### 2.4 Historical availability and limitations

**[Provider docs]**

- `TIME_SERIES_DAILY` covers 25+ years of daily history.
- `compact` returns only the latest 100 data points.
- `full` returns the full history but requires a premium key according to the current documentation.
- `TIME_SERIES_DAILY` is **raw / unadjusted**. The adjusted daily endpoint, `TIME_SERIES_DAILY_ADJUSTED`, is marked as a premium API function.

**[Implemented]**

- The project defaults to `compact`, so a normal free-key request sees only the latest ~100 sessions.
- `fetch_alpha_vantage(symbol)` does **not** send `start` or `end` to the provider. `fetch_daily()` applies `start`/`end` as a **local filter after download**.
- Consequence: with `compact`, a watermark older than the compact window cannot be backfilled from that response. The pipeline will only see the latest ~100 sessions and then filter them. This is a real limitation of the current implementation, not a provider guarantee.
- The project does not use the adjusted endpoint. Splits and dividends can therefore distort price levels and returns around corporate actions. The raw JSON landing file preserves the vendor payload, so a later re-processing job could add adjustments.
- There is no pagination, quota guard, or request counter in the code.

### 2.5 Error and throttle handling

**[Implemented]** `validate_http_response()` rejects:

- any HTTP status other than 200 (`APIClientError`);
- an empty HTTP body (`APIClientError`);
- a body that is not valid JSON (`PayloadValidationError`);
- an empty JSON object (`PayloadValidationError`).

`parse_alpha_vantage_payload()` treats these top-level keys as fatal and raises `PayloadValidationError`:

- `"Error Message"` — commonly used for an invalid request, symbol, or key;
- `"Note"` — commonly used for a rate-limit / call-frequency message;
- `"Information"` — commonly used for an entitlement, premium, or daily-cap message.

**[Implemented]** `_retry_call()` retries `ConnectionError`, `Timeout`, and any `APIClientError` (including `PayloadValidationError`) up to `MAX_RETRIES` total attempts (default 3), with exponential backoff starting at `RETRY_BASE_DELAY_SECONDS` (default 1 second). There is no `Retry-After` handling and no special treatment for HTTP 429.

Because payload errors are retried too, a rate-limit `Note` or a premium `Information` response can consume multiple attempts before the symbol is marked failed. This is worth monitoring on the free tier.

**[Unverified]** Alpha Vantage’s public documentation reviewed for this note does not define a stable schema or exact wording for these error envelopes. The code checks the three conventional keys rather than depending on message text. The provider may add new envelopes or change wording without notice.

### 2.6 Rate limits and quotas

**[Provider docs]** Alpha Vantage’s support page states:

- the free service covers **25 API requests per day**;
- **unlimited API requests** are available for **verified open-source or educational projects**;
- larger volumes require a premium membership.

Source: Alpha Vantage Support / FAQ, <https://www.alphavantage.co/support/> (accessed 5 November 2026).

**[Implemented]** The project has not applied for, or verified, the open-source/educational exception. Treat **25 requests/day** as the planning assumption for a free key. A full refresh of the eight default symbols normally uses 8 successful requests, before retries. Retries, manual runs, and the Airflow schedule can push a day above 8.

There is currently no code-level quota guard. Quota compliance is an operator responsibility. See also `docs/07_requirements_gathering.md` NFR4.

### 2.7 Licensing, terms, and production use

**[Provider docs]** The Alpha Vantage Terms of Service grant a personal, non-commercial right to use the platform unless a separate written agreement exists. The Terms define “commercial use” broadly (for example, use on behalf of a company, or providing the data to others as part of a commercial activity). The platform and content are provided “as is” and “as available”, with disclaimers of warranties.

Source: Alpha Vantage Terms of Service, <https://www.alphavantage.co/terms_of_service/> (accessed 5 November 2026).

**Production considerations (not implemented here):**

- Confirm whether the intended use is personal/educational or commercial; contact Alpha Vantage for commercial use.
- Budget for a premium plan if `full` history, adjusted prices, or higher throughput is required.
- Add a quota/rate-limit guard before scheduling frequent runs.
- Treat the API as best-effort: the Terms disclaim uninterrupted or error-free service.
- Keep the key out of source control and logs.

This is a summary for engineering planning, not legal advice.

---

## 3. yfinance — third-party Yahoo Finance library

### 3.1 What it is

**[Provider docs]** `yfinance` is an open-source Python library that downloads market data from Yahoo Finance’s publicly available endpoints. It is **not** affiliated, endorsed, or vetted by Yahoo, Inc. The project README describes it as intended for research and educational purposes, and states that the Yahoo Finance API is intended for personal use only. Yahoo’s own terms govern the downloaded data.

Sources:

- yfinance GitHub README, <https://github.com/ranaroussi/yfinance> (accessed 5 November 2026)
- yfinance license (Apache-2.0), <https://github.com/ranaroussi/yfinance/blob/main/LICENSE.txt> (accessed 5 November 2026)
- Yahoo terms linked from that README, including <https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm> (accessed 5 November 2026)

**Do not describe yfinance as “unlimited” or as an official Yahoo Finance API.** The library is convenient for development, but it has no project-level SLA, no contractual quota, and no guarantee of availability.

### 3.2 Actual implementation

**[Implemented]** `fetch_yfinance()`:

1. Calls `yf.Ticker(symbol).history(interval="1d", auto_adjust=False, ...)`.
2. Passes `start`/`end` when supplied.
3. If neither is supplied, requests `period="1y"`.
4. Falls back to `yf.download(...)` only if `Ticker.history()` returns an empty frame.
5. Raises `APIClientError` if both calls return no rows.
6. Flattens a `MultiIndex` column layout if necessary.
7. Requires `Open`, `High`, `Low`, `Close`, and `Volume`.
8. Renames those columns to the same internal OHLCV names used by Alpha Vantage.
9. Writes a record-oriented JSON landing file:
   `{"source": "yfinance", "symbol": "<SYMBOL>", "bars": [...]}`.

The older note that said the project calls `yfinance.download(...)` first was inaccurate; `Ticker.history()` is the primary call.

### 3.3 Response structure and adjustment caveat

**[Provider docs]** With `auto_adjust=True` (the library default), yfinance returns OHLC prices adjusted for splits and dividends and does not return a separate `Adj Close` column. With `auto_adjust=False`, it returns Yahoo’s split-adjusted OHLC plus an `Adj Close` column that additionally accounts for dividends.

Source: yfinance “Price adjustment” documentation, <https://ranaroussi.github.io/yfinance/advanced/price_adjustment.html> (accessed 5 November 2026).

**[Implemented]** The project explicitly passes `auto_adjust=False`. It keeps only `Open`, `High`, `Low`, `Close`, and `Volume`; `Adj Close` is discarded by `_standardize_frame()`. Therefore the stored yfinance OHLCV is **split-adjusted but not dividend-adjusted**, not raw as-traded data. This differs from Alpha Vantage `TIME_SERIES_DAILY`, which is raw/as-traded. The two providers are therefore not adjustment-equivalent.

### 3.4 Historical availability and limitations

**[Provider docs]** `yfinance.download()` accepts either `period` or `start`/`end`. Valid periods include `1d`, `5d`, `1mo`, `3mo`, `6mo`, `1y`, `2y`, `5y`, `10y`, `ytd`, and `max`. `start` is inclusive; `end` is exclusive.

Source: yfinance `download()` API reference, <https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html> (accessed 5 November 2026).

**[Implemented]** When no explicit range is supplied, the project requests **1 year**, not `max`. Incremental runs normally supply a watermark as `start`. The maximum history actually available depends on Yahoo and is **not guaranteed** by the project or by the library documentation reviewed here.

Weekend and holiday gaps are expected in daily equity data and are not defects. A timezone-aware index is normalized to a naive UTC timestamp by `_standardize_frame()`.

### 3.5 Error handling

**[Implemented]**

- An empty result from both `Ticker.history()` and `yf.download()` raises `APIClientError`; this happens inside the call wrapped by `_retry_call()` and is retried up to `MAX_RETRIES` total attempts.
- A frame missing any required OHLCV column raises `PayloadValidationError`. This check runs **after** `_retry_call()` returns, so the missing-column error is not retried by that helper.
- Either error is recorded as a symbol failure by `run_extraction()`.

Because an empty yfinance download is raised as an error, an up-to-date symbol is treated as a **symbol failure**, not as the `NO_NEW_DATA` success path. This is consistent with the failure policy in `docs/08_system_analysis_and_design.md` §8.

### 3.6 Quotas, throttling, and production use

**[Unverified]** The yfinance README and API reference reviewed for this note do not publish a guaranteed request quota, rate limit, or SLA. Yahoo may throttle, block, or change its endpoints. The project has no yfinance-specific quota guard or retry-after handling.

**Production considerations:**

- Do not rely on yfinance for an SLA-backed production feed.
- Review Yahoo’s terms for the intended use; the README describes the Yahoo Finance API as personal use only.
- Consider a licensed commercial provider if the pipeline becomes a production or commercial service.
- Cache/land raw responses (the project already writes JSON files) to reduce repeated calls and preserve an audit trail.

This is not legal advice.

---

## 4. Provider comparison

| Aspect | Alpha Vantage | yfinance |
|--------|---------------|----------|
| Nature | Official REST API from Alpha Vantage Inc. | Third-party open-source Python library over Yahoo Finance endpoints |
| Official Yahoo support | Not applicable | **No** — not affiliated, endorsed, or vetted by Yahoo |
| Auth in this project | `ALPHA_VANTAGE_API_KEY` query parameter | None |
| Primary call | `GET https://www.alphavantage.co/query` with `TIME_SERIES_DAILY` | `yf.Ticker(symbol).history(interval="1d", auto_adjust=False, ...)` |
| Output | JSON vendor envelope parsed into a DataFrame | pandas DataFrame from the library |
| Default history in this project | `compact` = latest ~100 sessions | `period="1y"` when no range is given |
| Adjustment | Raw/as-traded (`TIME_SERIES_DAILY`); adjusted endpoint is premium | Split-adjusted OHLC; `Adj Close` (dividend-adjusted) is discarded |
| Quota / SLA | 25 requests/day free per provider support page; premium plans; no uptime SLA in the Terms | No published quota or SLA in the sources reviewed; Yahoo may throttle |
| Project role | Official API requirement / demo | Runtime default for development and tests |

---

## 5. Validation rules during extraction

### 5.1 API/HTTP validation

**[Implemented]**

1. HTTP status must be 200.
2. The body must be non-empty.
3. The body must be valid JSON and a non-empty JSON object.
4. Every required OHLCV field must exist on every bar (Alpha Vantage), or every required column must exist (yfinance).
5. Retries use `MAX_RETRIES` total attempts (default 3), exponential backoff, on timeout / connection / `APIClientError`. Alpha Vantage payload errors are raised inside the retried call, so they are retried; yfinance’s post-download missing-column check is not.
6. Symbol failures are collected by `run_extraction()`. All symbols must complete without error for extraction to succeed. If any symbol fails — even when other symbols succeeded — `ExtractionError` is raised, `run_pipeline()` marks the run failed, and no `_staging.csv` is written. Successful frames from a partial run are not loaded.
7. An empty frame is not a failure by itself: if every symbol returns empty with no errors, `run_extraction()` marks the result `no_new_data`. yfinance’s empty download is raised as `APIClientError`, so it becomes a symbol failure instead.

### 5.2 Pipeline staging validation (separate from API validation)

After extraction succeeds, `pipeline.validate_task()` validates the batch file, not the vendor response:

1. If `raw_data/_staging.csv` does not exist, the task returns `"SKIP_NO_NEW_DATA"` (the no-new-data path).
2. The staging CSV must contain rows.
3. Required columns: `symbol`, `trade_date`, `open_price`, `high_price`, `low_price`, `close_price`, `volume`.
4. `symbol` and `trade_date` must not be null.
5. Any failure raises `ValueError`, which `run_pipeline()` records as a failed run and re-raises.

For the complete cross-task failure policy, see [08_system_analysis_and_design.md](./08_system_analysis_and_design.md#8-error-handling-policy).

---

## 6. Manual test (development)

```bash
cp .env.example .env
# Default source is yfinance; no Alpha Vantage key is required.
python -c "from ingestion.api_client import fetch_daily; print(fetch_daily('AAPL').head())"
```

For an Alpha Vantage smoke test, set `DATA_SOURCE=alphavantage` and a real `ALPHA_VANTAGE_API_KEY` in `.env`, then run the same command. Prefer one symbol and `ALPHA_VANTAGE_OUTPUT_SIZE=compact` to conserve the free-tier quota:

```bash
DATA_SOURCE=alphavantage ALPHA_VANTAGE_OUTPUT_SIZE=compact \
  python -c "from ingestion.api_client import fetch_daily; print(fetch_daily('AAPL').head())"
```

JSON files appear under `raw_data/`. Do not commit them.

**[Unverified]** No live request/response transcript or measured quota consumption is stored in this repository. The manual commands above are the intended smoke test; they are not evidence that a live call was performed.

---

## 7. Decision

| Environment | Source | Why |
|-------------|--------|-----|
| Coding / pytest / mentor laptop | **yfinance** (runtime default) | No key, convenient backfills, suitable for development; not an official or unlimited service |
| Official API demonstration | **alphavantage**, `compact`, limited symbols if quota is low | Satisfies the DEPI “consume an API” requirement and exercises the documented REST path |
| Airflow scheduled runs | Whatever `DATA_SOURCE` is configured; **yfinance** if unset | The DAG does not override the settings default |
| Production / commercial (out of scope) | Not selected | Requires a licensed provider, quota/SLA review, and legal review |

---

## 8. Sources and remaining research gaps

### 8.1 External sources (accessed 5 November 2026)

| Source | URL | Used for |
|--------|-----|----------|
| Alpha Vantage API documentation | <https://www.alphavantage.co/documentation/> | Endpoint, parameters, `compact`/`full`, 25+ years, adjusted endpoint, premium status |
| Alpha Vantage Support / FAQ | <https://www.alphavantage.co/support/> | Free-tier 25 requests/day, educational exception, premium pointer |
| Alpha Vantage Terms of Service | <https://www.alphavantage.co/terms_of_service/> | Personal/non-commercial licence, commercial-use definition, “as is” disclaimer |
| Alpha Vantage premium plans | <https://www.alphavantage.co/premium/> | Premium path for `full`/adjusted/higher volume |
| yfinance GitHub README | <https://github.com/ranaroussi/yfinance> | Not affiliated/endorsed by Yahoo, research/educational intent, Yahoo terms |
| yfinance `download()` reference | <https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html> | `period`, `start`/`end`, intervals |
| yfinance price adjustment | <https://ranaroussi.github.io/yfinance/advanced/price_adjustment.html> | `auto_adjust` behaviour and `Adj Close` |
| yfinance licence | <https://github.com/ranaroussi/yfinance/blob/main/LICENSE.txt> | Apache-2.0 library licence |
| Yahoo terms (linked by yfinance) | <https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm> | Data-use terms |

### 8.2 Remaining gaps

- **No live API evidence in the repository.** There is no stored request/response log or measured quota usage for either provider.
- **Alpha Vantage error envelopes are not formally specified** in the public docs reviewed. The implementation checks `"Error Message"`, `"Note"`, and `"Information"` generically.
- **yfinance quota, throttling, and maximum history are not guaranteed** by the sources reviewed. Treat them as unverified.
- **No production/commercial licensing review** has been done. The notes above are engineering summaries, not legal advice.
- **No quota guard or `Retry-After` handling** exists in the code. A free-tier Alpha Vantage key can be exhausted by retries or manual runs.
- **`alpha-vantage` package is listed but unused.** Alpha Vantage calls use `requests` directly; removing the unused dependency is an implementation decision, not part of this documentation task.
- **`ALPHA_VANTAGE_OUTPUT_SIZE` is not validated** in `config/settings.py`. A free key cannot use `full` according to the current provider documentation.
- **Database/Airflow runtime is not verified** in this repository. The technology-stack rationale in `docs/08_system_analysis_and_design.md` distinguishes code-complete components from verified runtime.
