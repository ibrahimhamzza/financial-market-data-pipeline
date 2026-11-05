# Requirements Gathering

**Project:** Automated Financial Market Data Pipeline
**Date:** 26 October 2026

This document covers DEPI section 3: stakeholders, user stories / use cases, functional requirements, and non-functional requirements. UI wireframes are **not** applicable; actors use Airflow, SQL, and notebooks.

---

## 1. Stakeholder analysis

| Stakeholder | Type | Needs | How the pipeline serves them |
|-------------|------|-------|------------------------------|
| DEPI evaluation panel | External | Working repo, docs, 15-min defense, clear roles | GitHub + this `docs/` pack + demo DAG and SQL |
| Group instructor / mentor | External | Checkpoints on DEPI dates, honest progress | Milestone uploads; leader communication |
| Team members (A–D) | Internal | Clear modules, no overlapping files, runnable local setup | Roles file + README |
| Market analyst (persona) | Internal / hypothetical | Trusted daily OHLCV + returns/MAs in SQL | Curated table + views + notebooks |
| Data engineer (persona) | Internal / hypothetical | Scheduled job, logs, no duplicate loads | Airflow + `pipeline_runs` + idempotent loader |
| API provider (Alpha Vantage) | External constraint | Respect rate limits and API key secrecy | yfinance for volume work; key in `.env` only |

---

## 2. Actors

| Actor | Description |
|-------|-------------|
| **Operator** | Starts/monitors the DAG (team member on demo day). |
| **Analyst** | Reads PostgreSQL / notebooks; does not call the API by hand. |
| **Developer** | Changes Python/SQL; runs pytest. |
| **Scheduler** | Airflow scheduler (system actor) at 18:00 weekdays. |
| **Market Data API** | External system (Alpha Vantage or yfinance). |
| **PostgreSQL** | System of record. |

---

## 3. User stories

| ID | Story | Acceptance idea |
|----|--------|-----------------|
| US1 | As an **operator**, I want a one-click / scheduled DAG so I do not run five scripts by hand. | Airflow graph extract → validate → transform → load → quality_check turns green. |
| US2 | As an **analyst**, I want daily close and volume per symbol in a table. | `raw_market_data` has one row per `(symbol, trade_date)`. |
| US3 | As an **analyst**, I want daily return and 7/20/50-day MAs already computed. | `transformed_market_data` populated; sample SQL returns rows. |
| US4 | As a **developer**, I want retries on HTTP 429/5xx so a blip does not fail the night run. | Client retries up to 3 times with backoff. |
| US5 | As a **developer**, I want re-running today not to duplicate yesterday. | Second run inserts 0 duplicate keys. |
| US6 | As an **operator**, I want to see whether last night failed and why. | `pipeline_runs` has status + `error_message`. |
| US7 | As an **analyst**, I want only **new** days fetched after the first backfill. | Incremental watermark per symbol. |
| US8 | As an **evaluator**, I want SQL that uses window functions, not only Pandas. | ≥ 5 queries in `analytics.sql`. |
| US9 | As an **evaluator**, I want proof the data is not garbage. | Quality notebook: nulls, duplicates, completeness. |
| US10 | As a **developer**, I want secrets out of Git. | `.env` gitignored; `.env.example` committed. |

---

## 4. Use cases (brief)

### UC-01 Nightly incremental load

1. Scheduler triggers `financial_pipeline_dag`.
2. System reads last loaded date per symbol.
3. System calls API for missing dates.
4. System validates HTTP + payload during extraction.
5. System writes JSON files and the staging CSV.
6. System validates the staging CSV.
7. System cleans, calculates metrics, loads raw and transformed tables, and runs quality checks.
8. System records `pipeline_runs = success`.
**Exception:** API down after retries, or any symbol failing while others succeed → run `failed` (`ExtractionError`), error stored, and no staging/load occurs; partial success is never reported as success.

### UC-02 First-time historical backfill

Same flow with an empty watermark (full history within API limits / yfinance range).

### UC-03 Analyst ranking query

Analyst runs a CTE + `RANK() OVER (PARTITION BY symbol ORDER BY daily_return DESC)` on the last 90 days (see README sample).

### UC-04 Local quality inspection

Developer opens the data-quality notebook against PostgreSQL and exports figures for the report.

---

## 5. Functional requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| FR1 | Fetch daily OHLCV for the configured symbol list | Must |
| FR2 | Support Alpha Vantage REST as the official API; yfinance as the development source | Must |
| FR3 | Validate HTTP status and required JSON fields before parse | Must |
| FR4 | Retry transient failures (max 3, exponential backoff) | Must |
| FR5 | Persist raw vendor JSON under `raw_data/` | Must |
| FR6 | Persist raw structured rows in `raw_market_data` | Must |
| FR7 | Clean: types, duplicate `(symbol, date)`, price forward-fill policy | Must |
| FR8 | Compute daily return, SMA 7/20/50, volume ratio, cumulative return, 20d volatility | Must |
| FR9 | Persist metrics in `transformed_market_data` | Must |
| FR10 | Incremental extract using last loaded date per symbol | Must |
| FR11 | Idempotent load (no duplicate business keys) | Must |
| FR12 | Log each run in `pipeline_runs` | Must |
| FR13 | Airflow DAG with five tasks and weekday 18:00 schedule | Must |
| FR14 | ≥ 5 analytical SQL queries (windows + CTEs) and reusable views | Must |
| FR15 | Unit tests for client, cleaner, calculator | Must |
| FR16 | Data quality report + time-series charts | Must |
| FR17 | Configuration via environment variables (`.env.example`) | Must |
| FR18 | Central logging module used by pipeline stages | Should |
| FR19 | Docker Compose for app + Postgres | Optional |
| FR20 | MongoDB raw document store | Optional |

### 5.1 Acceptance criteria — functional requirements

Each criterion is written against the existing design in [08_system_analysis_and_design.md](./08_system_analysis_and_design.md) and [09_api_research.md](./09_api_research.md). A requirement is satisfied only when all of its criteria are met. The criteria do not change the requirement text above; where the current implementation does not meet a criterion, the gap is recorded in the traceability table in §9.

Verification methods used below: **Test** = automated pytest or equivalent repeatable check; **SQL query** = query run against `financial_db`; **Inspection** = source, configuration, or repository review; **Manual check** = documented operator step.

#### FR1 — Fetch daily OHLCV for the configured symbol list

- **FR1-AC1:** Given `SYMBOLS` (default `AAPL,MSFT,GOOGL,AMZN,META,TSLA,NVDA,JPM`), `extractor.run_extraction()` attempts a fetch for every configured ticker, regardless of whether an earlier ticker failed.
- **FR1-AC2:** A successful per-symbol fetch returns rows with the standardized columns `symbol`, `trade_date`, `open_price`, `high_price`, `low_price`, `close_price`, and `volume`; `symbol` is upper-case and `trade_date` is a timezone-naive UTC date.
- **FR1-AC3:** If all requested symbols return data without error, the combined extraction result is non-empty and contains at least one row for each symbol that returned data. Symbols that return an empty frame without error are logged as “No new rows” and omitted; symbols that raise an error are handled by FR3/FR4 and the failure policy in [08 §8](./08_system_analysis_and_design.md#8-error-handling-policy).

#### FR2 — Support Alpha Vantage REST as the official API; yfinance as the development source

- **FR2-AC1:** `DATA_SOURCE` is normalized for case and surrounding whitespace and accepts only `yfinance` and `alphavantage`. Any other value raises `ValueError` when settings are loaded and is converted to `APIClientError` if passed directly to `fetch_daily()`.
- **FR2-AC2:** With `DATA_SOURCE=alphavantage`, a non-placeholder `ALPHA_VANTAGE_API_KEY` causes a request to `https://www.alphavantage.co/query` with `function=TIME_SERIES_DAILY`, `symbol`, `outputsize`, `apikey`, and `datatype=json`. A missing or placeholder key raises `RuntimeError` before any HTTP request is sent.
- **FR2-AC3:** With `DATA_SOURCE=yfinance`, data is fetched without requiring an Alpha Vantage key. Both sources return the same standardized column set defined in FR1-AC2.

#### FR3 — Validate HTTP status and required JSON fields before parse

- **FR3-AC1:** `validate_http_response()` raises `APIClientError` for a non-200 HTTP status or an empty response body, and raises `PayloadValidationError` for a body that is not JSON or whose decoded JSON is empty.
- **FR3-AC2:** Alpha Vantage payloads containing `Error Message`, `Note`, or `Information`, missing `Time Series (Daily)`, a bar that is not an object, or a bar missing any of `1. open`, `2. high`, `3. low`, `4. close`, `5. volume` raise `PayloadValidationError` and do not return a DataFrame.
- **FR3-AC3:** A yfinance frame missing any of `Open`, `High`, `Low`, `Close`, or `Volume` raises `PayloadValidationError`; an empty yfinance download raises `APIClientError`.
- **FR3-AC4:** No invalid response or payload reaches staging, transformation, or the database as if it were valid OHLCV data.

#### FR4 — Retry transient failures (max 3, exponential backoff)

- **FR4-AC1:** `_retry_call()` makes at most `MAX_RETRIES` attempts (default `3`) when the wrapped call raises `requests.ConnectionError`, `requests.Timeout`, or `APIClientError` (including its `PayloadValidationError` subclass). Other exception types are not retried.
- **FR4-AC2:** After failed attempt *n* (where *n* < `MAX_RETRIES`), the client sleeps `RETRY_BASE_DELAY_SECONDS × 2^(n−1)` seconds. With defaults this is 1 second after attempt 1 and 2 seconds after attempt 2. No sleep occurs after the final attempt.
- **FR4-AC3:** If an attempt succeeds, its result is returned immediately. If all attempts fail, the client raises `APIClientError` containing the attempt count and the last error, and the caller records that symbol as failed.
- **FR4-AC4:** Each retry logs a warning containing the attempt number, maximum attempts, error text, and delay.

#### FR5 — Persist raw vendor JSON under `raw_data/`

- **FR5-AC1:** For Alpha Vantage, each response that passes `validate_http_response()` is written before parsing; for yfinance, each successful non-empty download is written. Both use `RAW_DATA_DIR` (default `raw_data/`) and the file name `<UPPERCASE_SYMBOL>_<YYYYMMDDTHHMMSSZ>.json`. The directory is created if it does not exist.
- **FR5-AC2:** The JSON file is valid JSON. Alpha Vantage files contain the vendor payload; yfinance files contain `{"source": "yfinance", "symbol": "<SYMBOL>", "bars": [...]}`.
- **FR5-AC3:** Files matching `raw_data/*.json` are excluded by `.gitignore` and are not committed.

#### FR6 — Persist raw structured rows in `raw_market_data`

- **FR6-AC1:** After a successful `load_task()` on a non-empty staging file, every staging row with a valid `(symbol, trade_date)` key has a corresponding row in `raw_market_data`.
- **FR6-AC2:** The stored columns `symbol`, `trade_date`, `open_price`, `high_price`, `low_price`, `close_price`, and `volume` match the staging values, and `loaded_at` is populated by the database default.
- **FR6-AC3:** A second load of the same business keys does not insert a second raw row; this is enforced by the unique constraint and `ON CONFLICT DO NOTHING` (see FR11).

#### FR7 — Clean: types, duplicate `(symbol, date)`, price forward-fill policy

- **FR7-AC1:** `clean_data()` renames supported source names (`Open`, `High`, `Low`, `Close`, `Volume`, `Date`, etc.) to canonical names, upper-cases and trims `symbol`, parses `trade_date` to a timezone-naive UTC datetime, numeric-casts prices and volume, and raises `ValueError` if a required column is missing.
- **FR7-AC2:** Rows are sorted by `symbol` and `trade_date`; duplicate `(symbol, trade_date)` pairs collapse to one row, keeping the last occurrence.
- **FR7-AC3:** Within each symbol, missing prices are forward-filled only from earlier rows. Rows that still have a null `close_price`, `symbol`, or `trade_date` are dropped. Null volume becomes 0, negative volume is clipped to 0, and volume is stored as an integer. Rows with `high_price < low_price` are dropped.
- **FR7-AC4:** The transform stage applies `clean_data()` before metric calculation. The raw load path persists extractor-standardized staging rows without an additional cleaning call, so forward-fill and de-duplication affect transformed metrics rather than `raw_market_data`.

#### FR8 — Compute daily return, SMA 7/20/50, volume ratio, cumulative return, 20d volatility

Metrics are calculated per symbol in ascending `trade_date` order.

- **FR8-AC1:** `daily_return = close_price.pct_change()`; the first row of each symbol has a null return.
- **FR8-AC2:** `ma_7`, `ma_20`, and `ma_50` are rolling means of `close_price` with windows 7, 20, and 50 and `min_periods` equal to the window; values are null until enough observations exist.
- **FR8-AC3:** `avg_volume_7d` is the 7-row rolling mean of `volume` with `min_periods=7`; `volume_ratio = volume / avg_volume_7d` using pandas division and is null when `avg_volume_7d` is null. The current code has no separate guard for an `avg_volume_7d` of zero.
- **FR8-AC4:** `cumulative_return = (1 + daily_return.fillna(0)).cumprod() − 1`, compounding from the first row of the supplied per-symbol history.
- **FR8-AC5:** `volatility_20d` is the 20-row rolling standard deviation of `daily_return` with `min_periods=20` and pandas' default sample standard deviation (`ddof=1`); it is null until 20 returns exist.
- **FR8-AC6:** Metrics for different symbols do not share rolling windows or cumulative state.

#### FR9 — Persist metrics in `transformed_market_data`

- **FR9-AC1:** When the transform output is non-empty, every metric row produced for a staging key has exactly one row in `transformed_market_data` with the same `(symbol, trade_date)`.
- **FR9-AC2:** The persisted columns `close_price`, `daily_return`, `ma_7`, `ma_20`, `ma_50`, `avg_volume_7d`, `volume_ratio`, `cumulative_return`, and `volatility_20d` match the FR8 definitions. `max_drawdown` is calculated in memory by `calculate_metrics()` but is not persisted because the table has no such column.
- **FR9-AC3:** Re-loading the same business keys updates the metric values with `ON CONFLICT DO UPDATE` and does not create duplicate keys.

#### FR10 — Incremental extract using last loaded date per symbol

- **FR10-AC1:** For a symbol whose maximum stored date is *D*, `incremental.next_start_date(symbol)` returns `D + 1 day` in ISO format. If no row exists, it returns `None` (full history).
- **FR10-AC2:** `run_extraction()` passes the watermark result as the `start` argument to `fetch_daily()` when `incremental=True` and no explicit start was supplied.
- **FR10-AC3:** Alpha Vantage results are filtered to `trade_date >= start` before being returned. yfinance receives `start` through its history/download call and relies on the provider to honor it; the code does not apply an additional post-filter to the yfinance frame.
- **FR10-AC4:** If the watermark query fails, extraction logs a warning and falls back to a full-history request for that symbol. An empty yfinance download is treated as a symbol failure (`APIClientError`), not as `NO_NEW_DATA`, consistent with [08 §8](./08_system_analysis_and_design.md#8-error-handling-policy).

#### FR11 — Idempotent load (no duplicate business keys)

- **FR11-AC1:** `raw_market_data` and `transformed_market_data` each have a unique constraint on `(symbol, trade_date)`.
- **FR11-AC2:** Running the same non-empty batch twice leaves the number of distinct business keys unchanged. The second raw load inserts 0 new rows; the transformed upsert may update existing rows but creates no duplicate keys.
- **FR11-AC3:** After loading, `SELECT COUNT(*) FROM (SELECT symbol, trade_date FROM raw_market_data GROUP BY symbol, trade_date HAVING COUNT(*) > 1) d` returns 0.

#### FR12 — Log each run in `pipeline_runs`

- **FR12-AC1:** On the CLI path (`run_pipeline(record_run=True)`), one `pipeline_runs` row is inserted with `status='running'` before extraction, and the same row is updated to `success` or `failed` after the run.
- **FR12-AC2:** For a successful non-empty CLI run, `duration_seconds` is the measured wall time, `rows_inserted` is the raw insert count returned by `load_task()`, and `symbols_processed` is the `COUNT(DISTINCT symbol)` returned by `run_quality_checks()` at the end of the run (not only symbols fetched that night).
- **FR12-AC3:** A `NO_NEW_DATA` run is recorded as `success` with `symbols_processed=0` and `rows_inserted=0`. A failed run stores `status='failed'` and a non-empty `error_message` containing the exception text. If the initial audit insert fails, the pipeline logs a warning and continues without a run row.
- **FR12-AC4 — Needs stakeholder decision:** The current Airflow DAG invokes the five task callables directly and does **not** write `pipeline_runs`; Airflow task state is separate. Decide whether “each run” in FR12 must include Airflow-triggered runs. If yes, the DAG must create and finish run-audit rows in addition to the CLI path; if no, FR12 is satisfied for CLI runs only and this limitation must be stated in the evaluation materials.

#### FR13 — Airflow DAG with five tasks and weekday 18:00 schedule

- **FR13-AC1:** `dags/financial_pipeline_dag.py` defines a DAG with `dag_id="financial_pipeline_dag"`, `schedule="0 18 * * 1-5"`, `start_date=datetime(2026, 10, 26)`, `catchup=False`, and `max_active_runs=1`.
- **FR13-AC2:** The DAG contains exactly five `PythonOperator` tasks with IDs `extract`, `validate`, `transform`, `load`, and `quality_check`, wired as `extract >> validate >> transform >> load >> quality_check`.
- **FR13-AC3:** When Airflow is installed, the DAG imports without error and appears in the Airflow UI with the five tasks and the weekday schedule. Airflow owns task state; it does not update `pipeline_runs` (see FR12-AC4).

#### FR14 — ≥ 5 analytical SQL queries (windows + CTEs) and reusable views

- **FR14-AC1:** `database/analytics.sql` contains at least five top-level `SELECT` statements, each executable against `financial_db` after `schema.sql` has been applied.
- **FR14-AC2:** At least five queries use a window function (`RANK`, `LAG`, `LEAD`, `AVG() OVER`, or `ROW_NUMBER`); at least one query uses a common table expression. All referenced tables and columns exist in the schema.
- **FR14-AC3:** `database/views.sql` creates reusable views after `schema.sql`; the current views are `v_latest_prices`, `v_top_gainers`, and `v_volume_spikes`, and each can be queried successfully against a populated database.

#### FR15 — Unit tests for client, cleaner, calculator

- **FR15-AC1:** `tests/test_api_client.py`, `tests/test_cleaner.py`, and `tests/test_calculator.py` exist and each contains at least one test of its module.
- **FR15-AC2:** `pytest tests/ -v` exits with status 0 and reports no failures or errors. The suite does not require a live database or network access.
- **FR15-AC3:** The tests cover, at minimum: HTTP/payload rejection and Alpha Vantage parsing; duplicate removal and price forward-fill; daily return and the full-window requirement for MA 7/20/50.

#### FR16 — Data quality report + time-series charts

- **FR16-AC1:** `notebooks/data_quality_report.ipynb` and `notebooks/market_analysis.ipynb` exist and execute against a populated `financial_db` without manual data editing.
- **FR16-AC2:** The data-quality report shows row counts, OHLC null counts/rates, duplicate `(symbol, trade_date)` counts, per-symbol coverage/completeness, orphan transformed rows, and a `pipeline_runs` summary.
- **FR16-AC3:** The time-series output includes, per configured symbol, a price chart with MA 7/20/50, a volume or volume-ratio chart, and a daily/cumulative return chart. At least one chart is exported as an image for the report.
- **FR16-AC4:** Charts are generated from PostgreSQL query results, not from hard-coded sample values.

#### FR17 — Configuration via environment variables (`.env.example`)

- **FR17-AC1:** Settings are read from environment variables, with `.env` loaded from the project root. Supported variables include `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `ALPHA_VANTAGE_API_KEY`, `DATA_SOURCE`, `SYMBOLS`, `RAW_DATA_DIR`, `ALPHA_VANTAGE_OUTPUT_SIZE`, `LOG_LEVEL`, `HTTP_TIMEOUT_SECONDS`, `MAX_RETRIES`, and `RETRY_BASE_DELAY_SECONDS`.
- **FR17-AC2:** `.env.example` is committed, contains the operator-facing variables needed to run the pipeline with placeholder values, and contains no real credentials.
- **FR17-AC3:** `.env` is excluded by `.gitignore` and is not tracked by Git.
- **FR17-AC4:** Invalid configuration — unsupported `DATA_SOURCE`, non-integer or out-of-range `DB_PORT`, non-positive `HTTP_TIMEOUT_SECONDS`, negative `MAX_RETRIES` or `RETRY_BASE_DELAY_SECONDS`, placeholder `DB_PASSWORD`, or missing/placeholder `ALPHA_VANTAGE_API_KEY` for Alpha Vantage — produces a clear `ValueError` or `RuntimeError` rather than a silent fallback. `RETRY_BASE_DELAY_SECONDS` has a code default and is not currently listed in `.env.example`.

#### FR18 — Central logging module used by pipeline stages

- **FR18-AC1:** `utils/logger.py:get_logger()` returns a logger in the `financial_pipeline` hierarchy and attaches exactly one stdout handler and one file handler (`logs/pipeline.log`) on first use; repeated calls do not duplicate handlers.
- **FR18-AC2:** `LOG_LEVEL` (default `INFO`) controls the logger level, and log lines include timestamp, level, logger name, and message.
- **FR18-AC3:** Ingestion, transform, database, and pipeline modules use `get_logger()` rather than `print()` or ad-hoc logger configuration.
- **FR18-AC4:** Stage logs at `INFO` include row/symbol counts, and failures log at `ERROR` with exception text. Airflow captures the same stdout in its task logs.

#### FR19 — Docker Compose for app + Postgres (optional)

- **FR19-AC1:** If implemented, `docker compose up` starts a PostgreSQL 15 service and a pipeline service, with database readiness checked before the pipeline runs.
- **FR19-AC2:** The pipeline service connects to the Compose database using environment variables, applies `schema.sql` and `views.sql`, and completes `python pipeline.py` without changing application code.
- **FR19-AC3:** README documents the Docker setup and credentials are supplied through `.env`, not committed in Compose files.
- **FR19-AC4 — Needs stakeholder decision:** This item is optional. Decide whether it is included in the 4 December submission. If not selected, it remains out of the must-have acceptance gate.

#### FR20 — MongoDB raw document store (optional)

- **FR20-A — Needs stakeholder decision:** FR20 is optional and has no implementation in the repository. Decide whether it remains in project scope. If it does, the team must define the collection schema, whether every raw vendor JSON is mirrored, retention policy, and how MongoDB availability affects run status before measurable acceptance criteria can be set. Until then FR20 is not part of the must-have acceptance gate.

---

## 6. Non-functional requirements

| ID | Category | Requirement |
|----|----------|-------------|
| NFR1 | Reliability | DAG retries (`retries=3`, 5-minute delay); failed runs visible in UI and `pipeline_runs`. |
| NFR2 | Data integrity | Unique `(symbol, trade_date)`; transformed row implies raw row. |
| NFR3 | Security | No secrets in Git; DB user is least-privilege for local student use. |
| NFR4 | Performance | Incremental weekday run for 8 symbols completes in < 10 minutes locally (see KPIs). Respect API quotas. |
| NFR5 | Maintainability | Modular packages (`ingestion`, `transform`, `database`, `dags`); docstrings; README setup. |
| NFR6 | Reproducibility | Python 3.10+, pinned `requirements.txt`, schema scripts. |
| NFR7 | Observability | Logs at INFO for counts; ERROR with exception text; Airflow task logs. |
| NFR8 | Usability | Operator uses Airflow UI + pgAdmin/psql + Jupyter; no custom web app required. |
| NFR9 | Portability | Linux/macOS/Windows documented; paths via config, not hardcoded user homes. |
| NFR10 | Compliance (academic) | Fixed GitHub URL; role split visible in docs and presentation. |

### 6.1 Acceptance criteria — non-functional requirements

#### NFR1 — Reliability — DAG retries (`retries=3`, 5-minute delay); failed runs visible in UI and `pipeline_runs`

- **NFR1-AC1:** The DAG's `default_args` set `retries=3`, `retry_delay=timedelta(minutes=5)`, and `depends_on_past=False`; these settings apply to all five tasks.
- **NFR1-AC2:** A task that raises is retried at most three times. A final failure is visible as a failed task in the Airflow UI and in Airflow task logs.
- **NFR1-AC3:** A CLI run that raises records `status='failed'` and the exception text in `pipeline_runs`; a `NO_NEW_DATA` run records `status='success'` with zero rows as specified in FR12.
- **NFR1-AC4 — Needs stakeholder decision:** “Failed runs visible in UI **and** `pipeline_runs`” is not met by either execution path as a whole: Airflow shows UI/task state but does not write `pipeline_runs`, while the CLI writes `pipeline_runs` but has no Airflow UI state. Decide whether Airflow-triggered runs must also update `pipeline_runs` (see FR12-AC4), or whether this NFR is satisfied by UI state for Airflow runs and `pipeline_runs` for CLI runs.

#### NFR2 — Data integrity — Unique `(symbol, trade_date)`; transformed row implies raw row

- **NFR2-AC1:** The schema enforces `UNIQUE (symbol, trade_date)` on both fact tables, `volume >= 0`, and `high_price >= low_price` on raw rows.
- **NFR2-AC2:** `run_quality_checks()` raises `ValueError` when the duplicate raw-key count is greater than 0 or the orphan transformed-row count is greater than 0; it returns the counts when both are 0.
- **NFR2-AC3:** After a successful quality check, these queries return 0:
  - `SELECT COUNT(*) FROM (SELECT symbol, trade_date FROM raw_market_data GROUP BY symbol, trade_date HAVING COUNT(*) > 1) d;`
  - `SELECT COUNT(*) FROM transformed_market_data t LEFT JOIN raw_market_data r ON r.symbol = t.symbol AND r.trade_date = t.trade_date WHERE r.id IS NULL;`
- **NFR2-AC4:** Raw rows are loaded before transformed rows. Detection happens after load; a failed quality check does not roll back rows already committed (documented limitation in [08 §8](./08_system_analysis_and_design.md#8-error-handling-policy)).

#### NFR3 — Security — No secrets in Git; DB user is least-privilege for local student use

- **NFR3-AC1:** `git ls-files` contains no `.env`, API key, database password, private key, or PEM file. `.env` is matched by `.gitignore`, and `.env.example` contains placeholders only.
- **NFR3-AC2:** Automated tests confirm that a configured Alpha Vantage key is not printed in logs or error messages, and that placeholder/empty database passwords are rejected without echoing the password.
- **NFR3-AC3 — Needs stakeholder decision:** “Least-privilege for local student use” has no defined role or grant set in the project. Current defaults and README use the PostgreSQL `postgres` superuser. Decide whether that is acceptable for local student deployment or require a dedicated application role with only `CONNECT`, `USAGE` on the schema, and `SELECT`/`INSERT`/`UPDATE` on the three tables (no `SUPERUSER`, `CREATEDB`, or `CREATEROLE`). Once decided, verify with `\du` and `information_schema.role_table_grants`.

#### NFR4 — Performance — Incremental weekday run for 8 symbols completes in < 10 minutes locally; respect API quotas

- **NFR4-AC1:** On a recorded reference machine, an incremental weekday run for the 8 default symbols completes extract → validate → transform → load → quality_check in less than 600 seconds, measured by `pipeline_runs.duration_seconds` or by the CLI wall-clock result. The evidence must record the date, hardware, data source, and whether the run was a first backfill or incremental.
- **NFR4-AC2:** For `DATA_SOURCE=alphavantage`, a full refresh of 8 symbols with `outputsize=compact` sends at most 8 successful requests when no retries occur; each symbol is limited to at most `MAX_RETRIES` attempts by FR4. The total Alpha Vantage requests in a calendar day must remain at or below the free-tier limit of 25.
- **NFR4-AC3:** The default development configuration uses `DATA_SOURCE=yfinance`, which consumes no Alpha Vantage quota. Alpha Vantage is used only for the official API demonstration or a limited symbol set.
- **NFR4-AC4 — Needs stakeholder decision:** Name the reference laptop/VM and its specifications for the < 10-minute sign-off, and decide whether Alpha Vantage quota compliance must be enforced in code (for example, a request counter or guard) or is an operator procedure verified from provider logs. No duration measurement or quota guard currently exists.

#### NFR5 — Maintainability — Modular packages (`ingestion`, `transform`, `database`, `dags`); docstrings; README setup

- **NFR5-AC1:** The repository contains the packages `ingestion/`, `transform/`, `database/`, `dags/`, `config/`, and `utils/`, and each module has a single documented responsibility matching [08 §9](./08_system_analysis_and_design.md#9-component--class-view).
- **NFR5-AC2:** Public modules, functions, and classes have docstrings describing purpose and important behavior. Pipeline orchestration lives in `pipeline.py`; stage logic is not duplicated across the DAG and modules.
- **NFR5-AC3:** `README.md` documents project structure, setup, environment configuration, database schema, manual run command, and test command.

#### NFR6 — Reproducibility — Python 3.10+, pinned `requirements.txt`, schema scripts

- **NFR6-AC1:** `README.md` states Python 3.10+ and gives commands for virtual environment creation, dependency installation, database creation, and schema/views application.
- **NFR6-AC2:** `requirements.txt` pins the core runtime dependencies to exact versions (`pandas`, `numpy`, `requests`, `python-dotenv`, `SQLAlchemy`, `psycopg2-binary`, `apache-airflow`, `pytest`), and `database/schema.sql` plus `database/views.sql` are present and applied in that order.
- **NFR6-AC3:** A fresh clone can import `config.settings` and run the unit tests without a database connection; a documented setup dry run can create `financial_db` and execute `python pipeline.py`. Any deviation found during the dry run must be corrected in the README, not worked around silently.

#### NFR7 — Observability — Logs at INFO for counts; ERROR with exception text; Airflow task logs

- **NFR7-AC1:** Every pipeline stage logs through `utils/logger.py`. `INFO` records include row counts, symbol counts, and stage transitions; `ERROR` records include the exception message for failed symbols or stages.
- **NFR7-AC2:** Logs are written to stdout and to `logs/pipeline.log` in a timestamped `timestamp | level | logger | message` format, controlled by `LOG_LEVEL`.
- **NFR7-AC3:** Airflow task logs contain the same stage output for each task attempt. `pipeline_runs.error_message` holds the exception text for failed CLI runs.

#### NFR8 — Usability — Operator uses Airflow UI + pgAdmin/psql + Jupyter; no custom web app required

- **NFR8-AC1:** There is no custom web or mobile application in the repository; operator surfaces are the Airflow UI, `psql`/pgAdmin, and Jupyter.
- **NFR8-AC2:** README explains how to start Airflow (`localhost:8080`), run `psql`/apply SQL, and run tests. The DAG can be triggered and monitored from the Airflow UI, and analysts can run the SQL in `database/analytics.sql`.
- **NFR8-AC3:** Jupyter notebooks for quality inspection and charting exist and open against `financial_db` (currently missing; see FR16).

#### NFR9 — Portability — Linux/macOS/Windows documented; paths via config, not hardcoded user homes

- **NFR9-AC1:** `README.md` documents virtual-environment activation for Linux/macOS and Windows.
- **NFR9-AC2:** Paths are derived with `pathlib` from `PROJECT_ROOT` or from environment variables (`RAW_DATA_DIR`); no source file hard-codes `/home/<user>`, `C:\Users\<user>`, or another absolute user home.
- **NFR9-AC3:** Database location, port, name, user, and password come from `DB_*` environment variables, and data-source/symbol configuration comes from environment variables with documented defaults.

#### NFR10 — Compliance (academic) — Fixed GitHub URL; role split visible in docs and presentation

- **NFR10-AC1:** The repository has one fixed GitHub remote (`git remote -v` shows the project origin) and the documentation instructs the team not to recreate or rename the repository.
- **NFR10-AC2:** The team table in `README.md` and [03_task_assignment_and_roles.md](./03_task_assignment_and_roles.md) lists all four members, their student IDs, and their roles; the RACI table shows accountability by work item.
- **NFR10-AC3:** The final presentation and rehearsal materials show each member presenting their own module, consistent with the KPI of ≤ 3 minutes per member. These materials are not yet in the repository.
- **NFR10-AC4:** The README clone command still contains the placeholder `YOUR_USERNAME`; the official repository URL must replace it before final submission.

---

## 7. Constraints and assumptions

- US equity **daily** bars only; no intraday.
- Free-tier APIs; development must not depend on paid keys.
- Team of four; local or single shared Postgres, not a cloud production account.
- Evaluation is a 15-minute presentation + GitHub + ZIP, not 24/7 hosting.
- Column names in the database follow the README schema (`open_price`, `trade_date`, …).

---

## 8. Traceability (story → requirement)

| Stories | Requirements |
|---------|----------------|
| US1 | FR13 |
| US2 | FR1, FR6 |
| US3 | FR8, FR9 |
| US4 | FR3, FR4 |
| US5, US7 | FR10, FR11 |
| US6 | FR12, NFR1, NFR7 |
| US8 | FR14 |
| US9 | FR15, FR16 |
| US10 | FR17, NFR3 |

---

## 9. Requirements traceability

Status definitions:

- **Implemented** — the repository contains code, configuration, or documentation that satisfies the acceptance criteria at source level.
- **Partially Implemented** — some criteria are met, but a listed gap remains.
- **Not Implemented** — no implementing artifact exists.
- **Not Yet Verified** — an implementing artifact may exist, but the measurable acceptance criterion requires a run or measurement that has not been performed.

| Requirement ID | Requirement title | Acceptance criteria reference | Verification method | Current implementation status |
|----------------|-------------------|-------------------------------|---------------------|-------------------------------|
| FR1 | Fetch daily OHLCV for the configured symbol list | §5.1 FR1-AC1–FR1-AC3 | Test (`tests/test_extraction_pipeline_failure.py`) + inspection (`ingestion/extractor.py`) | Implemented |
| FR2 | Support Alpha Vantage REST as the official API; yfinance as the development source | §5.1 FR2-AC1–FR2-AC3 | Inspection (`ingestion/api_client.py`, `config/settings.py`) + test (`tests/test_config_validation.py`) | Implemented |
| FR3 | Validate HTTP status and required JSON fields before parse | §5.1 FR3-AC1–FR3-AC4 | Test (`tests/test_api_client.py`) + inspection | Implemented |
| FR4 | Retry transient failures (max 3, exponential backoff) | §5.1 FR4-AC1–FR4-AC4 | Inspection (`ingestion/api_client.py:_retry_call`) + manual fake-call check | Implemented |
| FR5 | Persist raw vendor JSON under `raw_data/` | §5.1 FR5-AC1–FR5-AC3 | Inspection (`save_raw_json`, `.gitignore`) + manual filesystem check | Implemented |
| FR6 | Persist raw structured rows in `raw_market_data` | §5.1 FR6-AC1–FR6-AC3 | SQL query on `raw_market_data` + inspection (`database/loader.py`) | Implemented |
| FR7 | Clean: types, duplicate `(symbol, date)`, price forward-fill policy | §5.1 FR7-AC1–FR7-AC4 | Test (`tests/test_cleaner.py`) + inspection (`transform/cleaner.py`) | Implemented |
| FR8 | Compute daily return, SMA 7/20/50, volume ratio, cumulative return, 20d volatility | §5.1 FR8-AC1–FR8-AC6 | Test (`tests/test_calculator.py`, `tests/test_staged_metrics.py`) + SQL query | Implemented |
| FR9 | Persist metrics in `transformed_market_data` | §5.1 FR9-AC1–FR9-AC3 | SQL query on `transformed_market_data` + test (`tests/test_staged_metrics.py`) | Implemented |
| FR10 | Incremental extract using last loaded date per symbol | §5.1 FR10-AC1–FR10-AC4 | SQL query (`MAX(trade_date)`) + inspection (`ingestion/incremental.py`) + manual log check | Implemented |
| FR11 | Idempotent load (no duplicate business keys) | §5.1 FR11-AC1–FR11-AC3 | SQL duplicate-key query + inspection (`database/schema.sql`, `database/loader.py`) | Implemented |
| FR12 | Log each run in `pipeline_runs` | §5.1 FR12-AC1–FR12-AC4 | Test (`tests/test_extraction_pipeline_failure.py`) + SQL query on `pipeline_runs` + inspection (`pipeline.py`) | Partially Implemented |
| FR13 | Airflow DAG with five tasks and weekday 18:00 schedule | §5.1 FR13-AC1–FR13-AC3 | Inspection (`dags/financial_pipeline_dag.py`) + manual Airflow UI check | Implemented |
| FR14 | ≥ 5 analytical SQL queries (windows + CTEs) and reusable views | §5.1 FR14-AC1–FR14-AC3 | Inspection (`database/analytics.sql`, `database/views.sql`) + SQL query execution | Implemented |
| FR15 | Unit tests for client, cleaner, calculator | §5.1 FR15-AC1–FR15-AC3 | Test (`pytest tests/ -v`; 42 passed) | Implemented |
| FR16 | Data quality report + time-series charts | §5.1 FR16-AC1–FR16-AC4 | Inspection (`notebooks/` is empty) + manual check after implementation | Not Implemented |
| FR17 | Configuration via environment variables (`.env.example`) | §5.1 FR17-AC1–FR17-AC4 | Test (`tests/test_config_validation.py`) + inspection (`.env.example`, `.gitignore`); `git check-ignore .env` | Partially Implemented |
| FR18 | Central logging module used by pipeline stages | §5.1 FR18-AC1–FR18-AC4 | Inspection (`utils/logger.py`, module imports) + manual check of `logs/pipeline.log` | Implemented |
| FR19 | Docker Compose for app + Postgres (optional) | §5.1 FR19-AC1–FR19-AC4 | Inspection (no Compose file present) + manual check if selected | Not Implemented |
| FR20 | MongoDB raw document store (optional) | §5.1 FR20-A (stakeholder decision) | Inspection (no MongoDB code or dependency) + stakeholder decision | Not Implemented |
| NFR1 | Reliability — DAG retries (`retries=3`, 5-minute delay); failed runs visible in UI and `pipeline_runs` | §6.1 NFR1-AC1–NFR1-AC4 | Inspection (`dags/`), SQL query on `pipeline_runs`, manual Airflow UI check | Partially Implemented |
| NFR2 | Data integrity — Unique `(symbol, trade_date)`; transformed row implies raw row | §6.1 NFR2-AC1–NFR2-AC4 | SQL duplicate/orphan queries + inspection (`database/schema.sql`, `transform/quality.py`) | Implemented |
| NFR3 | Security — No secrets in Git; DB user is least-privilege for local student use | §6.1 NFR3-AC1–NFR3-AC3 | Inspection (`git ls-files`, `.gitignore`, docs) + test (`tests/test_config_validation.py`) + manual role check | Partially Implemented |
| NFR4 | Performance — Incremental weekday run for 8 symbols completes in < 10 minutes locally; respect API quotas | §6.1 NFR4-AC1–NFR4-AC4 | SQL query on `pipeline_runs.duration_seconds` + timed manual run + provider request count | Not Yet Verified |
| NFR5 | Maintainability — Modular packages (`ingestion`, `transform`, `database`, `dags`); docstrings; README setup | §6.1 NFR5-AC1–NFR5-AC3 | Inspection of package layout, docstrings, and `README.md` | Implemented |
| NFR6 | Reproducibility — Python 3.10+, pinned `requirements.txt`, schema scripts | §6.1 NFR6-AC1–NFR6-AC3 | Inspection + test (`pytest tests/ -v`) + documented fresh-clone dry run | Implemented |
| NFR7 | Observability — Logs at INFO for counts; ERROR with exception text; Airflow task logs | §6.1 NFR7-AC1–NFR7-AC3 | Inspection (`utils/logger.py`) + manual check of `logs/pipeline.log` and Airflow task logs | Implemented |
| NFR8 | Usability — Operator uses Airflow UI + pgAdmin/psql + Jupyter; no custom web app required | §6.1 NFR8-AC1–NFR8-AC3 | Documented manual check (Airflow UI, `psql`, Jupyter) + inspection (no custom web app; notebooks absent) | Partially Implemented |
| NFR9 | Portability — Linux/macOS/Windows documented; paths via config, not hardcoded user homes | §6.1 NFR9-AC1–NFR9-AC3 | Inspection (`config/settings.py`, `README.md`) + documented manual check on target OS | Implemented |
| NFR10 | Compliance (academic) — Fixed GitHub URL; role split visible in docs and presentation | §6.1 NFR10-AC1–NFR10-AC4 | Inspection (`git remote -v`, `docs/03_task_assignment_and_roles.md`, `README.md`) + manual presentation check | Partially Implemented |

### 9.1 Implementation status summary

Each requirement appears in exactly one category below; the counts sum to 30.

| Status | Count | Requirements |
|--------|-------|--------------|
| Implemented | 20 | FR1, FR2, FR3, FR4, FR5, FR6, FR7, FR8, FR9, FR10, FR11, FR13, FR14, FR15, FR18, NFR2, NFR5, NFR6, NFR7, NFR9 |
| Partially Implemented | 6 | FR12, FR17, NFR1, NFR3, NFR8, NFR10 |
| Not Implemented | 3 | FR16, FR19, FR20 |
| Not Yet Verified | 1 | NFR4 |
| **Total** | **30** | |

### 9.2 Gap summary

| Requirement | Gap / evidence |
|-------------|----------------|
| FR12, NFR1 | Airflow invokes task callables directly and does not write `pipeline_runs`; the audit table is written only by the CLI `run_pipeline()` path. A stakeholder decision is needed on whether Airflow runs must also be audited there. |
| FR16 | `notebooks/` is empty; no data-quality notebook, time-series notebook, or exported chart exists. |
| FR17 | The working tree contains `.env.example`, but it is untracked; the tracked path is still the legacy `env.example`, which is deleted in the working tree. Committing or staging the rename is outside this documentation-only task. |
| FR19 | No `docker-compose.yml`/`Dockerfile` exists; optional item, decision needed on inclusion. |
| FR20 | No MongoDB client, collection, or dependency is implemented; optional item, decision needed on scope and schema. |
| NFR3 | Secrets controls exist, but the README and defaults use the PostgreSQL `postgres` superuser; no least-privilege application role or grant set is defined. |
| NFR4 | No measured < 10-minute run is recorded, no reference hardware is named, and there is no automated Alpha Vantage quota guard. |
| NFR8 | The Jupyter part of the operator workflow cannot be exercised because the notebooks are absent. |
| NFR10 | Role split is documented, but the README clone URL still contains `YOUR_USERNAME` and the presentation/rehearsal materials do not exist yet. |

### 9.3 Evidence used for this review

- Source inspection of `pipeline.py`, `ingestion/`, `transform/`, `database/`, `dags/`, `config/settings.py`, and `utils/logger.py`.
- `pytest tests/ -v` → **42 passed** (run on 5 November 2026; no database or network required).
- `git remote -v` → project origin exists; `git check-ignore .env` confirms `.env` is ignored; `git ls-files` shows the legacy `env.example` is tracked while `.env.example` is currently untracked in the working tree.
- `README.md`, `docs/08_system_analysis_and_design.md`, `docs/09_api_research.md`, `docs/04_risk_assessment.md`, and `docs/05_kpis.md` for architecture, failure policy, API behavior, and thresholds.
