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
| US10 | As a **developer**, I want secrets out of Git. | `.env` gitignored; `env.example` committed. |

---

## 4. Use cases (brief)

### UC-01 Nightly incremental load

1. Scheduler triggers `financial_pipeline_dag`.
2. System reads last loaded date per symbol.
3. System calls API for missing dates.
4. System validates HTTP + payload.
5. System writes JSON files and inserts new raw rows.
6. System cleans, calculates metrics, loads transformed table.
7. System writes quality metrics and `pipeline_runs = success`.
**Exception:** API down after retries → run `failed`, error stored, no partial silent success without a log.

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
| FR17 | Configuration via environment variables (`env.example`) | Must |
| FR18 | Central logging module used by pipeline stages | Should |
| FR19 | Docker Compose for app + Postgres | Optional |
| FR20 | MongoDB raw document store | Optional |

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
