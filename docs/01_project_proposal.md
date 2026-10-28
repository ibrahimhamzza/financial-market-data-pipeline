# Project Proposal

**Project title:** Automated Financial Market Data Pipeline
**Program:** Digital Egypt Pioneers Initiative (DEPI) — Data Engineering Track
**Team leader:** Ibrahim Sayed Muhammed (21118171)
**Date:** 26 October 2026

---

## 1. Problem Statement

Financial analysts and data teams need historical and regularly updated stock-market data (open, high, low, close, volume). That data usually arrives from external REST APIs in inconsistent form: missing values, unstandardized timestamps, duplicate bars, rate-limit errors, and payloads that change between providers.

Without an automated pipeline, the work is manual (download, clean, recalculate metrics, paste into a database). That process is slow, not repeatable, and easy to get wrong. The team needs a **dependable, scheduled ETL pipeline** that retrieves, validates, cleans, transforms, and stores market data so it can be queried and visualized for time-series analysis and performance reporting.

---

## 2. Objectives

1. Build a scheduled pipeline that fetches daily OHLCV data for a fixed set of stock symbols from a financial REST API.
2. Validate HTTP responses and JSON payloads, and recover from transient failures with retries and logging.
3. Clean and standardize raw data: timestamps, nulls, types, and duplicates.
4. Calculate financial metrics: daily returns, 7/20/50-day moving averages, volume statistics, cumulative return, and rolling volatility.
5. Store **raw** and **transformed** data in separate PostgreSQL tables, with incremental loads (only new dates per symbol).
6. Orchestrate the workflow with Apache Airflow (retries, dependencies, run logging).
7. Provide analytical SQL (window functions and CTEs) for market questions.
8. Produce time-series visualizations and a data-quality report.

---

## 3. Scope

**In scope**

- Daily data for eight symbols: `AAPL`, `MSFT`, `GOOGL`, `AMZN`, `META`, `TSLA`, `NVDA`, `JPM` (configurable).
- Extract → validate → transform → load, plus quality checks.
- PostgreSQL tables: `raw_market_data`, `transformed_market_data`, `pipeline_runs`.
- Airflow DAG scheduled on weekdays after US market close (`0 18 * * 1-5`).
- Unit tests, SQL analytics, Jupyter visualizations, project documentation.

**Out of scope**

- Real-time tick data or trading/order execution.
- A public web/mobile product or Figma consumer UI.
- Portfolio optimization, ML price prediction, or brokerage integration.
- Production multi-region deployment (optional Docker is an extension only).

---

## 4. Methodology / Approach

| Stage | What we do |
|-------|------------|
| **Extract** | Call Alpha Vantage (`TIME_SERIES_DAILY`) as the official API requirement; use **yfinance** during development (no daily quota). |
| **Validate** | Check HTTP status, non-empty body, and required JSON fields before parsing. |
| **Transform** | Pandas: forward-fill prices, drop duplicate `(symbol, date)`, cast types, compute metrics. |
| **Load** | Write raw JSON under `raw_data/`; insert structured rows via SQLAlchemy. Skip rows already stored. |
| **Analyze** | SQL window functions (`LAG`, `RANK`, `AVG() OVER`) and CTEs; SQL views for common queries. |
| **Orchestrate** | Airflow DAG: extract → validate → transform → load → quality_check. |
| **Visualize** | Matplotlib / Plotly in Jupyter: price + MAs, volume, returns. |

---

## 5. Expected Outcomes

- An automated Airflow DAG that can run end-to-end on a weekday schedule.
- A PostgreSQL database with raw OHLCV, derived metrics, and an audit table of pipeline runs.
- Metrics available per symbol and date for analysis.
- A data-quality report (null rates, duplicates, completeness per symbol).
- Charts of price, moving averages, and volume.
- A GitHub repository with a README that another student can use to reproduce the setup.

---

## 6. Tools & Technologies

| Layer | Technology |
|-------|------------|
| Language | Python 3.10+ |
| Processing | Pandas, NumPy |
| APIs | Alpha Vantage, yfinance, `requests` |
| Database | PostgreSQL 15, SQLAlchemy, psycopg2 |
| Orchestration | Apache Airflow 2.x |
| Visualization | Matplotlib, Plotly, Jupyter |
| Testing | pytest |
| Version control | Git + GitHub |
| Optional | Docker Compose, MongoDB (raw JSON store) |

---

## 7. Team

| Member | Name | Student ID | Role |
|--------|------|------------|------|
| A (Leader) | Ibrahim Sayed Muhammed | 21118171 | Infrastructure & Airflow Engineer |
| B | Ali Khaled Fawzy Mansour | 21060005 | API & Data Ingestion Engineer |
| C | Bavly Osam Geedy Arean | 21076897 | Database & SQL Analytics Engineer |
| D | Emmanuel George Samy Riad | 21171868 | QA, Visualization & Documentation |

Emails will be added in [03_task_assignment_and_roles.md](./03_task_assignment_and_roles.md).

---

## 8. Milestones (aligned to DEPI dates)

| Checkpoint | DEPI date | Target outcome |
|------------|-----------|----------------|
| Planning, literature, requirements | 16 Oct 2026 | This `docs/` pack on GitHub |
| System analysis & design | 6 Nov 2026 | Architecture diagram, ERD, schema SQL, API notes |
| Implementation | 30 Nov 2026 | Working ingestion, transform, load, Airflow DAG |
| Presentation, testing & reports | 4 Dec 2026 | Tests, quality report, DEPI slides, ZIP + GitHub |

Detailed weekly work is in [02_project_plan.md](./02_project_plan.md).
