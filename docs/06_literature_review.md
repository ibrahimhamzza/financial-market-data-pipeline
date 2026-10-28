# Literature Review

**Course:** DEPI Data Engineering — Graduation Project
**Topic:** Automated ingestion, quality control, and orchestration of daily equity market data
**Date:** 26 October 2026

This review positions the project against standard data-engineering practice. It is **not** a finance-trading paper: we study how market data is *produced as a reliable dataset*, not how to predict prices.

---

## 1. ETL / ELT pipelines

Kimball’s warehouse lifecycle still describes the core pattern: extract from sources, conform data, load into a queryable store, then serve analytics (Kimball & Ross, *The Data Warehouse Toolkit*). Modern stacks often land raw data first (**ELT**): keep an immutable raw layer, then transform. Our design follows that split:

- `raw_market_data` + JSON files in `raw_data/` = landing zone
- `transformed_market_data` = conformed metrics for analysts

That matches the “raw → curated” lakehouse idea without requiring Spark or cloud object storage, which is appropriate for a four-person academic pipeline.

---

## 2. Orchestration

Cron scripts hide failures and have no visible graph of dependencies. **Apache Airflow** models work as a DAG with retries, logs, and a UI—now a default skill in data-engineering job descriptions (Apache Software Foundation, Airflow documentation). For this project, Airflow is the required orchestrator: weekday schedule after market close, `retries=3`, and a recorded run status. We treat Airflow as the *control plane*; Pandas and PostgreSQL remain the *data plane*.

---

## 3. Financial data APIs

Commercial market-data vendors (Bloomberg, Refinitiv) are out of reach. Teaching and retail projects typically use:

| Source | Strength | Constraint |
|--------|----------|------------|
| **Alpha Vantage** | REST API, documented daily time series, fits “consume an API” rubrics | Free tier ~25 requests/day |
| **Yahoo Finance via yfinance** | Practical for backfill and tests | Unofficial; not a contractual SLA |
| **Polygon.io** | Clean JSON, rate limits on free tier | Extra key to manage |

Published API docs (Alpha Vantage `TIME_SERIES_DAILY`) confirm OHLCV bars keyed by date—enough for daily analytics. Literature and practitioner blogs consistently warn about **rate limits, symbol delistings, and split/dividend adjustments**. We store both raw vendor JSON and a cleaned table so adjustments can be re-run later.

---

## 4. Data quality

Pipelines fail silently when they load “something.” Wang & Strong’s data-quality dimensions (accuracy, completeness, consistency, timeliness) are still the usual checklist. In market data that means:

- OHLC present and `high ≥ low`, `high ≥ open/close`
- No duplicate trading days per symbol
- Volume non-negative
- Timeliness: last bar not older than the previous session after a successful weekday run

We will report these in a Jupyter **data quality report**, which is a DEPI required deliverable and aligns with industry “data contracts” practice (even if we do not implement a full contract tool such as Great Expectations).

---

## 5. Incremental loading

Re-downloading full history every night wastes quota and risks duplicates. Warehouse practice uses a **high-watermark** (last successful date per key) and idempotent merges (Kimball incremental loads; SQL `UNIQUE` + skip or `UPSERT`). Our `incremental.py` + `pipeline_runs` table is a student-scale version of that pattern.

---

## 6. Analytics layer (SQL window functions)

PostgreSQL window functions (`LAG`, `RANK`, `AVG() OVER (PARTITION BY …)`) are the standard way to compute returns, ranking, and moving averages **in the database** after Pandas has written a clean fact table. This satisfies the DEPI SQL analytics requirement and matches how analysts query a warehouse without exporting CSVs.

---

## 7. Gap this project fills

Coursework often stops at “download a CSV and plot it.” Production data engineering needs **repeatability, validation, and scheduling**. The gap we address:

> There is no small, documented, Airflow-orchestrated pipeline that lands vendor equity bars into PostgreSQL with a raw/curated split, incremental loads, tests, and a quality report—using only free-tier tools a DEPI student can install locally.

---

## 8. Suggested improvements (after the graded MVP)

These are **not** in the 4 December scope; they show we know the next professional step:

1. Swap file JSON for an object store (S3-compatible) and partition by date.
2. Add Great Expectations or dbt tests in CI.
3. Corporate-action-aware prices (splits/dividends) from a dedicated endpoint.
4. Containerize Postgres + Airflow for one-command demos.
5. Optional MongoDB collection of raw payloads (already listed as extra credit in the internal plan).

---

## 9. Lecturer feedback & evaluation

*To be filled after the instructor reviews this document (DEPI “Literature Review — Feedback & Evaluation”).*

| Date | Mentor | Summary of feedback | Action taken |
|------|--------|---------------------|--------------|
| | | | |

---

## References

1. Kimball, R. & Ross, M. *The Data Warehouse Toolkit* (3rd ed.). Wiley.
2. Apache Airflow documentation. https://airflow.apache.org/docs/
3. Alpha Vantage API documentation. https://www.alphavantage.co/documentation/
4. yfinance (Ran Aroussi). https://github.com/ranaroussi/yfinance
5. PostgreSQL documentation — Window Functions. https://www.postgresql.org/docs/current/tutorial-window.html
6. Wang, R. Y. & Strong, D. M. (1996). Beyond accuracy: What data quality means to data consumers. *Journal of Management Information Systems*.
7. Pandas documentation — Time series / `rolling` / `pct_change`. https://pandas.pydata.org/docs/
8. DEPI Graduation Project Playbook and Project Documentation Guidelines (program materials, 2026).
