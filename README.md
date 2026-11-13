# 📈 Financial Market Data Pipeline

> **DEPI Graduation Project — Data Engineering Track**
> A batch ETL pipeline that collects daily stock market data from Alpha Vantage’s official REST API or the third-party yfinance library, validates and transforms it, and loads it into PostgreSQL. Apache Airflow is the intended scheduler; the same task functions can also be run from the command line. See **Project Status** below for what is implemented and what remains unverified.

---

## 👥 Team

| Member | Name | Role
|--------|------|------
| A (Leader) | Ibrahim Sayed Muhammed | Infrastructure & Airflow Engineer
| B | Ali Khaled Fawzy Mansour | API & Data Ingestion Engineer
| C | Bavly Osam Geedy Arean | Database & SQL Analytics Engineer
| D | Emmanuel George Samy Riad | QA, Visualization & Documentation

---

## 🗂️ Project Structure

```
financial-market-data-pipeline/
│
├── ingestion/                   # Data extraction from API
│   ├── __init__.py
│   ├── api_client.py            # HTTP requests, validation, retry logic, raw JSON save
│   ├── extractor.py             # Loops over stock symbols, calls api_client
│   ├── incremental.py          # Tracks last loaded date per symbol
│   └── market_calendar.py      # XNYS trading-calendar session gate (M2.1)
│
├── transform/                   # Data cleaning & financial calculations
│   ├── __init__.py
│   ├── cleaner.py               # Null handling, deduplication, type casting
│   ├── calculator.py            # Daily returns, moving averages, volume stats
│   ├── staged_metrics.py        # Metrics for the staging batch using stored history
│   └── quality.py               # Post-load database quality checks
│
├── database/                    # PostgreSQL interaction
│   ├── __init__.py
│   ├── db_connector.py         # SQLAlchemy engine (reads from .env)
│   ├── loader.py               # Inserts DataFrames into DB tables
│   ├── schema.sql              # CREATE TABLE, constraint, and index scripts (run once)
│   ├── analytics.sql           # Optional analytical queries with window functions & CTEs
│   └── views.sql               # CREATE VIEW scripts
│
├── dags/                        # Apache Airflow DAG definition
│   └── financial_pipeline_dag.py
│
├── tests/                       # Offline unit tests (pytest)
│   ├── __init__.py
│   ├── test_api_client.py
│   ├── test_cleaner.py
│   ├── test_calculator.py
│   ├── test_config_validation.py
│   ├── test_extraction_pipeline_failure.py
│   ├── test_extractor_calendar.py   # Calendar-gating behavior (M2.1)
│   ├── test_incremental.py          # Incremental watermark logic (M2.1)
│   ├── test_market_calendar.py      # XNYS calendar helpers (M2.1)
│   └── test_staged_metrics.py
│
├── notebooks/                   # Planned visualization work (currently empty; not tracked by Git)
│
├── utils/                       # Shared utilities
│   ├── __init__.py
│   └── logger.py               # Centralized logging setup
│
├── config/                      # Configuration
│   ├── __init__.py
│   └── settings.py             # Loads environment variables
│
├── raw_data/                    # Runtime data directory (generated files are gitignored)
│   ├── SYMBOL_<UTC timestamp>.json  # Write-only vendor JSON landing files
│   ├── _staging.csv             # Extract output (ephemeral)
│   ├── _metrics.csv             # Transform output (ephemeral)
│   └── .gitkeep
│
├── logs/                        # Runtime logs (gitignored)
│   └── pipeline.log
│
├── docs/                        # DEPI documentation (upload to GitHub)
│   ├── README.md                # Index + official due dates
│   ├── 01_project_proposal.md
│   ├── 02_project_plan.md
│   ├── 03_task_assignment_and_roles.md
│   ├── 04_risk_assessment.md
│   ├── 05_kpis.md
│   ├── 06_literature_review.md
│   ├── 07_requirements_gathering.md
│   ├── 08_system_analysis_and_design.md
│   ├── 09_api_research.md
│   └── diagrams/                # Mermaid sources + SVG/PNG exports
│       ├── *.mmd
│       └── exports/
│
├── pipeline.py                  # Five-task orchestration for CLI and Airflow
├── .env.example                 # Template for environment variables (safe to commit)
├── .gitignore
├── requirements.txt
└── README.md
```

**Runtime artifacts:** `raw_data/*.json`, `raw_data/_staging.csv`, `raw_data/_metrics.csv`, and `logs/` are generated at run time and ignored by Git. `raw_data/.gitkeep` is a placeholder for the directory; generated files are not committed. The JSON files are write-only vendor payload archives. `_staging.csv` and `_metrics.csv` are ephemeral batch hand-off files; when extraction reports no new data, `pipeline.extract_task()` removes stale copies.

---

## 🏗️ Architecture

The Airflow DAG defines five tasks in order: **extract → validate → transform → load → quality_check**.

**Data-source default:** `config/settings.py` uses **yfinance** when `DATA_SOURCE` is not set. yfinance is the runtime default for development and for any scheduled run that does not override it. **Alpha Vantage** is the supported official REST API path used for the DEPI “consume an API” requirement and demonstrations. Only `yfinance` and `alphavantage` are accepted by `SUPPORTED_DATA_SOURCES`; Polygon.io is not implemented. Provider details and limitations are in [docs/09_api_research.md](./docs/09_api_research.md).

![System architecture](docs/diagrams/exports/system-architecture.png)

Canonical source: [system-architecture.mmd](docs/diagrams/system-architecture.mmd) · [SVG export](docs/diagrams/exports/system-architecture.svg) · [Level-1 DFD](docs/diagrams/dfd-level-1.mmd) · [all diagram sources](docs/diagrams/README.md)

| Task | What it does | Output |
|------|--------------|--------|
| **Extract** | Reads the per-symbol watermark, fetches daily bars, validates the API HTTP response and payload, and saves the vendor JSON. | `raw_data/*.json` and `raw_data/_staging.csv` |
| **Validate** | Checks the staging CSV is non-empty, has the required OHLCV columns, and has no null `symbol` or `trade_date`. This is separate from API response validation. | Validated staging file |
| **Transform** | Combines staging with stored raw history, cleans the data, calculates metrics, and keeps only staging keys. It does not write to the database. | `raw_data/_metrics.csv` |
| **Load** | Inserts raw rows with `ON CONFLICT DO NOTHING` and upserts transformed metrics with `ON CONFLICT DO UPDATE`. | `raw_market_data` and `transformed_market_data` rows |
| **Quality Check** | Queries both tables for counts, duplicate raw keys, null closes, and orphan transformed rows. | Logged check counts; raises on duplicates or orphans |

If extraction reports no new data (every symbol returns an empty frame without error), `pipeline.extract_task()` returns `NO_NEW_DATA` and removes stale `_staging.csv` / `_metrics.csv` files. The `run_pipeline()` CLI path records the run as a success with zero rows and skips downstream tasks; Airflow still schedules the remaining tasks, which self-skip when the staging file is absent. Any symbol failure — including a partial failure — raises `ExtractionError`, so the run fails and no staging file is written; `fetch_yfinance()` treats an empty download as a symbol failure rather than no-new-data. See [docs/08_system_analysis_and_design.md](./docs/08_system_analysis_and_design.md#8-error-handling-policy) for the full policy.

**Audit boundary:** Airflow invokes the five task callables directly. The `run_pipeline()` CLI wrapper is the only path that writes `pipeline_runs` audit rows, so an Airflow-triggered run does **not** create or update that table.

---

## 🛠️ Tech Stack

“Implemented” means the module exists in this repository and is covered by offline tests where applicable. It does not mean a live database, Airflow deployment, or provider call has been verified here. Full rationale: [docs/08_system_analysis_and_design.md §14](./docs/08_system_analysis_and_design.md#14-technology-stack-rationale).

| Layer | Technology | Status in this repository |
|-------|-----------|---------------------------|
| Language | Python 3.10+ | Implemented; verified locally with Python 3.10.12; `requirements.txt` does not pin the interpreter |
| Data Processing | Pandas | Implemented |
| Numeric dependency | NumPy | Transitive dependency of Pandas; imported directly only by tests |
| HTTP / config | `requests`, `python-dotenv` | Implemented |
| Financial API | Alpha Vantage REST API (called via `requests`) | Implemented code path; requires `ALPHA_VANTAGE_API_KEY`; official API requirement |
| Financial API | yfinance | Implemented code path; runtime default; third-party, unofficial, and **not unlimited** |
| Database | PostgreSQL (14.24 available locally) + SQLAlchemy 1.4.52 + psycopg2-binary | Implemented in code; live database runtime not verified by the test suite. SQL requires PostgreSQL 9.5+ for `ON CONFLICT`; PostgreSQL 15 is not mandatory |
| Orchestration | Apache Airflow 2.8.1 | Implemented in code (DAG); not verified running here |
| Visualization | Matplotlib, Plotly, Jupyter | Listed/planned; `notebooks/` is currently empty |
| Testing | pytest | Implemented (offline unit tests; 42 passing) |
| Version Control | Git + GitHub | Implemented |
| Optional | Docker, Docker Compose, MongoDB | **Not implemented**: no Docker files; MongoDB only commented in `requirements.txt` |

**Why this stack:** Python + Pandas is sufficient for a small daily batch of eight symbols, so no distributed engine is used. PostgreSQL provides constraints, idempotent upserts, and SQL window-function analytics; SQLAlchemy + psycopg2 provide a parameterised database layer. Airflow is the scheduling/retry/visibility layer, while `pipeline.py` remains the manual CLI path. yfinance is convenient for development; Alpha Vantage is the documented REST path for the DEPI requirement. Neither source is treated as unlimited or SLA-backed. Docker and MongoDB are optional extensions, not current capabilities. The `alpha-vantage` Python package is listed in `requirements.txt` but is not imported; the code calls the REST endpoint directly.

---

## 📌 Project Status

Status reflects this repository revision. **Implemented** means the code exists and the offline test suite covers the relevant logic where applicable; it does not mean a live database, Airflow deployment, or provider call has been verified.

| Area | Status | Notes |
|------|--------|-------|
| Ingestion | Implemented in code; offline tests pass | yfinance and Alpha Vantage paths, HTTP/payload validation, retries, raw JSON archive, incremental watermark. No live provider call was made during this task. |
| Transform | Implemented in code; offline tests pass | Cleaning, returns/SMAs/volatility/volume metrics, and incremental staged-metric calculations. |
| Database layer | Implemented in code; not integration-tested | Schema, constraints, indexes, views, analytics SQL, idempotent loader, and quality checks. No populated PostgreSQL database was verified in this task. |
| CLI orchestration | Implemented in code; failure and no-new-data paths unit-tested | `pipeline.py` runs extract → validate → transform → load → quality_check and writes `pipeline_runs` audit rows. |
| Airflow DAG | Source present; not verified running | The DAG defines the same five tasks with retries and a weekday schedule. It calls the task functions directly, so it does not write `pipeline_runs` rows. |
| Testing | 42 offline tests pass | Unit tests use mocks/stubs and do not require a live database or network. See **Running Tests**. |
| Visualization | Planned/optional | Matplotlib, Plotly, and Jupyter are listed in `requirements.txt`, but `notebooks/` is empty. |
| Docker, MongoDB | Planned/optional | No Docker files are present; MongoDB is only commented out in `requirements.txt`. |
| End-to-end verification | **Not verified** | No live PostgreSQL + Airflow + provider run is documented in this repository. |

---

## 📦 Installation & Setup

### 1. Clone the repository
```bash
git clone https://github.com/ibrahimhamzza/financial-market-data-pipeline.git
cd financial-market-data-pipeline
```

### 2. Create a virtual environment
```bash
python -m venv venv
source venv/bin/activate        # Linux / macOS
venv\Scripts\activate           # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment variables
```bash
cp .env.example .env
# Open .env and fill in your values
```

| Variable | Needed for | Notes |
|----------|------------|-------|
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | Database tasks | Defaults are `localhost`, `5432`, `financial_db`, `postgres`, and an empty password. `DB_PASSWORD` must be non-empty and must not remain a placeholder; validation happens when a connection is created. |
| `DATA_SOURCE` | All extraction | `yfinance` (default) or `alphavantage`. Any other value is rejected at import. |
| `ALPHA_VANTAGE_API_KEY` | `DATA_SOURCE=alphavantage` | Required only for Alpha Vantage; missing or placeholder values are rejected. |
| `SYMBOLS` | All extraction | Comma-separated tickers. Defaults to the eight symbols listed below when unset. |
| `ALPHA_VANTAGE_OUTPUT_SIZE` | Alpha Vantage | Defaults to `compact` (latest ~100 sessions). |
| `RAW_DATA_DIR` | All runs | Defaults to `raw_data`; relative paths are resolved from the project root. |

**Airflow does not automatically load the project `.env`.** The pipeline modules call `python-dotenv` when `config/settings.py` is imported, so task code can read pipeline settings, but Airflow's own configuration (metadata database, DAGs folder, executor, and `AIRFLOW__*` variables) must come from the shell environment or `airflow.cfg`. In particular, the `AIRFLOW__CORE__LOAD_EXAMPLES` line in `.env.example` does not affect Airflow unless it is exported in the Airflow environment.

### 5. Set up the PostgreSQL database

The pipeline needs a reachable PostgreSQL server and a database (default name `financial_db`). The SQL uses `ON CONFLICT` (PostgreSQL 9.5+) and standard window functions; it does **not** require PostgreSQL 15. The local environment provides PostgreSQL 14.24; no live schema/load test was run in this task. PostgreSQL 15 is the design target in the project documents, not a hard requirement of the code. Check the supported-backend matrix for the Airflow version you deploy.

```bash
# Make sure PostgreSQL is running, then:
psql -U postgres -c "CREATE DATABASE financial_db;"
psql -U postgres -d financial_db -f database/schema.sql
psql -U postgres -d financial_db -f database/views.sql
```

The `psql -U postgres` examples assume a local `postgres` role. Use a role with permission to create tables and views, or adjust the commands and the `DB_*` values in `.env`. `database/schema.sql` creates the three tables, constraints, and indexes; `database/views.sql` creates the analyst views. `database/analytics.sql` contains optional example queries and is not required to run the pipeline.

### 6. Initialize Airflow

Run these commands from the repository root, in the same Python environment where `requirements.txt` is installed. Airflow's default DAGs folder is `$AIRFLOW_HOME/dags`, which is **not** this project's `dags/` directory, so point Airflow at the project DAG folder explicitly:

```bash
export AIRFLOW_HOME="$(pwd)/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db init
airflow users create \
    --username admin --password admin \
    --firstname Admin --lastname User \
    --role Admin --email admin@example.com
```

> **Local-only example account:** `admin` / `admin` is an example for a local machine. Change the password before starting Airflow and never expose this account outside a local development environment. For anything shared, use a strong password and restrict access to the Airflow UI.

Alternatively, symlink the DAG file under `$AIRFLOW_HOME/dags` rather than copying it. The DAG resolves the repository root from its own path (`Path(__file__).resolve().parent.parent`), so a copy in `$AIRFLOW_HOME/dags` will not import `pipeline` unless the repository root is on `PYTHONPATH`. If you change the DAGs folder after Airflow starts, restart the scheduler.

### 7. Start Airflow
```bash
airflow webserver --port 8080 &
airflow scheduler &
# Open http://localhost:8080
# Log in with the local-only example account created above, and change its password before use
```

---

## ▶️ Running the Pipeline Manually

Run from the repository root:

```bash
# Full pipeline: extract → validate → transform → load → quality_check
python pipeline.py
```

A full run requires a reachable PostgreSQL database with `database/schema.sql` applied. `database/views.sql` is optional for the pipeline itself; it is needed only for the analyst views. Extraction uses the incremental watermark from `raw_market_data`; transform reads stored history for metrics; load and quality check write and read the database. Network access is required for the selected provider. With `DATA_SOURCE=alphavantage`, a valid `ALPHA_VANTAGE_API_KEY` is also required; the default `yfinance` path needs no key.

The CLI path calls `run_pipeline()`, which writes a `pipeline_runs` row (`running` → `success`/`failed`). Triggering `financial_pipeline_dag` in Airflow runs the same five task callables directly, so **Airflow-triggered runs do not create or update `pipeline_runs` audit rows**. This is a known gap; see **Limitations & Known Issues**.

Or trigger it from the Airflow UI under DAGs → `financial_pipeline_dag` → Trigger DAG ▶ (provided Airflow is configured to discover `dags/` as described above).

---

## 📊 Tracked Stock Symbols

`SYMBOLS` is read from the environment (or `.env`) as a comma-separated list; values are trimmed and uppercased. The default when `SYMBOLS` is unset is:

```bash
SYMBOLS=AAPL,MSFT,GOOGL,AMZN,META,TSLA,NVDA,JPM
```

Set it in `.env` or export it in the shell before running `python pipeline.py`. The default list lives in `config/settings.py`; edit that file only if you intentionally want a different project-wide default. Adding a symbol means its first run has no watermark and will request the provider's default history (one year for yfinance; the `compact` window for Alpha Vantage).

---

## 📐 Database Schema

### `raw_market_data`
| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | Auto-increment |
| symbol | VARCHAR(10) NOT NULL | Stock ticker (e.g. AAPL) |
| trade_date | DATE NOT NULL | Trading date |
| open_price | NUMERIC(12,4) NOT NULL | Opening price |
| high_price | NUMERIC(12,4) NOT NULL | Daily high |
| low_price | NUMERIC(12,4) NOT NULL | Daily low |
| close_price | NUMERIC(12,4) NOT NULL | Closing price |
| volume | BIGINT NOT NULL | Trading volume; `CHECK (volume >= 0)` |
| loaded_at | TIMESTAMP NOT NULL DEFAULT NOW() | When this raw row was first inserted |

Constraints: `UNIQUE (symbol, trade_date)` and `CHECK (high_price >= low_price)`.

### `transformed_market_data`
| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | Auto-increment |
| symbol | VARCHAR(10) NOT NULL | Stock ticker |
| trade_date | DATE NOT NULL | Trading date |
| close_price | NUMERIC(12,4) NOT NULL | Closing price |
| daily_return | NUMERIC(10,6) | Decimal return from the previous close (`0.01` = 1%) |
| ma_7 | NUMERIC(12,4) | 7-day simple moving average |
| ma_20 | NUMERIC(12,4) | 20-day simple moving average |
| ma_50 | NUMERIC(12,4) | 50-day simple moving average |
| avg_volume_7d | NUMERIC(20,2) | 7-day average volume |
| volume_ratio | NUMERIC(10,4) | Current volume / 7-day average volume |
| cumulative_return | NUMERIC(10,6) | Compounded return over the full history supplied to the calculation |
| volatility_20d | NUMERIC(10,6) | 20-day rolling standard deviation of daily returns |
| loaded_at | TIMESTAMP NOT NULL DEFAULT NOW() | Last insert/upsert refresh time for this row |

Constraint: `UNIQUE (symbol, trade_date)`.

### `pipeline_runs`
| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | Auto-increment |
| run_date | TIMESTAMP NOT NULL DEFAULT NOW() | When the run row was created |
| status | VARCHAR(20) NOT NULL | `running`, `success`, or `failed` (enforced by a `CHECK` constraint) |
| symbols_processed | INTEGER | Distinct symbols present in `raw_market_data` when the run finishes; `0` for a no-new-data run |
| rows_inserted | INTEGER | New **raw** rows inserted by `load_raw()`; transformed upserts are not included |
| duration_seconds | NUMERIC(12,3) | Measured runtime |
| error_message | TEXT | Exception text when status = `failed` |

`database/schema.sql` also creates supporting indexes; `database/views.sql` adds `v_latest_prices`, `v_top_gainers`, and `v_volume_spikes`.

**Audit rows:** `pipeline_runs` rows are written only by the CLI `run_pipeline()` path. `start_pipeline_run()` inserts a `running` row, and `finish_pipeline_run()` updates it to `success` or `failed`. The Airflow DAG calls the five task callables directly and does not write this table.

---

## 🧮 Financial Metrics Calculated

Metrics are calculated per symbol after sorting by `trade_date` in `transform/calculator.py`. `daily_return` is a decimal fraction (`0.01` = 1%). Rolling windows stay `NaN` until the full window is available; the loader stores them as SQL `NULL`.

| Metric | Column | Definition | Persisted? |
|--------|--------|------------|------------|
| Daily Return | `daily_return` | `close_price.pct_change()` within a symbol | Yes |
| 7-day SMA | `ma_7` | Mean of the last 7 closes (`min_periods=7`) | Yes |
| 20-day SMA | `ma_20` | Mean of the last 20 closes (`min_periods=20`) | Yes |
| 50-day SMA | `ma_50` | Mean of the last 50 closes (`min_periods=50`) | Yes |
| 7-day Average Volume | `avg_volume_7d` | Mean of the last 7 volumes (`min_periods=7`) | Yes |
| Volume Ratio | `volume_ratio` | `volume / avg_volume_7d`; the current day is included in the average | Yes |
| Cumulative Return | `cumulative_return` | `(1 + daily_return.fillna(0)).cumprod() - 1` over the full history supplied to the calculation | Yes |
| 20-day Volatility | `volatility_20d` | Rolling standard deviation of `daily_return` (`min_periods=20`) | Yes |
| Maximum Drawdown | `max_drawdown` | `cumulative_return - cumulative_return.cummax()` (drawdown from the running peak; negative values) | **No** — calculated in `transform/calculator.py`, but absent from `database/schema.sql` and `database/loader.py` |

For incremental runs, `transform/staged_metrics.py` supplies the full stored raw history plus the staging batch, so `cumulative_return` uses all known bars rather than only the current batch. `max_drawdown` is calculated and covered by tests, but it is **not persisted**; it cannot be queried from PostgreSQL without recalculating it or extending the schema and loader.

---

## 🔬 Sample SQL Analytics Query

```sql
-- Top 3 best-performing days per symbol (using window function)
WITH ranked AS (
  SELECT
    symbol,
    trade_date,
    daily_return,
    close_price,
    RANK() OVER (
      PARTITION BY symbol
      ORDER BY daily_return DESC NULLS LAST
    ) AS return_rank
  FROM transformed_market_data
  WHERE trade_date >= CURRENT_DATE - INTERVAL '90 days'
)
SELECT symbol, trade_date, ROUND(daily_return * 100, 2) AS return_pct, close_price
FROM ranked
WHERE return_rank <= 3
ORDER BY symbol, return_rank;
```

`NULLS LAST` keeps rows with a missing `daily_return` out of the top ranks instead of treating `NULL` as the highest value. The full set of window-function and CTE examples is in `database/analytics.sql`; reusable views are in `database/views.sql`.

---

## ✅ Running Tests

```bash
pytest tests/ -v
```

Actual result at the time of this README update (Python 3.10.12, pytest 9.1.1 on Linux):

```text
89 passed
```

The suite is offline: it uses mocks/stubs and does not require PostgreSQL, Airflow, or network access. It covers HTTP/payload validation, Alpha Vantage parsing, cleaning, metrics, staged incremental calculations, configuration validation, and extraction failure/no-new-data behavior. It does **not** prove an end-to-end run against a live database, a running Airflow deployment, or a real provider; no such verification is claimed here. The local environment used pytest 9.1.1, while `requirements.txt` pins 7.4.3, so a fresh install may report a different pytest version.

---

## ⚠️ Limitations & Known Issues

- **No end-to-end verification is claimed.** The code and offline tests exist, but no live PostgreSQL load, Airflow run, or real provider call is documented as verified in this repository.
- **Airflow does not load the project `.env` for its own configuration.** Export `AIRFLOW__*` settings in the shell or set them in `airflow.cfg`. The pipeline modules load `.env` through `python-dotenv` when `config/settings.py` is imported, so task code can read DB/provider settings; do not rely on this for the Airflow metadata database or DAG discovery.
- **Airflow DAG discovery is not automatic from `./dags`.** Airflow's default is `$AIRFLOW_HOME/dags`. Set `AIRFLOW__CORE__DAGS_FOLDER` to the project `dags/` directory, or symlink/copy the DAG into `$AIRFLOW_HOME/dags`.
- **`pipeline_runs` is not populated by Airflow runs.** Only `pipeline.run_pipeline()` (the CLI path) calls `start_pipeline_run()` and `finish_pipeline_run()`. The DAG invokes the five task callables directly, so Airflow runs leave no audit row.
- **A crashed CLI run can leave a `running` row.** If the process dies before the final update, `pipeline_runs.status` remains `running`; there is no automatic reconciliation.
- **`max_drawdown` is calculated but not persisted.** It exists in the calculator output and tests but not in the transformed table or loader column list.
- **`symbols_processed` is not a per-run fetch count.** It is the distinct symbol count in `raw_market_data` at quality-check time. `rows_inserted` counts new raw rows only.
- **yfinance is third-party, unofficial, and not unlimited.** It is the runtime default for development, but no published quota or SLA was verified, and it is not affiliated with or endorsed by Yahoo. Provider terms apply. The “unlimited” wording in `.env.example` is informal shorthand, not a project guarantee.
- **Alpha Vantage limits apply.** The provider support page states 25 requests/day on the free tier (checked 5 November 2026; terms can change). The default `compact` response covers roughly the latest 100 sessions; `full` history requires a premium key per the provider documentation.
- **Only yfinance and Alpha Vantage are supported.** Polygon.io is not implemented, and values outside `SUPPORTED_DATA_SOURCES` are rejected.
- **Visualization and notebooks are not implemented yet.** `notebooks/` is empty; Matplotlib, Plotly, and Jupyter are listed as planned tooling.
- **Docker and MongoDB are optional/planned.** There are no Docker files, and MongoDB is only commented out in `requirements.txt`.
- **No integration tests cover SQL, PostgreSQL, or Airflow.** The 42 tests are offline unit tests; SQL queries and DAG execution were not run in this task.
- **PostgreSQL 15 is not a hard requirement.** The code uses `ON CONFLICT` (PostgreSQL 9.5+) and does not require 15; the local environment provides PostgreSQL 14.24. Airflow 2.8.1's own supported-backend matrix still applies.

---

## 📁 Data Sources

| Source | Role in this project | API key / quota | Important limitation |
|--------|----------------------|-----------------|----------------------|
| **yfinance** | Runtime default (`DATA_SOURCE=yfinance`) for development and convenience backfills | No key in this project. No published quota or SLA in the sources reviewed. | Third-party library, **not** affiliated/endorsed by Yahoo and **not unlimited**. Yahoo’s terms apply; intended for research/educational and personal use. |
| **Alpha Vantage** | Official REST API path (`DATA_SOURCE=alphavantage`) for the DEPI “consume an API” requirement and demos | Key required. Provider support page states 25 requests/day on the free tier; premium plans and an open-source/educational exception (subject to provider verification) are available. | Project defaults to `TIME_SERIES_DAILY` with `outputsize=compact` (latest ~100 sessions). `full` history requires a premium key per current provider docs. |

Get a free Alpha Vantage key at <https://www.alphavantage.co/support/#api-key>. Provider facts above were checked on **5 November 2026**; quotas and terms can change. See [docs/09_api_research.md](./docs/09_api_research.md) for source URLs, response structures, error handling, and licensing notes.

Only these two providers are supported by `SUPPORTED_DATA_SOURCES`. **Polygon.io is not implemented** and is not a drop-in alternative in the current code.

---

## 📚 Project documentation (DEPI)

Planning, literature review, and requirements are in [`docs/`](./docs/README.md). These files must stay on the **same** GitHub repository (the link cannot change).

| Document | File |
|----------|------|
| Proposal | [docs/01_project_proposal.md](./docs/01_project_proposal.md) |
| Plan / Gantt | [docs/02_project_plan.md](./docs/02_project_plan.md) |
| Roles | [docs/03_task_assignment_and_roles.md](./docs/03_task_assignment_and_roles.md) |
| Risks | [docs/04_risk_assessment.md](./docs/04_risk_assessment.md) |
| KPIs | [docs/05_kpis.md](./docs/05_kpis.md) |
| Literature review | [docs/06_literature_review.md](./docs/06_literature_review.md) |
| Requirements | [docs/07_requirements_gathering.md](./docs/07_requirements_gathering.md) |
| System analysis & design | [docs/08_system_analysis_and_design.md](./docs/08_system_analysis_and_design.md) |
| API research | [docs/09_api_research.md](./docs/09_api_research.md) |
| Diagram sources & exports | [docs/diagrams/README.md](./docs/diagrams/README.md) |

---

## 📷 Screenshots

No Airflow, pgAdmin, or visualization screenshots are included in this repository yet. The project has not been verified with a running Airflow deployment or a populated database here, so no runtime screenshots are claimed. Add screenshots only after an actual run, and state the environment and date.

---

## 📄 License

This project is developed as a graduation project for the **Digital Egypt Pioneers Initiative (DEPI)** — Data Engineering Track.

---

## 🙏 Acknowledgements

- [Alpha Vantage](https://www.alphavantage.co/) for the financial data API
- [Apache Airflow](https://airflow.apache.org/) documentation
- [DEPI Program](https://depi.gov.eg/) for project guidance and mentorship
