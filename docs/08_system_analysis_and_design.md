# System Analysis & Design

**Due (DEPI):** 6 November 2026
**Status:** Design complete — implementation in repository modules

---

## 1. Problem statement & objectives

See [01_project_proposal.md](./01_project_proposal.md). In one sentence: analysts need a **repeatable, scheduled ETL pipeline** that turns messy vendor OHLCV JSON into queryable PostgreSQL tables with returns and moving averages.

**Architecture style:** batch ETL / data pipeline (not MVC, not microservices). Control plane = Apache Airflow. Data plane = Python (Pandas) + PostgreSQL.

---

## 2. Software architecture

```mermaid
flowchart LR
    API["Alpha Vantage / yfinance"] --> EXT["extract\ningestion/"]
    EXT --> JSON["raw_data/*.json"]
    EXT --> VAL["validate"]
    VAL --> TR["transform\ncleaner + calculator"]
    TR --> LOAD["load\nSQLAlchemy"]
    LOAD --> PG[(PostgreSQL)]
    PG --> SQL["analytics.sql + views"]
    PG --> NB["Jupyter charts"]
    AF["Airflow DAG\n0 18 * * 1-5"] --> EXT
    AF --> VAL
    AF --> TR
    AF --> LOAD
    AF --> QC["quality_check"]
    QC --> PG
```

Modules:

| Package | Responsibility |
|---------|----------------|
| `config/` | Environment variables |
| `utils/` | Logging |
| `ingestion/` | HTTP/API, symbol loop, watermarks |
| `transform/` | Clean, metrics, quality queries |
| `database/` | Engine, schema, loader |
| `dags/` | Airflow graph |
| `pipeline.py` | Same five tasks for CLI and DAG |

---

## 3. Use case diagram

```mermaid
flowchart TB
    subgraph actors
        OP[Operator]
        AN[Analyst]
        SCH[Airflow scheduler]
        API[Market data API]
    end
    OP --> UC1[Trigger or monitor DAG]
    SCH --> UC2[Nightly incremental load]
    AN --> UC3[Query prices and rankings]
    AN --> UC4[Inspect quality report]
    UC2 --> API
    UC2 --> UC5[Write PostgreSQL]
    UC3 --> UC5
```

| Use case | Description |
|----------|-------------|
| UC-01 Nightly load | Scheduler runs extract → validate → transform → load → quality_check |
| UC-02 Manual run | Operator: `python pipeline.py` |
| UC-03 Rank query | Analyst runs window-function SQL |
| UC-04 Quality inspection | Notebook / `run_quality_checks()` |

---

## 4. Data flow (DFD)

**Context (level 0)**

```mermaid
flowchart LR
    API[Financial API] -->|OHLCV JSON| SYS[Financial pipeline]
    SYS -->|raw + curated tables| PG[(PostgreSQL)]
    OP[Operator] -->|trigger / logs| SYS
    AN[Analyst] -->|SQL / charts| PG
```

**Level 1**

1. Extract pulls bars (incremental watermark).
2. Landing zone stores vendor JSON.
3. Validate checks columns and keys.
4. Transform cleans and computes metrics.
5. Load upserts PostgreSQL.
6. Quality check counts nulls, duplicates, orphans.

---

## 5. Sequence — incremental weekday run

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant E as extractor
    participant A as api_client
    participant DB as PostgreSQL
    participant T as transform
    S->>E: extract_task
    E->>DB: MAX(trade_date) per symbol
    E->>A: fetch_daily(symbol, start=watermark+1)
    A-->>E: OHLCV DataFrame + JSON file
    S->>T: validate + clean + calculate
    T->>DB: INSERT raw ON CONFLICT DO NOTHING
    T->>DB: UPSERT transformed
    S->>DB: quality_check queries
```

---

## 6. Activity — one symbol

```mermaid
flowchart TD
    A[Read last loaded date] --> B{Rows already in DB?}
    B -->|no| C[Full history request]
    B -->|yes| D[Request from next calendar day]
    C --> E[HTTP / yfinance call]
    D --> E
    E --> F{Valid payload?}
    F -->|no, retries left| E
    F -->|no, exhausted| X[Log error, skip symbol]
    F -->|yes| G[Save JSON]
    G --> H[Standardize columns]
    H --> I[Clean + metrics]
    I --> J[Idempotent load]
```

---

## 7. State diagram — `pipeline_runs.status`

```mermaid
stateDiagram-v2
    [*] --> running: INSERT run
    running --> success: all tasks ok
    running --> failed: unhandled exception
    success --> [*]
    failed --> [*]
```

Airflow task states (queued → running → success | up_for_retry → failed) are owned by Airflow, not our table.

---

## 8. Class / component view

```mermaid
classDiagram
    class Settings {
        +SYMBOLS
        +DATA_SOURCE
        +database_url()
    }
    class APIClient {
        +fetch_daily(symbol)
        +validate_http_response()
    }
    class Extractor {
        +run_extraction()
    }
    class Incremental {
        +get_last_loaded_date()
    }
    class Cleaner {
        +clean_data()
    }
    class Calculator {
        +calculate_metrics()
    }
    class Loader {
        +load_raw()
        +load_transformed()
    }
    Extractor --> APIClient
    Extractor --> Incremental
    Incremental --> Loader
    Cleaner --> Calculator
    Calculator --> Loader
```

---

## 9. ERD (logical)

```mermaid
erDiagram
    RAW_MARKET_DATA {
        int id PK
        varchar symbol
        date trade_date
        numeric open_price
        numeric high_price
        numeric low_price
        numeric close_price
        bigint volume
        timestamp loaded_at
    }
    TRANSFORMED_MARKET_DATA {
        int id PK
        varchar symbol
        date trade_date
        numeric close_price
        numeric daily_return
        numeric ma_7
        numeric ma_20
        numeric ma_50
        numeric avg_volume_7d
        numeric volume_ratio
        numeric cumulative_return
        numeric volatility_20d
        timestamp loaded_at
    }
    PIPELINE_RUNS {
        int id PK
        timestamp run_date
        varchar status
        int symbols_processed
        int rows_inserted
        numeric duration_seconds
        text error_message
    }
    RAW_MARKET_DATA ||--o| TRANSFORMED_MARKET_DATA : "same symbol+date"
```

Physical DDL: [`database/schema.sql`](../database/schema.sql). Unique business key: `(symbol, trade_date)`. Composite indexes on `(symbol, trade_date)` and `(symbol, trade_date, close_price)`.

**Normalization:** 3NF fact tables keyed by symbol and session date. Metrics are stored denormalized in `transformed_market_data` on purpose (warehouse fact grain = one session bar) so analysts do not recompute windows on every query.

---

## 10. Deployment / component diagram

```mermaid
flowchart TB
    subgraph laptop [Student workstation]
        PY[Python 3.10 venv]
        AF[Airflow webserver + scheduler]
        PG[(PostgreSQL 15)]
        DISK[raw_data/ JSON]
        PY --> AF
        PY --> PG
        PY --> DISK
    end
    NET[Internet] --> PY
```

Optional later: Docker Compose with `pipeline` + `postgres` services. No public web UI is required.

---

## 11. UI/UX

Not a product UI. Operator surfaces: Airflow (localhost:8080), pgAdmin or `psql`, Jupyter notebooks. No Figma mockups.

---

## 12. Testing & deployment notes

- Unit tests: `pytest tests/ -v` (no database required).
- Integration: create `financial_db`, apply `schema.sql` + `views.sql`, copy `env.example` → `.env`, `python pipeline.py`.
- Secrets stay in `.env`. GitHub URL stays fixed.
