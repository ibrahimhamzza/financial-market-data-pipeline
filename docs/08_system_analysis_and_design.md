# System Analysis & Design

**Due (DEPI):** 6 November 2026
**Status:** Design complete — implementation in repository modules

**Diagram sources & exports:** canonical Mermaid sources live in [`docs/diagrams/`](./diagrams/), with rendered SVG/PNG files in [`docs/diagrams/exports/`](./diagrams/exports/). The diagrams embedded in this document are kept consistent with those standalone sources; see [`docs/diagrams/README.md`](./diagrams/README.md).

---

## 1. Problem statement & objectives

See [01_project_proposal.md](./01_project_proposal.md). In one sentence: analysts need a **repeatable, scheduled ETL pipeline** that turns messy vendor OHLCV JSON into queryable PostgreSQL tables with returns and moving averages.

**Architecture style:** batch ETL / data pipeline (not MVC, not microservices). Control plane = Apache Airflow. Data plane = Python (Pandas) + PostgreSQL.

**Data-source default:** `config/settings.py` sets `DATA_SOURCE` to **yfinance** when the environment variable is absent. yfinance is therefore the runtime default for development, tests, and any scheduled run that does not override the setting. **Alpha Vantage** is the supported official REST API path used for the DEPI “consume an API” requirement and demonstrations; it is not the default. The diagrams label the external entity as “Alpha Vantage / yfinance” to show both supported paths, not to imply that Alpha Vantage is the default. Only those two values are accepted by `SUPPORTED_DATA_SOURCES`; Polygon.io is not implemented. See [09_api_research.md](./09_api_research.md) for provider evidence and limitations.

---

## 2. Software architecture

```mermaid
flowchart LR
    API["Financial Data API<br/>Alpha Vantage / yfinance"]
    AF["Airflow DAG<br/>financial_pipeline_dag<br/>0 18 * * 1-5"]
    CLI["Operator / CLI<br/>python pipeline.py"]
    AN["Analyst<br/>SQL · notebooks"]

    subgraph ORCH["Orchestration — pipeline.py"]
        RP["run_pipeline()<br/>CLI path with run audit"]
        ET["extract_task()"]
        VT["validate_task()"]
        TT["transform_task()"]
        LT["load_task()"]
        QT["quality_check_task()"]
    end

    subgraph ING["Ingestion — ingestion/"]
        EX["extractor.run_extraction()"]
        AC["api_client.fetch_daily()<br/>validate_http_response()<br/>save_raw_json()"]
        IN["incremental.next_start_date()"]
    end

    subgraph TRF["Transform — transform/"]
        SM["staged_metrics.compute_metrics_for_staging()"]
        CL["cleaner.clean_data()"]
        CA["calculator.calculate_metrics()"]
        QC["quality.run_quality_checks()"]
    end

    subgraph DBL["Database — database/"]
        DBC["db_connector.get_engine()"]
        LD["loader.load_raw()<br/>loader.load_transformed()<br/>loader.fetch_raw_for_symbols()"]
        AUD["loader.start_pipeline_run()<br/>loader.finish_pipeline_run()"]
    end

    subgraph FILES["raw_data/ — intermediate files"]
        JSON["SYMBOL_TIMESTAMP.json<br/>vendor payloads"]
        STG["_staging.csv<br/>extract batch"]
        MET["_metrics.csv<br/>metric batch"]
    end

    subgraph PG["PostgreSQL — persistent tables"]
        RAW[("raw_market_data")]
        TRANS[("transformed_market_data")]
        RUNS[("pipeline_runs")]
    end

    ANALYTICS["analytics.sql · views.sql<br/>notebooks"]

    AF --> ET
    AF --> VT
    AF --> TT
    AF --> LT
    AF --> QT
    CLI --> RP
    RP --> ET
    RP --> VT
    RP --> TT
    RP --> LT
    RP --> QT
    RP --> AUD
    AUD --> DBC
    AUD --> RUNS

    ET --> EX
    EX --> IN
    IN --> DBC
    EX --> AC
    AC --> API
    AC --> JSON
    ET --> STG

    STG --> VT
    STG --> TT
    STG --> LT
    TT --> SM
    SM --> CL
    SM --> CA
    TT --> MET
    MET --> LT

    TT --> LD
    LD --> RAW
    LT --> LD
    LD --> RAW
    LD --> TRANS

    QT --> QC
    QC --> DBC
    QC --> RAW
    QC --> TRANS

    AN --> ANALYTICS
    ANALYTICS --> RAW
    ANALYTICS --> TRANS
    ANALYTICS --> RUNS

    NOTE["Airflow invokes the five task callables directly.<br/>run_pipeline() is the CLI path that also records pipeline_runs."]
    AF -.- NOTE

    classDef external fill:#f5f5f5,stroke:#666,stroke-width:1px
    classDef process fill:#e8f0fe,stroke:#3b6fb6,stroke-width:1px
    classDef file fill:#fff4d6,stroke:#c99a00,stroke-width:1px
    classDef table fill:#e6f4ea,stroke:#2e7d32,stroke-width:1px
    classDef note fill:#fffbe6,stroke:#999,stroke-dasharray:3 3

    class API,AF,CLI,AN,ANALYTICS external
    class RP,ET,VT,TT,LT,QT,EX,AC,IN,SM,CL,CA,QC,DBC,LD,AUD process
    class JSON,STG,MET file
    class RAW,TRANS,RUNS table
    class NOTE note
```

Extract writes the raw vendor JSON files and the batch staging file. Validate reads only the staging CSV. Transform reads the staging CSV and stored raw history, then writes `_metrics.csv`; it performs no database inserts. Load is the only task that writes raw and transformed records to PostgreSQL. Quality check then queries both tables.

`run_pipeline()` is the CLI orchestrator and is the only path that writes the `pipeline_runs` audit rows (through `database/loader.py`). The Airflow DAG imports the five task callables and invokes them directly, so Airflow owns task state and does not update `pipeline_runs`.

Modules:

| Package | Responsibility |
|---------|----------------|
| `config/` | Environment variables and validation helpers |
| `utils/` | Logging |
| `ingestion/` | HTTP/API client, response validation, symbol loop, watermarks |
| `transform/` | Cleaning, metrics calculation, staged metrics, quality queries |
| `database/` | Engine, schema, loader, pipeline-run audit helpers |
| `dags/` | Airflow graph |
| `pipeline.py` | Five tasks (`extract`, `validate`, `transform`, `load`, `quality_check`) used by the Airflow DAG, plus `run_pipeline` for direct CLI runs |

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
    E1["E1 · Financial Data API<br/>Alpha Vantage / yfinance"]
    E2["E2 · Scheduler / Operator<br/>Airflow · CLI"]
    E3["E3 · Analyst<br/>SQL · notebooks"]

    P0(("0<br/>Financial Market<br/>Data Pipeline"))

    subgraph FILES["Intermediate files · raw_data/ (not database tables)"]
        D1[["D1 · Raw JSON files<br/>vendor payload archive"]]
        D2[["D2 · _staging.csv<br/>extract batch"]]
        D3[["D3 · _metrics.csv<br/>metric batch"]]
    end

    subgraph TABLES["Persistent PostgreSQL tables"]
        D4[("D4 · raw_market_data")]
        D5[("D5 · transformed_market_data")]
        D6[("D6 · pipeline_runs")]
    end

    E1 -->|"OHLCV JSON + HTTP status"| P0
    E2 -->|"trigger · retry · monitor"| P0
    P0 -->|"vendor JSON"| D1
    P0 -->|"staging batch"| D2
    P0 -->|"metric batch"| D3
    P0 -->|"raw inserts"| D4
    P0 -->|"metric upserts"| D5
    P0 -->|"run audit: status · counts · error"| D6
    D4 -->|"stored OHLCV history"| P0
    D4 -->|"tables and views"| E3
    D5 -->|"tables and views"| E3
    D6 -->|"run history"| E3
    P0 -->|"logs · run result"| E2

    classDef external fill:#f5f5f5,stroke:#666,stroke-width:1px
    classDef process fill:#e8f0fe,stroke:#3b6fb6,stroke-width:1px
    classDef file fill:#fff4d6,stroke:#c99a00,stroke-width:1px
    classDef table fill:#e6f4ea,stroke:#2e7d32,stroke-width:1px

    class E1,E2,E3 external
    class P0 process
    class D1,D2,D3 file
    class D4,D5,D6 table
```

**Level 1**

```mermaid
flowchart LR
    E1["E1 · Financial Data API<br/>Alpha Vantage / yfinance"]
    E2["E2 · Scheduler / Operator<br/>Airflow · CLI"]
    E3["E3 · Analyst<br/>SQL · notebooks"]

    subgraph PROC["Processes"]
        P0(["P0 · Orchestrate & audit<br/>run_pipeline()"])
        P1(["P1 · Extract"])
        P2(["P2 · Validate"])
        P3(["P3 · Transform"])
        P4(["P4 · Load"])
        P5(["P5 · Quality Check"])
    end

    subgraph FILES["Intermediate files · raw_data/ (not database tables)"]
        D1[["D1 · Raw JSON files<br/>SYMBOL_UTC.json"]]
        D2[["D2 · _staging.csv<br/>batch OHLCV"]]
        D3[["D3 · _metrics.csv<br/>metric rows"]]
    end

    subgraph TABLES["Persistent PostgreSQL tables"]
        D4[("D4 · raw_market_data")]
        D5[("D5 · transformed_market_data")]
        D6[("D6 · pipeline_runs")]
    end

    E1 -->|"OHLCV daily bars + HTTP status"| P1
    E2 -->|"Airflow task trigger"| P1
    E2 -->|"CLI: python pipeline.py"| P0
    P0 -->|"start task chain (CLI)"| P1

    D4 -->|"MAX(trade_date) per-symbol watermark"| P1
    P1 -->|"vendor payload per symbol (write-only archive)"| D1
    P1 -->|"standardized staging batch"| D2

    D2 -->|"staging rows"| P2
    P2 -->|"validated staging path"| P3
    D2 -->|"staging rows"| P3
    D4 -->|"stored OHLCV history"| P3
    P3 -->|"metrics for staging keys"| D3

    D2 -->|"raw rows"| P4
    D3 -->|"metric rows"| P4
    P4 -->|"INSERT · ON CONFLICT DO NOTHING"| D4
    P4 -->|"UPSERT · ON CONFLICT DO UPDATE"| D5

    D4 -->|"row counts · duplicate keys · null closes"| P5
    D5 -->|"row counts · orphan keys"| P5
    P5 -->|"CLI: quality counts / error"| P0

    P1 -.->|"CLI: NO_NEW_DATA or ExtractionError"| P0
    P2 -.->|"CLI: validation ValueError"| P0
    P3 -.->|"CLI: transform exception"| P0
    P4 -.->|"CLI: load / database exception"| P0
    P0 -->|"CLI audit: status · counts · duration · error"| D6
    P0 -->|"run result / logs"| E2

    D4 -->|"price queries"| E3
    D5 -->|"metric queries"| E3
    D6 -->|"run history"| E3

    NOTE["CLI path: P0 invokes P1 → P2 → P3 → P4 → P5 in order and writes pipeline_runs.<br/>Airflow path: Scheduler/Operator triggers the five task callables directly; Airflow owns task state."]
    P0 -.- NOTE

    classDef external fill:#f5f5f5,stroke:#666,stroke-width:1px
    classDef process fill:#e8f0fe,stroke:#3b6fb6,stroke-width:1px
    classDef file fill:#fff4d6,stroke:#c99a00,stroke-width:1px
    classDef table fill:#e6f4ea,stroke:#2e7d32,stroke-width:1px
    classDef note fill:#fffbe6,stroke:#999,stroke-dasharray:3 3

    class E1,E2,E3 external
    class P0,P1,P2,P3,P4,P5 process
    class D1,D2,D3 file
    class D4,D5,D6 table
    class NOTE note
```

The Level-1 DFD numbers the five ETL processes P1–P5 and adds P0 for run orchestration/audit. P0 is only needed because `pipeline_runs` is written by `run_pipeline()` on the CLI path; the Airflow DAG triggers the task callables directly.

1. Extract reads the per-symbol watermark, fetches bars, validates the API HTTP response and payload, and saves the vendor JSON. `pipeline.extract_task()` — not `extractor.run_extraction()` — writes the batch to `raw_data/_staging.csv`.
2. Landing zone stores the vendor JSON returned by each completed API request (`<SYMBOL>_<UTC timestamp>.json`).
3. Validate reads `_staging.csv` and checks that it is non-empty, has the required OHLCV columns, and has no null `symbol` or `trade_date`. This is separate from the API response validation performed during extraction.
4. Transform combines the staging batch with stored raw history, cleans the data, calculates metrics, and writes only the staging keys to `raw_data/_metrics.csv`. It does not insert into PostgreSQL.
5. Load inserts raw rows into `raw_market_data` (`ON CONFLICT DO NOTHING`) and upserts metric rows into `transformed_market_data` (`ON CONFLICT DO UPDATE`).
6. Quality check queries both tables for row counts, duplicate raw keys, null closes, and transformed rows without a raw parent; duplicates or orphans raise an error.
7. If extraction reports no new data, `pipeline.extract_task()` removes stale `_staging.csv` and `_metrics.csv` files and returns `NO_NEW_DATA`; downstream tasks are skipped and the run is recorded as a success with zero rows.

**Intermediate files:** `raw_data/_staging.csv` (extract output, validate/transform/load input) and `raw_data/_metrics.csv` (transform output, load input). Both are batch files under `RAW_DATA_DIR`, not database tables. Raw JSON files are the write-only vendor landing archive. `raw_market_data`, `transformed_market_data`, and `pipeline_runs` are the persistent PostgreSQL tables.

---

## 5. Sequence — incremental weekday run

```mermaid
sequenceDiagram
    autonumber
    participant S as Scheduler / run_pipeline
    participant PX as pipeline.extract_task
    participant EX as extractor.run_extraction
    participant IN as incremental.next_start_date
    participant A as api_client
    participant FS as raw_data/
    participant V as pipeline.validate_task
    participant T as pipeline.transform_task
    participant L as pipeline.load_task
    participant Q as pipeline.quality_check_task
    participant DB as PostgreSQL

    opt run_pipeline() CLI path (not Airflow)
        S->>DB: start_pipeline_run() INSERT pipeline_runs status='running'
    end

    S->>PX: extract_task()
    PX->>EX: run_extraction(incremental=True)

    loop each configured symbol
        EX->>IN: next_start_date(symbol)
        IN->>DB: SELECT MAX(trade_date) FROM raw_market_data
        DB-->>IN: last loaded date or NULL
        IN-->>EX: watermark + 1 day or None
        EX->>A: fetch_daily(symbol, start=watermark)
        A->>FS: save vendor JSON
        A-->>EX: standardized OHLCV DataFrame
        alt rows returned and no API/payload error
            EX->>EX: append frame
        else empty frame without error
            EX->>EX: log "No new rows" for symbol
        else API/payload error after retries
            EX->>EX: record symbol error and continue loop
        end
    end

    alt no symbol errors and at least one frame
        EX-->>PX: combined OHLCV DataFrame
        Note over PX: extract_task owns staging creation.
        PX->>FS: write _staging.csv
        PX-->>S: staging path

        S->>V: validate_task()
        V->>FS: read _staging.csv

        alt staging non-empty, required columns present, no null keys
            V-->>S: validated staging path
            S->>T: transform_task()
            T->>FS: read _staging.csv
            T->>DB: fetch_raw_for_symbols() read-only history
            DB-->>T: stored OHLCV rows
            Note over T: may raise on missing columns<br/>or history-read failure
            T->>FS: write _metrics.csv
            T-->>S: metrics path

            S->>L: load_task()
            L->>FS: read _staging.csv + _metrics.csv
            L->>DB: INSERT raw_market_data ON CONFLICT DO NOTHING
            L->>DB: UPSERT transformed_market_data ON CONFLICT DO UPDATE
            L-->>S: inserted raw row count

            S->>Q: quality_check_task()
            Q->>DB: counts · duplicates · null closes · orphans
            DB-->>Q: check results
            alt duplicates = 0 and orphans = 0
                Q-->>S: quality check counts
                Note over S: finish_pipeline_run(status='success', ...)
            else duplicates > 0 or orphans > 0
                Q-->>S: raise ValueError
            end
        else validation fails
            V-->>S: raise ValueError
        end

    else every symbol checked, none returned rows, no errors
        EX-->>PX: empty DataFrame with attrs["no_new_data"] = True
        Note over PX: extract_task owns NO_NEW_DATA handling.
        PX->>FS: remove stale _staging.csv and _metrics.csv
        PX-->>S: "NO_NEW_DATA"
        Note over S: run_pipeline (CLI) records success with 0 rows<br/>and skips validate → quality_check. Airflow tasks self-skip

    else any symbol fails (partial or all)
        EX-->>PX: raise ExtractionError
        PX-->>S: re-raise ExtractionError
        Note over S: run_pipeline records failed run<br/>and no staging file is written
    end
```

`pipeline.extract_task()` owns batch-file creation and removal: it writes `_staging.csv` on a successful extraction, removes stale `_staging.csv` and `_metrics.csv` on `NO_NEW_DATA`, and re-raises `ExtractionError`. `extractor.run_extraction()` only returns the combined DataFrame (with the `no_new_data` attribute when applicable) or raises. Transform only reads stored raw history in this flow; Load performs both database writes.

---

## 6. Activity — extraction and pipeline flow

```mermaid
flowchart TD
    START(["start: Airflow task chain or run_pipeline()"]) --> RECORD{"run_pipeline()<br/>CLI path?"}
    RECORD -->|yes| RUNSTART["start_pipeline_run()<br/>INSERT pipeline_runs status=running"]
    RECORD -->|"no · Airflow task chain"| EXT
    RUNSTART --> EXT

    EXT["extract_task()"] --> MORE{"More configured<br/>symbols?"}
    MORE -->|yes| WM["incremental.next_start_date()<br/>SELECT MAX(trade_date)"]
    WM --> FETCH["api_client.fetch_daily()<br/>retry connection / timeout / API errors"]
    FETCH --> API_OK{"HTTP and payload<br/>valid?"}
    API_OK -->|"no · retries left"| FETCH
    API_OK -->|"no · retries exhausted"| SYMERR["Record symbol error<br/>continue loop"]
    API_OK -->|yes| EMPTY{"Frame empty?"}
    EMPTY -->|yes| NOROW["Log no new rows for symbol"]
    EMPTY -->|no| APPEND["Append standardized frame"]
    SYMERR --> MORE
    NOROW --> MORE
    APPEND --> MORE

    MORE -->|no| ERRORS{"Any symbol<br/>errors?"}
    ERRORS -->|"yes · partial or all"| XFAIL["run_extraction() raises ExtractionError<br/>extract_task() re-raises"]
    ERRORS -->|no| ANYROWS{"Any rows<br/>collected?"}
    ANYROWS -->|no| NODATA["run_extraction() sets attrs['no_new_data']<br/>extract_task() removes stale<br/>_staging.csv and _metrics.csv<br/>returns NO_NEW_DATA"]
    ANYROWS -->|yes| STAGE["extract_task() writes _staging.csv<br/>returns staging path"]

    XFAIL --> FAIL["Exception propagates<br/>pipeline failed"]
    NODATA --> NODONE(["NO_NEW_DATA success · 0 rows<br/>downstream skipped (CLI) / self-skip (Airflow)"])
    STAGE --> VALID{"validate_task()<br/>non-empty, required columns,<br/>no null symbol / trade_date?"}
    VALID -->|no| VFAIL["ValueError<br/>pipeline failed"]
    VALID -->|yes| TRANSFORM["transform_task()<br/>read staging + raw history<br/>clean + calculate metrics<br/>write _metrics.csv"]
    TRANSFORM -->|"exception · missing columns<br/>or history-read failure"| TFAIL["Exception<br/>pipeline failed"]
    TRANSFORM --> LOAD["load_task()<br/>INSERT raw ON CONFLICT DO NOTHING<br/>UPSERT transformed"]
    LOAD -->|"database error"| LFAIL["Exception<br/>pipeline failed"]
    LOAD --> QUALITY{"quality_check_task()<br/>duplicates = 0 and<br/>orphans = 0?"}
    QUALITY -->|"no · duplicates or orphans"| QFAIL["ValueError<br/>pipeline failed after load committed"]
    QUALITY -->|yes| DONE([all tasks succeeded])

    FAIL --> FIN["run_pipeline() records<br/>finish_pipeline_run(status=failed)<br/>and re-raises; Airflow marks task failed"]
    VFAIL --> FIN
    TFAIL --> FIN
    LFAIL --> FIN
    QFAIL --> FIN
    NODONE --> FIN_OK["finish_pipeline_run(status=success)<br/>or Airflow success state"]
    DONE --> FIN_OK
    FIN --> END([end])
    FIN_OK --> END

    classDef decision fill:#fff4d6,stroke:#c99a00,stroke-width:1px
    classDef failure fill:#fdecea,stroke:#c62828,stroke-width:1px
    classDef success fill:#e6f4ea,stroke:#2e7d32,stroke-width:1px

    class RECORD,MORE,API_OK,EMPTY,ERRORS,ANYROWS,VALID,QUALITY decision
    class XFAIL,FAIL,VFAIL,TFAIL,LFAIL,QFAIL failure
    class NODATA,NODONE,DONE,FIN_OK success
```

A partial symbol failure is not skipped: successful frames are discarded when `ExtractionError` is raised, and no staging file is written.

---

## 7. State diagram — `pipeline_runs.status`

```mermaid
stateDiagram-v2
    [*] --> running: start_pipeline_run()<br/>INSERT pipeline_runs
    running --> success: all tasks complete<br/>quality checks pass
    running --> success: NO_NEW_DATA<br/>0 rows · downstream skipped or self-skipped
    running --> failed: ExtractionError<br/>partial or all symbols
    running --> failed: validation ValueError
    running --> failed: transform exception<br/>clean / calculate / history read
    running --> failed: load or database exception
    running --> failed: quality ValueError<br/>duplicates or orphans · load already committed
    success --> [*]
    failed --> [*]

    note right of running
        If start_pipeline_run() fails,
        no pipeline_runs row is written;
        the pipeline logs a warning and continues.
    end note

    note left of failed
        Airflow task states are separate.
        Airflow does not update pipeline_runs.
    end note
```

A transform exception (missing columns, calculation failure, or history-read failure) marks the run failed, just like extraction, validation, load/database, and quality-check errors. If `start_pipeline_run()` fails, the pipeline logs a warning and continues without a `pipeline_runs` row. Airflow task states (queued → running → success | up_for_retry → failed) are owned by Airflow, not our table.

---

## 8. Error-handling policy

The extractor treats any symbol failure as a run failure; it does not report partial success.

| Case | Actual behavior |
|------|-----------------|
| All symbols succeed (at least one row overall) | `run_extraction()` returns the combined OHLCV DataFrame; `extract_task()` writes `_staging.csv`; the pipeline continues to validate → transform → load → quality check. |
| Some symbols return rows, others return none (empty frame, no error) | `run_extraction()` logs "No new rows" for the empty symbols and returns the combined frames from the non-empty symbols. If there are no errors, extraction succeeds and the pipeline continues; this is not the all-symbols `NO_NEW_DATA` case. For yfinance, an empty download raises `APIClientError`, so it falls under partial or all-symbol failure instead. |
| Partial symbol failure | `run_extraction()` records failed symbols in `ExtractionError.errors` and successful ones in `symbols_ok`, then raises `ExtractionError`. Successful frames stay in memory only. `extract_task()` re-raises, so no `_staging.csv` is written and the successful symbols are not loaded. `run_pipeline()` records `pipeline_runs.status = 'failed'` and re-raises. |
| All symbols fail | `ExtractionError` is raised with an empty `symbols_ok`; the same failed-run behavior applies. |
| No new data | Occurs when every symbol's fetch returns an empty frame without raising and no errors occurred (for example, Alpha Vantage data filtered out by the incremental start date). `run_extraction()` then returns an empty DataFrame with `attrs["no_new_data"] = True`. `extract_task()` deletes stale `_staging.csv` and `_metrics.csv` and returns `"NO_NEW_DATA"`. `run_pipeline()` records `status = 'success'` with `symbols_processed = 0` and `rows_inserted = 0`, and skips validate, transform, load, and quality check. Note: `fetch_yfinance()` raises `APIClientError` for an empty download, so an up-to-date yfinance symbol is treated as a symbol failure, not `NO_NEW_DATA`. |
| Validation failure | `validate_task()` raises `ValueError` when staging is empty, required columns are missing, or `symbol` / `trade_date` is null. `run_pipeline()` records the failed run and re-raises. |
| Database / load failure | Exceptions from `load_task()` (engine/connection errors, constraint violations, and so on) propagate. `run_pipeline()` records the failed run and re-raises. `load_raw()` and `load_transformed()` each use their own transaction, so if the transformed upsert fails, raw rows committed by the same run may remain. |
| Quality-check failure | `run_quality_checks()` raises `ValueError` when `raw_duplicates > 0` or `orphan_transformed > 0`; null close counts are returned and logged but do not fail the run. `quality_check_task()` propagates the error, and `run_pipeline()` records the failed run and re-raises. Quality check runs after load, so rows written by that run remain committed even though the run is marked failed. |
| Pipeline-run audit write failure | A failure to insert the initial `pipeline_runs` row is logged as a warning and the pipeline continues without a run record. Failures to update the no-new-data and failure-state finish records are also best-effort warnings. A failure to update the normal success finish record is not wrapped locally, so it enters the outer exception handler, marks the run failed, and re-raises. |
| Incremental watermark lookup failure | Logged as a warning; extraction falls back to a full-history request for that symbol (`next_start_date()` returns `None`). |
| Rollback and pipeline-level retries (not implemented) | `run_pipeline()` has no automatic rollback or compensation after a quality-check failure, and no retry logic of its own; this is unspecified by the current code. Retries are owned by Airflow (`default_args`: `retries=3`, 5-minute delay) or by the caller. |

> `run_extraction()` retries individual API calls inside `api_client._retry_call()` (connection, timeout, and `APIClientError`), but after those retries are exhausted the symbol is recorded as failed and the extraction policy above applies.

> Downstream tasks also guard against a missing staging file: `validate_task()` and `transform_task()` return `"SKIP_NO_NEW_DATA"`, `load_task()` returns `0`, and `quality_check_task()` returns `{"status": "skipped_no_new_data"}`. `run_pipeline()` normally short-circuits before those tasks on `NO_NEW_DATA`.

> On extraction failure, vendor JSON files already written for successful symbols remain on disk, and any pre-existing `_staging.csv` is not deleted; the run simply stops before downstream tasks.

---

## 9. Component / class view

The implementation is module/function based; the only custom classes are the exception types.

```mermaid
flowchart TB
    subgraph DAG["dags/"]
        DAGF["financial_pipeline_dag.py<br/>extract >> validate >> transform >> load >> quality_check"]
    end

    subgraph PIPE["pipeline.py"]
        RP["run_pipeline()"]
        ET["extract_task()"]
        VT["validate_task()"]
        TT["transform_task()"]
        LT["load_task()"]
        QT["quality_check_task()"]
    end

    subgraph EXTRACT["ingestion/"]
        EX["extractor.run_extraction()<br/>ExtractionError"]
        AC["api_client.fetch_daily()<br/>fetch_alpha_vantage() / fetch_yfinance()<br/>validate_http_response()<br/>parse_alpha_vantage_payload()<br/>save_raw_json() / _retry_call()<br/>APIClientError / PayloadValidationError"]
        IN["incremental.get_last_loaded_date()<br/>incremental.next_start_date()"]
    end

    subgraph TRANSFORM["transform/"]
        SM["staged_metrics.compute_metrics_for_staging()<br/>combine_history_and_staging()<br/>staging_keys()"]
        CL["cleaner.clean_data()"]
        CA["calculator.calculate_metrics()"]
        QU["quality.run_quality_checks()"]
    end

    subgraph DATABASE["database/"]
        DBC["db_connector.get_engine()<br/>db_connector.ping()"]
        LD["loader.load_raw()<br/>loader.load_transformed()<br/>loader.fetch_raw_for_symbols()<br/>loader.load_to_db() (convenience wrapper)"]
        AUD["loader.start_pipeline_run()<br/>loader.finish_pipeline_run()"]
    end

    subgraph SHARED["config/ and utils/"]
        CFG["settings.py<br/>DATA_SOURCE · SYMBOLS · RAW_DATA_DIR<br/>database_url() · validation helpers"]
        LOG["logger.get_logger()"]
    end

    STG[["_staging.csv<br/>intermediate"]]
    MET[["_metrics.csv<br/>intermediate"]]

    DAGF --> ET
    DAGF --> VT
    DAGF --> TT
    DAGF --> LT
    DAGF --> QT

    RP --> ET
    RP --> VT
    RP --> TT
    RP --> LT
    RP --> QT
    RP --> AUD

    ET --> EX
    EX --> AC
    EX --> IN
    ET -->|writes| STG
    STG -->|reads| VT
    STG -->|reads| TT
    STG -->|reads| LT

    TT --> SM
    TT -->|history read| LD
    SM --> CL
    SM --> CA
    SM -->|writes| MET
    MET -->|reads| LT

    LT --> LD
    QT --> QU

    IN --> DBC
    LD --> DBC
    AUD --> DBC
    QU --> DBC

    AC --> CFG
    IN --> CFG
    DBC --> CFG
    EX --> LOG
    LD --> LOG
    QU --> LOG

    classDef module fill:#e8f0fe,stroke:#3b6fb6,stroke-width:1px
    classDef file fill:#fff4d6,stroke:#c99a00,stroke-width:1px
    classDef shared fill:#f5f5f5,stroke:#666,stroke-width:1px

    class ET,VT,TT,LT,QT,RP,EX,AC,IN,SM,CL,CA,QU,DBC,LD,AUD,DAGF module
    class STG,MET file
    class CFG,LOG shared
```

```mermaid
classDiagram
    class ExtractionError {
        +dict errors
        +list symbols_ok
    }
    class APIClientError
    class PayloadValidationError
    Exception <|-- ExtractionError
    Exception <|-- APIClientError
    APIClientError <|-- PayloadValidationError
```

| Component | Key symbols | Responsibility |
|-----------|-------------|----------------|
| `config/settings.py` | `DATA_SOURCE`, `SYMBOLS`, `RAW_DATA_DIR`, `database_url()`, `validate_data_source()`, `require_database_config()`, `require_alpha_vantage_key()` | Environment configuration, source/DB/key validation |
| `utils/logger.py` | `get_logger()` | One root logger with stdout and `logs/pipeline.log` handlers |
| `ingestion/api_client.py` | `fetch_daily()`, `validate_http_response()`, `save_raw_json()`, `_retry_call()`, `parse_alpha_vantage_payload()`, `APIClientError`, `PayloadValidationError` | Source dispatch, HTTP/payload validation, retries, raw JSON persistence, column standardization |
| `ingestion/extractor.py` | `run_extraction()`, `ExtractionError` | Symbol loop, incremental start date, error collection, no-new-data result, failure policy |
| `ingestion/incremental.py` | `get_last_loaded_date()`, `next_start_date()` | High-watermark lookup from `raw_market_data`; full-history fallback on lookup failure |
| `transform/cleaner.py` | `clean_data()` | Rename/type-cast, forward-fill prices, deduplicate, drop unusable closes, sanitize volume |
| `transform/calculator.py` | `calculate_metrics()` | Daily return, SMA 7/20/50, average volume, volume ratio, cumulative return, volatility, max drawdown |
| `transform/staged_metrics.py` | `compute_metrics_for_staging()`, `combine_history_and_staging()` | Combine stored history with staging; return metric rows only for staging keys |
| `transform/quality.py` | `run_quality_checks()` | Post-load counts, duplicate and orphan checks; raises on duplicates/orphans |
| `database/db_connector.py` | `get_engine()`, `ping()` | Cached SQLAlchemy engine and DB configuration validation |
| `database/loader.py` | `load_raw()`, `load_transformed()`, `fetch_raw_for_symbols()`, `load_to_db()`, `start_pipeline_run()`, `finish_pipeline_run()` | Raw insert with conflict skip, transformed upsert, history read, pipeline-run audit |
| `pipeline.py` | `extract_task()`, `validate_task()`, `transform_task()`, `load_task()`, `quality_check_task()`, `run_pipeline()` | Task orchestration, staging/metrics files, NO_NEW_DATA, run audit |
| `dags/financial_pipeline_dag.py` | DAG definition | Weekday schedule and `extract >> validate >> transform >> load >> quality_check` dependency chain |

---

## 10. ERD (logical)

```mermaid
erDiagram
    RAW_MARKET_DATA {
        serial id PK
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
        serial id PK
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
        serial id PK
        timestamp run_date
        varchar status
        int symbols_processed
        int rows_inserted
        numeric duration_seconds
        text error_message
    }
    RAW_MARKET_DATA ||--o| TRANSFORMED_MARKET_DATA : "0..1 derived row per symbol + trade_date"
```

Physical DDL: [`database/schema.sql`](../database/schema.sql). Unique business key: `(symbol, trade_date)`. Composite indexes on `(symbol, trade_date)` and `(symbol, trade_date, close_price)`. `pipeline_runs` is an independent audit log with no foreign key to the fact tables.

**Normalization:** 3NF fact tables keyed by symbol and session date. Metrics are stored denormalized in `transformed_market_data` on purpose (warehouse fact grain = one session bar) so analysts do not recompute windows on every query.

---

## 11. Deployment / component diagram

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

Optional later: Docker Compose with `pipeline` + `postgres` services. No public web UI is required. **Status:** planned/optional only — no `Dockerfile` or `docker-compose.yml` is present in the repository at this revision, so Docker is not part of the current implementation.

---

## 12. UI/UX

Not a product UI. Operator surfaces: Airflow (localhost:8080), pgAdmin or `psql`, Jupyter notebooks. No Figma mockups.

---

## 13. Testing & deployment notes

- Unit tests: `pytest tests/ -v` (no database required).
- Failure-path coverage: `tests/test_extraction_pipeline_failure.py` asserts all-symbol/partial `ExtractionError`, no-new-data success, and failed-run recording.
- Integration: create `financial_db`, apply `schema.sql` + `views.sql`, copy `.env.example` → `.env`, `python pipeline.py`.
- Secrets stay in `.env`. GitHub URL stays fixed.

---

## 14. Technology stack rationale

This section explains why each technology is used in this repository and separates **implemented** components from **configured/listed** or **planned** ones. “Implemented in code” means the module exists and is exercised by offline tests where noted; it does **not** mean a live database, Airflow deployment, or provider call has been verified in this repository. The current verification gap is summarised in §13 and in `DEPI_Project_Walkthrough_Report.md`.

| Technology | Why it is used here | Status | Evidence |
|------------|---------------------|--------|----------|
| **Python 3.10+** | One language for ingestion, cleaning, metric calculation, database loading, Airflow callables, and tests | Implemented | All application modules; `README.md`; `docs/07_requirements_gathering.md` NFR6. `requirements.txt` does not pin the interpreter. |
| **Pandas** | In-memory batch frames for parsing vendor payloads, standardising columns, cleaning, and rolling-window metrics | Implemented | `ingestion/api_client.py`, `ingestion/extractor.py`, `transform/*.py`, `database/loader.py`, `pipeline.py` |
| **NumPy** | Numerical dependency of Pandas; direct imports are limited to tests | Configured / transitive; not directly imported by application modules | `requirements.txt`; `tests/test_calculator.py`, `tests/test_staged_metrics.py` |
| **requests** | Direct HTTP `GET` for Alpha Vantage with explicit timeout and query parameters; avoids hiding the request/response contract behind another wrapper | Implemented | `ingestion/api_client.py` (`ALPHA_VANTAGE_URL`, `requests.get`, `validate_http_response`) |
| **python-dotenv** | Load `.env` without hard-coding secrets; supports local development and Airflow environment injection | Implemented | `config/settings.py` |
| **yfinance** | Runtime-default development source: no API key, pandas-native output, convenient backfills | Implemented | `ingestion/api_client.py` `fetch_yfinance()`; see [09_api_research.md](./09_api_research.md) §3 |
| **Alpha Vantage REST API** | Official API path for the DEPI requirement; documented daily OHLCV endpoint with a documented key/quota model | Implemented via `requests` | `ingestion/api_client.py` `fetch_alpha_vantage()`; `config/settings.py` `require_alpha_vantage_key()`; see [09_api_research.md](./09_api_research.md) §2 |
| **`alpha-vantage` Python package** | Listed as a dependency, but the application calls the REST endpoint directly with `requests` | Not used by application code | `requirements.txt`; no import in `ingestion/` |
| **SQLAlchemy 1.4.52** | Engine/connection management, parameterised SQL, transactions, and DataFrame reads; 1.4 is pinned because Airflow 2.8.1 requires it | Implemented | `database/db_connector.py`, `database/loader.py`, `ingestion/incremental.py`, `transform/quality.py`; `requirements.txt` comment |
| **psycopg2-binary** | PostgreSQL DBAPI driver selected by the `postgresql+psycopg2` SQLAlchemy URL | Implemented as the driver dependency | `config/settings.py` `database_url()`; `requirements.txt` |
| **PostgreSQL 15** | Persistent relational store: unique `(symbol, trade_date)` keys, `ON CONFLICT` inserts/upserts, audit table, and SQL window-function analytics | Implemented in code; runtime not verified here | `database/schema.sql`, `database/loader.py`, `database/analytics.sql`, `database/views.sql` |
| **Apache Airflow 2.8.1** | Control plane: weekday schedule, task dependencies, retries, and run visibility; the CLI path remains available for manual runs | Implemented in code; not verified running here | `dags/financial_pipeline_dag.py`, `pipeline.py`, `requirements.txt` |
| **pytest** | Offline unit tests for parsing, cleaning, metrics, configuration, and failure policy | Implemented | `tests/` |
| **Matplotlib / Plotly / Jupyter** | Planned visualization and reporting layer | Listed/planned; the `notebooks/` directory is currently empty | `requirements.txt`, `README.md` |
| **Docker / Docker Compose** | Optional packaging for a one-command local demo | Not implemented; no `Dockerfile` or `docker-compose.yml` in the repository | Repository tree; `README.md`; §11 |
| **MongoDB** | Optional alternative raw-JSON store | Not implemented; only a commented requirement | `requirements.txt` |

**Design rationale in one paragraph.** The workload is a small daily batch (eight default symbols), so Python + Pandas is sufficient; Spark or a distributed engine would add operational cost without a demonstrated need. PostgreSQL is chosen for transactional constraints, idempotent upserts, and SQL analytics rather than storing only flat files. SQLAlchemy + psycopg2 provide a parameterised database layer while keeping the schema explicit in `schema.sql`. Airflow supplies scheduling, retries, and task visibility; `pipeline.py` also supports a direct CLI run. yfinance is the convenience default for development, while Alpha Vantage is the official REST path that satisfies the DEPI API requirement. Neither provider is treated as unlimited or SLA-backed. Docker and MongoDB remain optional extensions and are not part of the current implementation.
