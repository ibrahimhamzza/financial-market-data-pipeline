# Task Assignment & Roles

Required by DEPI Playbook: each member’s name, role, student ID, and email.

**Emails:** each member must add their DEPI / university email in the table below before the instructor submission. Do not put passwords or API keys in this file.

| Member | Name | Student ID | Email | Role |
|--------|------|------------|-------|------|
| A (Leader) | Ibrahim Sayed Muhammed | 21118171 | _TBD_ | Infrastructure & Airflow Engineer |
| B | Ali Khaled Fawzy Mansour | 21060005 | _TBD_ | API & Data Ingestion Engineer |
| C | Bavly Osam Geedy Arean | 21076897 | _TBD_ | Database & SQL Analytics Engineer |
| D | Emmanuel George Samy Riad | 21171868 | _TBD_ | QA, Visualization & Documentation |

---

## Member A — Ibrahim Sayed Muhammed (Leader)

**Owns:** architecture, GitHub process, Airflow, logging, optional Docker, mentor communication, deadlines.

| Phase | Tasks |
|-------|--------|
| Now | Repo collaborators, branch rules, first push of structure + `docs/` |
| Design | End-to-end architecture diagram; `config/settings.py`; `utils/logger.py` |
| Build | Airflow install, `dags/financial_pipeline_dag.py`, schedule `0 18 * * 1-5`, retries, UI screenshots |
| Close | Optional `Dockerfile` / `docker-compose.yml`; help D with README architecture section |

**Tools:** Git, Airflow, draw.io / Mermaid, python-dotenv, logging, Docker (optional).

---

## Member B — Ali Khaled Fawzy Mansour

**Owns:** API consumption, HTTP/payload validation, JSON parsing, retries, symbol loop, Airflow callables for extract/validate.

| Phase | Tasks |
|-------|--------|
| Design | Document Alpha Vantage + yfinance payloads; manual test calls; rate-limit notes |
| Build | `ingestion/api_client.py`, `ingestion/extractor.py`; save JSON under `raw_data/` |
| Integrate | Wrap extract/validate as Airflow-callable functions; per-task logging (rows, errors) |
| Close | Docstrings on ingestion modules; support live demo of a fetch |

**Tools:** `requests`, yfinance, Alpha Vantage, Pandas (parse), exception handling.

---

## Member C — Bavly Osam Geedy Arean

**Owns:** PostgreSQL, ERD, schema, load, transformations, SQL analytics, indexes, views.

| Phase | Tasks |
|-------|--------|
| Design | ERD; `database/schema.sql` (raw, transformed, pipeline_runs); index plan |
| Build | `db_connector.py`, `loader.py` (idempotent insert); `cleaner.py` support; `calculator.py` |
| Analytics | `analytics.sql` (≥5 queries with window functions + CTEs); `views.sql` |
| Close | Final ERD export; comment every SQL query’s business meaning |

**Tools:** PostgreSQL, pgAdmin, SQLAlchemy, psycopg2, Pandas, SQL window functions.

---

## Member D — Emmanuel George Samy Riad

**Owns:** documentation pack, pytest, data-quality report, visualizations, DEPI presentation template.

| Phase | Tasks |
|-------|--------|
| Now | Maintain `docs/` (this pack); keep README team table accurate |
| Build | `ingestion/incremental.py` with C (last loaded date, `pipeline_runs`); test fixtures |
| QA | `tests/test_api_client.py`, `test_cleaner.py`, `test_calculator.py`; quality notebook |
| Close | Charts (price + MA, volume, returns); DEPI PPT; final documentation PDF; rehearsal timing |

**Tools:** pytest, Matplotlib, Plotly, Jupyter, technical writing, PowerPoint (DEPI template).

---

## Shared responsibilities

Every member:

1. Writes commented, modular code in their modules.
2. Pushes to GitHub regularly (at least every two days during implementation).
3. Attends the weekly sync and reviews teammates’ pull requests.
4. Can explain **any** pipeline stage in the 15-minute defense (not only their own files).

Work that is **not** owned by one person: integration week (Airflow wiring), final ZIP, and Q&A rehearsal.

---

## RACI (summary)

| Work item | Ibrahim | Ali | Bavly | Emmanuel |
|-----------|:-------:|:---:|:-----:|:--------:|
| GitHub + deadlines | **A** | C | C | C |
| Architecture diagram | **A** | C | C | I |
| API client / extractor | I | **A** | C | I |
| Schema / ERD / loader | I | C | **A** | I |
| Cleaner / calculator | I | C | **A** | C |
| Incremental + run audit | I | C | C | **A** |
| SQL analytics / views | I | I | **A** | C |
| Airflow DAG | **A** | C | I | I |
| Tests + quality report | C | C | C | **A** |
| Visualizations + PPT | I | C | C | **A** |
| Proposal / this docs pack | C | C | C | **A** |

**A** = accountable · **C** = contributor · **I** = informed
