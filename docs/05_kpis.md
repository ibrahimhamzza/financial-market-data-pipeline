# Key Performance Indicators (KPIs)

DEPI asks for measurable success criteria (reliability, completeness, maintainability)—not “user adoption,” because this is a data pipeline, not a consumer app.

Targets below are **project success bars** for the 4 December submission. Baseline is “not built yet” until implementation starts.

---

## 1. Pipeline reliability

| KPI | Definition | Target | How we measure |
|-----|------------|--------|----------------|
| DAG success rate | Successful Airflow runs / total triggered runs in the last 14 days of development | ≥ 90% after the first green end-to-end run | Airflow UI + `pipeline_runs.status` |
| Recoverable API failures | Failed HTTP calls that succeed within 3 retries | ≥ 80% of transient 429/5xx | Logs in `api_client` |
| Mean run duration | Wall time extract → quality_check for 8 symbols (incremental day) | < 10 minutes on a student laptop | `pipeline_runs.duration_seconds` |
| Idempotent reload | Rows in `raw_market_data` after running the same day twice | **0 extra rows** | Count before/after; unique `(symbol, trade_date)` |

---

## 2. Data completeness & quality

| KPI | Definition | Target | How we measure |
|-----|------------|--------|----------------|
| Symbol coverage | Symbols with at least one loaded row | 8 / 8 configured symbols | `COUNT(DISTINCT symbol)` |
| Null rate (OHLC) | Nulls in open/high/low/close after cleaning | < 1% of loaded rows | Quality notebook |
| Duplicate rate | Duplicate `(symbol, trade_date)` in raw or transformed | 0 | SQL `GROUP BY … HAVING COUNT(*) > 1` |
| Referential completeness | Transformed rows that have a matching raw row | 100% | `LEFT JOIN` anti-join count |
| Incremental correctness | After first backfill, subsequent runs fetch only dates **after** last loaded date per symbol | 100% of incremental runs | Compare watermark vs request range |

---

## 3. Analytics & documentation (academic)

| KPI | Definition | Target | How we measure |
|-----|------------|--------|----------------|
| SQL depth | Analytical queries using window functions and CTEs | ≥ 5 queries | `database/analytics.sql` |
| Test pass rate | pytest on ingestion/clean/calculate | 100% of written tests green | `pytest tests/ -v` |
| Reproducibility | New clone + `env.example` + README can reach a local run | Mentor or teammate follows README without extra tribal knowledge | Setup dry-run |
| Defense coverage | Each member can explain their module in ≤ 3 minutes | 4 / 4 members in rehearsal | Timed rehearsal notes |

---

## 4. Project-management KPIs

| KPI | Target |
|-----|--------|
| DEPI documents on GitHub | Planning, literature, requirements by this pack; design by 6 Nov |
| Milestone slip | ≤ 1 checkpoint late; never skip GitHub upload |
| PR review | No merge to `main` without a second teammate looking at ingestion/DB/Airflow changes |

---

## 5. What we will **not** fake

- Uptime 99.9% on a personal laptop is not a honest KPI. We report **DAG success on triggered runs**, not production SLA.
- “Daily active users” does not apply. Stakeholders are the team, mentor, and evaluation panel.
