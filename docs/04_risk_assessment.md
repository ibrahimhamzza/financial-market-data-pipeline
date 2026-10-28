# Risk Assessment & Mitigation Plan

Risks are scored on **Likelihood** (1–5) and **Impact** (1–5). **Score** = L × I. Owner is the member who watches the risk weekly.

| ID | Risk | L | I | Score | Impact if it happens | Mitigation | Owner |
|----|------|---|---|-------|----------------------|------------|-------|
| R1 | Alpha Vantage free tier (25 requests/day) blocks development and demos | 5 | 4 | 20 | Ingestion looks “broken”; cannot refill history | Develop and unit-test against **yfinance**; cache JSON in `raw_data/`; call Alpha Vantage only for the official API requirement and demos; batch symbols across days | Ali |
| R2 | API shape / field names differ or a symbol has gaps | 4 | 3 | 12 | Parser crashes; null-heavy tables | Schema validation before load; per-symbol try/except; log and continue; do not drop the whole run | Ali |
| R3 | Duplicate loads if the DAG is triggered twice | 4 | 4 | 16 | Wrong analytics (double volume, fake returns) | Unique `(symbol, trade_date)`; skip existing keys in `loader.py`; incremental watermark per symbol | Bavly |
| R4 | Secrets committed (`.env`, API key, DB password) | 3 | 5 | 15 | Key revoked; security fail in evaluation | `.gitignore` + `env.example` only; rotate key immediately if leaked | Ibrahim |
| R5 | PostgreSQL not installed or credentials differ per laptop | 4 | 4 | 16 | Load tasks fail on some machines | Document setup in README; same DB name `financial_db`; optional Docker Postgres later | Bavly / Ibrahim |
| R6 | Airflow install is heavy and breaks local Python | 4 | 4 | 16 | No DAG screenshots for submission | Dedicated venv; `AIRFLOW_HOME` inside the repo; DAG logic testable as plain Python first | Ibrahim |
| R7 | Schedule slip — design 6 Nov / code 30 Nov | 5 | 5 | 25 | Incomplete implementation grade | Compressed Gantt in the project plan; weekly sync; cut optional Docker/Mongo first | Ibrahim |
| R8 | One member blocked or unavailable | 3 | 4 | 12 | Module missing at demo | Cross-review PRs; leader reassigns; everyone can run the happy path locally | Ibrahim |
| R9 | Market holidays / empty bars treated as bugs | 3 | 2 | 6 | False quality-report failures | Treat missing trading days as expected; completeness vs **trading calendar**, not vs all calendar days | Emmanuel |
| R10 | Floating-point / timezone bugs in returns and timestamps | 3 | 3 | 9 | Wrong charts, failed tests | Store dates as `DATE`; timestamps UTC ISO-8601 in raw JSON; tests on known price series | Bavly / Emmanuel |
| R11 | GitHub URL changed after instructor submission | 2 | 5 | 10 | Violates DEPI rule (link is fixed) | One repo from day one; never recreate; only add collaborators | Ibrahim |
| R12 | Final presentation over 15 minutes or one person talks for all | 3 | 4 | 12 | Weak oral grade | Timed rehearsal; each member presents their module (see plan M8) | Emmanuel |

---

## Residual risk after mitigation

R1 and R7 stay **high** even with controls. They are accepted as follows:

- **R1:** Demo script uses cached `raw_data/` plus one live yfinance pull if the network is up.
- **R7:** Must-have for 30 Nov: extract → clean → metrics → PostgreSQL → DAG run. Nice-to-have: Docker, MongoDB, extra symbols.

---

## Escalation

1. Module owner tries the mitigation for 24 hours.
2. Raise in the weekly sync (or immediately in chat if it blocks others).
3. Team leader contacts the DEPI mentor if the deadline or GitHub link is at risk.
