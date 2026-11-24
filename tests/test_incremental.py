"""Tests for incremental loading, idempotency and pipeline_runs tracking.

* Unit tests use a fake engine (no database needed).
* Integration tests need PostgreSQL. They run in a throw-away schema and are
  skipped automatically when the database is unreachable.
  Optional: set TEST_DATABASE_URL to point at a specific database.
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from database import loader
from ingestion import incremental
from ingestion.incremental import (
    get_last_loaded_date,
    get_last_loaded_dates,
    next_start_date,
    resolve_start_dates,
)

# --------------------------------------------------------------------------- unit


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def scalar(self):
        return self._rows[0][0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)


class _FakeEngine:
    """Returns canned rows for any query; raises if `error` is set."""

    def __init__(self, rows=None, error=None):
        self.rows, self.error = rows or [], error

    @contextmanager
    def connect(self):
        if self.error:
            raise self.error
        engine = self

        class _Conn:
            def execute(self, *_a, **_k):
                return _Result(engine.rows)

        yield _Conn()


def test_next_start_is_day_after_watermark():
    assert next_start_date("aapl", engine=_FakeEngine([(date(2026, 3, 6),)])) == "2026-03-07"


def test_no_rows_means_full_history():
    assert next_start_date("AAPL", engine=_FakeEngine([(None,)])) is None


def test_single_symbol_lookup_falls_back_to_full_when_table_missing():
    assert get_last_loaded_date("AAPL", engine=_FakeEngine(error=RuntimeError("no table"))) is None


def test_bulk_lookup_marks_unknown_symbols_none():
    engine = _FakeEngine([("AAPL", date(2026, 3, 6))])
    assert get_last_loaded_dates(["aapl", "MSFT"], engine=engine) == {
        "AAPL": date(2026, 3, 6),
        "MSFT": None,
    }


def test_bulk_lookup_does_not_hide_database_errors():
    with pytest.raises(RuntimeError):
        get_last_loaded_dates(["AAPL"], engine=_FakeEngine(error=RuntimeError("db down")))


def test_resolve_start_dates_plan(monkeypatch):
    monkeypatch.setattr(
        incremental,
        "get_last_loaded_dates",
        lambda symbols, engine=None: {
            "AAPL": date(2026, 3, 5),   # behind -> fetch from 6th
            "MSFT": date(2026, 3, 9),   # watermark == today -> up to date, skipped
            "TSLA": None,               # never loaded -> full history
        },
    )
    plan = resolve_start_dates(["AAPL", "MSFT", "TSLA"], today=date(2026, 3, 9))
    assert plan == {"AAPL": "2026-03-06", "TSLA": None}
    assert "MSFT" not in plan


# ------------------------------------------------------------------- integration

SCHEMA_SQL = Path(__file__).resolve().parent.parent / "database" / "schema.sql"


@pytest.fixture()
def pg_engine():
    from config.settings import database_url

    url = os.getenv("TEST_DATABASE_URL") or database_url()
    schema = f"t_{uuid.uuid4().hex[:8]}"
    try:
        admin = create_engine(url, future=True)
        with admin.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA {schema}"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL not available: {exc}")

    engine = create_engine(url, future=True, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        conn.exec_driver_sql(SCHEMA_SQL.read_text())
    yield engine
    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def _raw(symbol: str, days: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": symbol,
            "trade_date": pd.to_datetime(days),
            "open_price": 10.0,
            "high_price": 12.0,
            "low_price": 9.0,
            "close_price": 11.0,
            "volume": 1000,
        }
    )


def _metrics(raw: pd.DataFrame) -> pd.DataFrame:
    out = raw[["symbol", "trade_date", "close_price"]].copy()
    for col in loader.TRANSFORMED_COLUMNS[3:]:
        out[col] = 0.1
    return out


def _count(engine, table: str) -> int:
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()


def test_running_the_load_twice_does_not_duplicate(pg_engine):
    raw = _raw("AAPL", ["2026-03-02", "2026-03-03", "2026-03-04"])
    first = loader.load_batch(raw, _metrics(raw), engine=pg_engine)
    second = loader.load_batch(raw, _metrics(raw), engine=pg_engine)
    assert first["raw"] == 3
    assert second["raw"] == 0  # nothing new inserted
    assert _count(pg_engine, "raw_market_data") == 3
    assert _count(pg_engine, "transformed_market_data") == 3


def test_watermark_advances_after_load(pg_engine):
    raw = _raw("AAPL", ["2026-03-02", "2026-03-03"])
    loader.load_batch(raw, _metrics(raw), engine=pg_engine)
    plan = resolve_start_dates(["AAPL", "MSFT"], today=date(2026, 3, 10), engine=pg_engine)
    assert plan == {"AAPL": "2026-03-04", "MSFT": None}
    # once we are caught up, AAPL is skipped entirely
    assert "AAPL" not in resolve_start_dates(["AAPL"], today=date(2026, 3, 3), engine=pg_engine)


def test_failed_metrics_roll_back_raw_rows(pg_engine):
    raw = _raw("AAPL", ["2026-03-02"])
    bad = _metrics(raw)
    bad["close_price"] = None  # violates NOT NULL on transformed_market_data
    with pytest.raises(Exception):
        loader.load_batch(raw, bad, engine=pg_engine)
    assert _count(pg_engine, "raw_market_data") == 0  # watermark must not move


def test_pipeline_run_lifecycle(pg_engine):
    run_id = loader.start_pipeline_run(engine=pg_engine)
    with pg_engine.connect() as conn:
        status, end_time = conn.execute(
            text("SELECT status, end_time FROM pipeline_runs WHERE id = :i"), {"i": run_id}
        ).one()
    assert status == "running" and end_time is None

    loader.finish_pipeline_run(run_id, "success", 8, 120, 1.5, engine=pg_engine)
    with pg_engine.connect() as conn:
        row = conn.execute(
            text("SELECT status, rows_inserted, end_time >= start_time FROM pipeline_runs WHERE id = :i"),
            {"i": run_id},
        ).one()
    assert tuple(row) == ("success", 120, True)


def test_stale_running_rows_are_closed(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO pipeline_runs (start_time, status) "
                "VALUES (NOW() - INTERVAL '3 hours', 'running'), (NOW(), 'running')"
            )
        )
    assert loader.mark_stale_runs(older_than_minutes=60, engine=pg_engine) == 1
    with pg_engine.connect() as conn:
        statuses = sorted(r[0] for r in conn.execute(text("SELECT status FROM pipeline_runs")))
    assert statuses == ["failed", "running"]
