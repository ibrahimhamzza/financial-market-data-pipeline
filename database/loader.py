"""Insert DataFrames into PostgreSQL with idempotent upserts."""

from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from database.db_connector import get_engine
from utils.logger import get_logger

logger = get_logger("loader")

RAW_COLUMNS = [
    "symbol",
    "trade_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
]

TRANSFORMED_COLUMNS = [
    "symbol",
    "trade_date",
    "close_price",
    "daily_return",
    "ma_7",
    "ma_20",
    "ma_50",
    "avg_volume_7d",
    "volume_ratio",
    "cumulative_return",
    "volatility_20d",
]


def fetch_raw_for_symbols(
    symbols: Iterable[str],
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    """All stored OHLCV rows for the given symbols (needed for cumulative metrics)."""
    tickers = sorted({str(symbol).upper().strip() for symbol in symbols if str(symbol).strip()})
    empty = pd.DataFrame(columns=RAW_COLUMNS)
    if not tickers:
        return empty

    engine = engine or get_engine()
    sql = text(
        """
        SELECT symbol, trade_date, open_price, high_price, low_price, close_price, volume
        FROM raw_market_data
        WHERE symbol IN :symbols
        ORDER BY symbol, trade_date
        """
    ).bindparams(bindparam("symbols", expanding=True))
    with engine.connect() as conn:
        frame = pd.read_sql(sql, conn, params={"symbols": tickers}, parse_dates=["trade_date"])
    logger.info("Fetched %s raw history rows for %s symbols", len(frame), len(tickers))
    return frame if not frame.empty else empty


def _records(df: pd.DataFrame, columns: list[str]) -> list[dict]:
    subset = df.loc[:, columns].copy()
    subset["trade_date"] = pd.to_datetime(subset["trade_date"]).dt.date
    subset = subset.where(pd.notnull(subset), None)
    return subset.to_dict(orient="records")


_RAW_INSERT_SQL = text(
    """
    INSERT INTO raw_market_data
        (symbol, trade_date, open_price, high_price, low_price, close_price, volume)
    VALUES
        (:symbol, :trade_date, :open_price, :high_price, :low_price, :close_price, :volume)
    ON CONFLICT (symbol, trade_date) DO NOTHING
    """
)


def load_raw(df: pd.DataFrame, engine: Optional[Engine] = None) -> int:
    """Insert raw OHLCV rows. Existing (symbol, trade_date) keys are skipped."""
    if df is None or df.empty:
        logger.info("No raw rows to load")
        return 0

    engine = engine or get_engine()
    rows = _records(df, RAW_COLUMNS)
    with engine.begin() as conn:
        result = conn.execute(_RAW_INSERT_SQL, rows)
        inserted = result.rowcount if result.rowcount is not None else 0
    logger.info("Loaded raw_market_data: attempted=%s rowcount=%s", len(rows), inserted)
    return inserted


_TRANSFORMED_UPSERT_SQL = text(
        """
        INSERT INTO transformed_market_data (
            symbol, trade_date, close_price, daily_return, ma_7, ma_20, ma_50,
            avg_volume_7d, volume_ratio, cumulative_return, volatility_20d
        )
        VALUES (
            :symbol, :trade_date, :close_price, :daily_return, :ma_7, :ma_20, :ma_50,
            :avg_volume_7d, :volume_ratio, :cumulative_return, :volatility_20d
        )
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            close_price = EXCLUDED.close_price,
            daily_return = EXCLUDED.daily_return,
            ma_7 = EXCLUDED.ma_7,
            ma_20 = EXCLUDED.ma_20,
            ma_50 = EXCLUDED.ma_50,
            avg_volume_7d = EXCLUDED.avg_volume_7d,
            volume_ratio = EXCLUDED.volume_ratio,
            cumulative_return = EXCLUDED.cumulative_return,
            volatility_20d = EXCLUDED.volatility_20d,
            loaded_at = NOW()
        """
)


def load_transformed(df: pd.DataFrame, engine: Optional[Engine] = None) -> int:
    """Upsert transformed metrics so a re-run refreshes calculations."""
    if df is None or df.empty:
        logger.info("No transformed rows to load")
        return 0

    engine = engine or get_engine()
    rows = _records(df, TRANSFORMED_COLUMNS)
    with engine.begin() as conn:
        result = conn.execute(_TRANSFORMED_UPSERT_SQL, rows)
        affected = result.rowcount if result.rowcount is not None else len(rows)
    logger.info("Upserted transformed_market_data: rows=%s", affected)
    return affected


def load_batch(
    raw: Optional[pd.DataFrame],
    metrics: Optional[pd.DataFrame],
    engine: Optional[Engine] = None,
) -> dict[str, int]:
    """Load raw + transformed in ONE transaction (all-or-nothing).

    The incremental watermark is MAX(trade_date) in raw_market_data. If raw rows
    were committed but the transformed upsert then failed, the watermark would
    move forward and those days would never get metrics. One transaction makes
    that impossible: either both tables advance, or neither does.
    """
    engine = engine or get_engine()
    counts = {"raw": 0, "transformed": 0}
    with engine.begin() as conn:
        if raw is not None and not raw.empty:
            result = conn.execute(_RAW_INSERT_SQL, _records(raw, RAW_COLUMNS))
            counts["raw"] = result.rowcount if result.rowcount is not None else 0
        if metrics is not None and not metrics.empty:
            result = conn.execute(_TRANSFORMED_UPSERT_SQL, _records(metrics, TRANSFORMED_COLUMNS))
            counts["transformed"] = result.rowcount if result.rowcount is not None else len(metrics)
    logger.info("load_batch committed: %s", counts)
    return counts


def load_to_db(metrics: pd.DataFrame, raw: Optional[pd.DataFrame] = None) -> dict[str, int]:
    """Convenience wrapper used by the README one-liner."""
    counts = {"raw": 0, "transformed": 0}
    if raw is not None:
        counts["raw"] = load_raw(raw)
    counts["transformed"] = load_transformed(metrics)
    return counts


def start_pipeline_run(engine: Optional[Engine] = None) -> int:
    engine = engine or get_engine()
    sql = text(
        """
        INSERT INTO pipeline_runs (status, symbols_processed, rows_inserted)
        VALUES ('running', 0, 0)
        RETURNING id
        """
    )
    with engine.begin() as conn:
        run_id = conn.execute(sql).scalar_one()
    logger.info("Started pipeline_runs id=%s", run_id)
    return int(run_id)


def finish_pipeline_run(
    run_id: int,
    status: str,
    symbols_processed: int,
    rows_inserted: int,
    duration_seconds: float,
    error_message: Optional[str] = None,
    engine: Optional[Engine] = None,
) -> None:
    engine = engine or get_engine()
    sql = text(
        """
        UPDATE pipeline_runs
        SET status = :status,
            end_time = NOW(),
            symbols_processed = :symbols_processed,
            rows_inserted = :rows_inserted,
            duration_seconds = :duration_seconds,
            error_message = :error_message
        WHERE id = :run_id
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "status": status,
                "symbols_processed": symbols_processed,
                "rows_inserted": rows_inserted,
                "duration_seconds": duration_seconds,
                "error_message": error_message,
                "run_id": run_id,
            },
        )
    logger.info("Finished pipeline_runs id=%s status=%s", run_id, status)


def mark_stale_runs(older_than_minutes: int = 60, engine: Optional[Engine] = None) -> int:
    """Close runs stuck in 'running' (process killed / machine lost) as 'failed'.

    Without this, a crashed run stays 'running' forever and the audit table lies.
    Returns the number of rows updated.
    """
    engine = engine or get_engine()
    sql = text(
        """
        UPDATE pipeline_runs
        SET status = 'failed',
            end_time = NOW(),
            error_message = COALESCE(error_message, 'Marked stale: run never finished')
        WHERE status = 'running'
          AND start_time < NOW() - (:minutes * INTERVAL '1 minute')
        """
    )
    with engine.begin() as conn:
        updated = conn.execute(sql, {"minutes": older_than_minutes}).rowcount or 0
    if updated:
        logger.warning("Marked %s stale pipeline_runs as failed", updated)
    return int(updated)
