"""High-watermark incremental extraction (last loaded date per symbol)."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from database.db_connector import get_engine
from utils.logger import get_logger

logger = get_logger("incremental")


def get_last_loaded_date(symbol: str, engine: Optional[Engine] = None) -> Optional[date]:
    """Return the latest trade_date already stored for a symbol, or None."""
    engine = engine or get_engine()
    sql = text(
        """
        SELECT MAX(trade_date) AS last_date
        FROM raw_market_data
        WHERE symbol = :symbol
        """
    )
    try:
        with engine.connect() as conn:
            value = conn.execute(sql, {"symbol": symbol.upper()}).scalar()
    except Exception as exc:  # noqa: BLE001 — empty DB / missing table is a first run
        logger.warning("Could not read watermark for %s (%s); doing full extract", symbol, exc)
        return None

    if value is None:
        logger.info("No watermark for %s — full history", symbol)
        return None
    last = value if isinstance(value, date) else date.fromisoformat(str(value))
    logger.info("Watermark for %s is %s", symbol, last)
    return last


def next_start_date(symbol: str, engine: Optional[Engine] = None) -> Optional[str]:
    """ISO date of the day after the last loaded bar (None = no filter)."""
    last = get_last_loaded_date(symbol, engine=engine)
    if last is None:
        return None
    return (last + timedelta(days=1)).isoformat()
