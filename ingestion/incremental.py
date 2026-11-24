"""High-watermark incremental extraction (last loaded date per symbol).

Idea: the watermark for a symbol is MAX(trade_date) already stored in
raw_market_data. The next run only asks the API for days AFTER that date, so
history is never re-downloaded. Combined with ON CONFLICT in the loader, running
the pipeline twice cannot create duplicate rows.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional

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


def get_last_loaded_dates(
    symbols: Iterable[str],
    engine: Optional[Engine] = None,
) -> dict[str, Optional[date]]:
    """Watermarks for many symbols in ONE query (instead of one query per symbol).

    Symbols never loaded map to None. DB errors are NOT swallowed here, so the
    caller can tell "first run" apart from "database is down".
    """
    tickers = sorted({str(s).upper().strip() for s in symbols if str(s).strip()})
    result: dict[str, Optional[date]] = {t: None for t in tickers}
    if not tickers:
        return result

    engine = engine or get_engine()
    sql = text(
        """
        SELECT symbol, MAX(trade_date) AS last_date
        FROM raw_market_data
        WHERE symbol = ANY(:symbols)
        GROUP BY symbol
        """
    )
    with engine.connect() as conn:
        for symbol, last in conn.execute(sql, {"symbols": tickers}):
            result[str(symbol).upper()] = last
    return result


def next_start_date(symbol: str, engine: Optional[Engine] = None) -> Optional[str]:
    """ISO date of the day after the last loaded bar (None = no filter)."""
    last = get_last_loaded_date(symbol, engine=engine)
    if last is None:
        return None
    return (last + timedelta(days=1)).isoformat()


def resolve_start_dates(
    symbols: Iterable[str],
    today: Optional[date] = None,
    engine: Optional[Engine] = None,
) -> dict[str, Optional[str]]:
    """Decide, per symbol, what to ask the API for.

    * symbol -> None          : never loaded, fetch full history
    * symbol -> "YYYY-MM-DD"  : fetch from that date (day after the watermark)
    * symbol missing from dict: already up to date (watermark is today or later),
                                skip the API call entirely

    ``None`` therefore always means "full history", never "nothing to do".
    """
    today = today or datetime.now(timezone.utc).date()
    plan: dict[str, Optional[str]] = {}
    for symbol, last in get_last_loaded_dates(symbols, engine=engine).items():
        if last is None:
            plan[symbol] = None
            continue
        nxt = last + timedelta(days=1)
        if nxt > today:
            logger.info("%s already up to date (watermark %s)", symbol, last)
            continue
        plan[symbol] = nxt.isoformat()
    return plan
