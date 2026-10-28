"""Post-load data quality checks used by the Airflow quality_check task."""

from __future__ import annotations

from sqlalchemy import text

from database.db_connector import get_engine
from utils.logger import get_logger

logger = get_logger("quality")


def run_quality_checks() -> dict:
    engine = get_engine()
    checks: dict = {}
    with engine.connect() as conn:
        checks["raw_rows"] = conn.execute(text("SELECT COUNT(*) FROM raw_market_data")).scalar_one()
        checks["transformed_rows"] = conn.execute(
            text("SELECT COUNT(*) FROM transformed_market_data")
        ).scalar_one()
        checks["symbol_count"] = conn.execute(
            text("SELECT COUNT(DISTINCT symbol) FROM raw_market_data")
        ).scalar_one()
        checks["raw_duplicates"] = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM (
                    SELECT symbol, trade_date
                    FROM raw_market_data
                    GROUP BY symbol, trade_date
                    HAVING COUNT(*) > 1
                ) d
                """
            )
        ).scalar_one()
        checks["close_nulls"] = conn.execute(
            text("SELECT COUNT(*) FROM raw_market_data WHERE close_price IS NULL")
        ).scalar_one()
        checks["orphan_transformed"] = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM transformed_market_data t
                LEFT JOIN raw_market_data r
                  ON r.symbol = t.symbol AND r.trade_date = t.trade_date
                WHERE r.id IS NULL
                """
            )
        ).scalar_one()

    logger.info("Quality checks: %s", checks)
    if checks["raw_duplicates"] > 0:
        raise ValueError(f"Duplicate raw keys detected: {checks['raw_duplicates']}")
    if checks["orphan_transformed"] > 0:
        raise ValueError(f"Transformed rows without raw parent: {checks['orphan_transformed']}")
    return checks
