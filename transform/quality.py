
"""Post-load data quality checks used by the pipeline."""

from __future__ import annotations

from sqlalchemy import text

from database.db_connector import get_engine
from utils.logger import get_logger

logger = get_logger("quality")


def run_quality_checks() -> dict:
    engine = get_engine()
    checks: dict = {}

    queries = {
        "raw_rows": """
            SELECT COUNT(*) FROM raw_market_data
        """,
        "transformed_rows": """
            SELECT COUNT(*) FROM transformed_market_data
        """,
        "symbol_count": """
            SELECT COUNT(DISTINCT symbol) FROM raw_market_data
        """,
        "raw_duplicates": """
            SELECT COUNT(*) FROM (
                SELECT symbol, trade_date
                FROM raw_market_data
                GROUP BY symbol, trade_date
                HAVING COUNT(*) > 1
            ) d
        """,
        "transformed_duplicates": """
            SELECT COUNT(*) FROM (
                SELECT symbol, trade_date
                FROM transformed_market_data
                GROUP BY symbol, trade_date
                HAVING COUNT(*) > 1
            ) d
        """,
        "raw_close_nulls": """
            SELECT COUNT(*) FROM raw_market_data
            WHERE close_price IS NULL
        """,
        "invalid_raw_prices": """
            SELECT COUNT(*)
            FROM raw_market_data
            WHERE open_price <= 0
               OR high_price <= 0
               OR low_price <= 0
               OR close_price <= 0
               OR high_price < low_price
               OR open_price < low_price
               OR open_price > high_price
               OR close_price < low_price
               OR close_price > high_price
               OR volume < 0
        """,
        "orphan_transformed": """
            SELECT COUNT(*)
            FROM transformed_market_data t
            LEFT JOIN raw_market_data r
              ON r.symbol = t.symbol
             AND r.trade_date = t.trade_date
            WHERE r.id IS NULL
        """,
        "transformed_close_mismatch": """
            SELECT COUNT(*)
            FROM transformed_market_data t
            JOIN raw_market_data r
              ON r.symbol = t.symbol
             AND r.trade_date = t.trade_date
            WHERE t.close_price <> r.close_price
        """,
    }

    with engine.connect() as conn:
        for name, query in queries.items():
            checks[name] = conn.execute(text(query)).scalar_one()

    logger.info("Quality checks: %s", checks)

    failures = {
        "raw_duplicates": checks["raw_duplicates"],
        "transformed_duplicates": checks["transformed_duplicates"],
        "raw_close_nulls": checks["raw_close_nulls"],
        "invalid_raw_prices": checks["invalid_raw_prices"],
        "orphan_transformed": checks["orphan_transformed"],
        "transformed_close_mismatch": checks["transformed_close_mismatch"],
    }

    failures = {
        name: count
        for name, count in failures.items()
        if count > 0
    }

    if failures:
        raise ValueError(f"Data quality checks failed: {failures}")

    if checks["transformed_rows"] > checks["raw_rows"]:
        raise ValueError(
            "Transformed row count exceeds raw row count"
        )

    return checks
