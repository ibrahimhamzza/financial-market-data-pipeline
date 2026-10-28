"""SQLAlchemy engine factory."""

from __future__ import annotations

from functools import lru_cache

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import database_url
from utils.logger import get_logger

logger = get_logger("db_connector")


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    url = database_url()
    engine = create_engine(url, pool_pre_ping=True, future=True)
    logger.info("Created SQLAlchemy engine")
    return engine


def ping() -> bool:
    """Return True if PostgreSQL accepts a connection."""
    with get_engine().connect() as conn:
        conn.execute(text("SELECT 1"))
    return True
