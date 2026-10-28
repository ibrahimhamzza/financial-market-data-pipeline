"""Load pipeline configuration from environment variables."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if value is None:
        return None
    return value.strip()


def _required(name: str) -> str:
    value = _env(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _csv_list(name: str, default: str) -> list[str]:
    raw = _env(name, default) or default
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


DB_HOST = _env("DB_HOST", "localhost") or "localhost"
DB_PORT = int(_env("DB_PORT", "5432") or "5432")
DB_NAME = _env("DB_NAME", "financial_db") or "financial_db"
DB_USER = _env("DB_USER", "postgres") or "postgres"
DB_PASSWORD = _env("DB_PASSWORD", "") or ""

ALPHA_VANTAGE_API_KEY = _env("ALPHA_VANTAGE_API_KEY", "") or ""
# yfinance for development (unlimited). alphavantage for the official API requirement.
DATA_SOURCE = (_env("DATA_SOURCE", "yfinance") or "yfinance").lower()
ALPHA_VANTAGE_OUTPUT_SIZE = _env("ALPHA_VANTAGE_OUTPUT_SIZE", "compact") or "compact"

SYMBOLS = _csv_list("SYMBOLS", "AAPL,MSFT,GOOGL,AMZN,META,TSLA,NVDA,JPM")

_raw_dir = _env("RAW_DATA_DIR", "raw_data") or "raw_data"
RAW_DATA_DIR = Path(_raw_dir)
if not RAW_DATA_DIR.is_absolute():
    RAW_DATA_DIR = PROJECT_ROOT / RAW_DATA_DIR

HTTP_TIMEOUT_SECONDS = int(_env("HTTP_TIMEOUT_SECONDS", "30") or "30")
MAX_RETRIES = int(_env("MAX_RETRIES", "3") or "3")
RETRY_BASE_DELAY_SECONDS = float(_env("RETRY_BASE_DELAY_SECONDS", "1") or "1")
LOG_LEVEL = (_env("LOG_LEVEL", "INFO") or "INFO").upper()


def database_url() -> str:
    """SQLAlchemy URL for PostgreSQL (password is URL-encoded so @ is safe)."""
    user = quote_plus(DB_USER)
    password = quote_plus(DB_PASSWORD)
    host = quote_plus(DB_HOST)
    name = quote_plus(DB_NAME)
    return f"postgresql+psycopg2://{user}:{password}@{host}:{DB_PORT}/{name}"


def require_alpha_vantage_key() -> str:
    if not ALPHA_VANTAGE_API_KEY or ALPHA_VANTAGE_API_KEY == "your_api_key_here":
        raise RuntimeError(
            "ALPHA_VANTAGE_API_KEY is not set. Copy env.example to .env and add a key, "
            "or set DATA_SOURCE=yfinance for development."
        )
    return ALPHA_VANTAGE_API_KEY
