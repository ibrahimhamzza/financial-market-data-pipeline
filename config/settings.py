"""Load pipeline configuration from environment variables."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

SUPPORTED_DATA_SOURCES = frozenset({"yfinance", "alphavantage"})

_PLACEHOLDER_PASSWORDS = frozenset(
    {
        "your_password_here",
        "your_password",
        "changeme",
        "change_me",
        "replace_me",
        "placeholder",
    }
)
_PLACEHOLDER_API_KEYS = frozenset(
    {
        "your_api_key_here",
        "your_api_key",
        "your_key_here",
        "changeme",
        "change_me",
        "replace_me",
        "placeholder",
    }
)


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if value is None:
        return None
    return value.strip()


def _env_int(name: str, default: str) -> int:
    raw = _env(name, default) or default
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be an integer (got {raw!r})") from None


def _env_float(name: str, default: str) -> float:
    raw = _env(name, default) or default
    try:
        return float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a number (got {raw!r})") from None


def _require_positive(name: str, value: int | float) -> int | float:
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0 (got {value})")
    return value


def _require_non_negative(name: str, value: int | float) -> int | float:
    if value < 0:
        raise ValueError(f"{name} must be zero or greater (got {value})")
    return value


def _is_placeholder(value: str, placeholders: frozenset[str]) -> bool:
    return value.strip().lower() in placeholders


def validate_data_source(value: str | None = None) -> str:
    """Return a normalized DATA_SOURCE value or raise a clear ValueError."""
    source = (value if value is not None else DATA_SOURCE) or ""
    source = source.strip().lower()
    if source not in SUPPORTED_DATA_SOURCES:
        supported = ", ".join(sorted(SUPPORTED_DATA_SOURCES))
        raise ValueError(
            f"Unsupported DATA_SOURCE={source!r}. Supported values: {supported}"
        )
    return source


def _required(name: str) -> str:
    value = _env(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _csv_list(name: str, default: str) -> list[str]:
    raw = _env(name, default) or default
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


DB_HOST = _env("DB_HOST", "localhost") or "localhost"
DB_PORT = _env_int("DB_PORT", "5432")
DB_NAME = _env("DB_NAME", "financial_db") or "financial_db"
DB_USER = _env("DB_USER", "postgres") or "postgres"
DB_PASSWORD = _env("DB_PASSWORD", "") or ""

ALPHA_VANTAGE_API_KEY = _env("ALPHA_VANTAGE_API_KEY", "") or ""
# yfinance for development (unlimited). alphavantage for the official API requirement.
DATA_SOURCE = validate_data_source(_env("DATA_SOURCE", "yfinance") or "yfinance")
ALPHA_VANTAGE_OUTPUT_SIZE = _env("ALPHA_VANTAGE_OUTPUT_SIZE", "compact") or "compact"

SYMBOLS = _csv_list("SYMBOLS", "AAPL,MSFT,GOOGL,AMZN,META,TSLA,NVDA,JPM")

_raw_dir = _env("RAW_DATA_DIR", "raw_data") or "raw_data"
RAW_DATA_DIR = Path(_raw_dir)
if not RAW_DATA_DIR.is_absolute():
    RAW_DATA_DIR = PROJECT_ROOT / RAW_DATA_DIR

HTTP_TIMEOUT_SECONDS = _require_positive(
    "HTTP_TIMEOUT_SECONDS", _env_int("HTTP_TIMEOUT_SECONDS", "30")
)
MAX_RETRIES = _require_non_negative("MAX_RETRIES", _env_int("MAX_RETRIES", "3"))
RETRY_BASE_DELAY_SECONDS = _require_non_negative(
    "RETRY_BASE_DELAY_SECONDS", _env_float("RETRY_BASE_DELAY_SECONDS", "1")
)
LOG_LEVEL = (_env("LOG_LEVEL", "INFO") or "INFO").upper()


def _validate_db_port(port: int | str) -> int:
    try:
        value = int(port)
    except (TypeError, ValueError):
        raise ValueError("DB_PORT must be an integer between 1 and 65535") from None
    if not 1 <= value <= 65535:
        raise ValueError(f"DB_PORT must be between 1 and 65535 (got {value})")
    return value


def database_url() -> str:
    """SQLAlchemy URL for PostgreSQL (password is URL-encoded so @ is safe)."""
    user = quote_plus(DB_USER)
    password = quote_plus(DB_PASSWORD)
    host = quote_plus(DB_HOST)
    name = quote_plus(DB_NAME)
    return f"postgresql+psycopg2://{user}:{password}@{host}:{DB_PORT}/{name}"


def require_database_config() -> None:
    """Validate database settings at connection time, not at import time."""
    required = {
        "DB_HOST": DB_HOST,
        "DB_PORT": DB_PORT,
        "DB_NAME": DB_NAME,
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
    }
    missing = [
        name for name, value in required.items() if value is None or not str(value).strip()
    ]
    if missing:
        raise ValueError(
            "Missing required database environment variable(s): " + ", ".join(missing)
        )
    if _is_placeholder(str(DB_PASSWORD), _PLACEHOLDER_PASSWORDS):
        raise ValueError(
            "DB_PASSWORD is still set to a placeholder value; "
            "set a real database password in .env"
        )
    _validate_db_port(DB_PORT)


def require_alpha_vantage_key() -> str:
    key = (ALPHA_VANTAGE_API_KEY or "").strip()
    if not key or _is_placeholder(key, _PLACEHOLDER_API_KEYS):
        raise RuntimeError(
            "ALPHA_VANTAGE_API_KEY is missing or still set to a placeholder value. "
            "Add a real key to .env, or set DATA_SOURCE=yfinance for development."
        )
    return key
