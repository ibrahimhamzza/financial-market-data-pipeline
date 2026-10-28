"""End-to-end pipeline used by the CLI and the Airflow DAG."""

from __future__ import annotations

import time
from typing import Optional

import pandas as pd

from config.settings import RAW_DATA_DIR
from database.loader import (
    fetch_raw_for_symbols,
    finish_pipeline_run,
    load_raw,
    load_transformed,
    start_pipeline_run,
)
from ingestion.extractor import run_extraction
from transform.quality import run_quality_checks
from transform.staged_metrics import compute_metrics_for_staging
from utils.logger import get_logger

logger = get_logger("pipeline")

STAGING_PATH = RAW_DATA_DIR / "_staging.csv"
METRICS_PATH = RAW_DATA_DIR / "_metrics.csv"


def extract_task(**_context) -> str:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw = run_extraction(incremental=True)
    raw.to_csv(STAGING_PATH, index=False)
    logger.info("extract_task wrote %s rows to %s", len(raw), STAGING_PATH.name)
    return str(STAGING_PATH)


def validate_task(**_context) -> str:
    if not STAGING_PATH.exists():
        raise FileNotFoundError("Staging CSV missing; extract_task must run first")
    raw = pd.read_csv(STAGING_PATH)
    required = {
        "symbol",
        "trade_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
    }
    if raw.empty:
        logger.warning("validate_task: no new rows (incremental may already be up to date)")
        return str(STAGING_PATH)
    missing = required - set(raw.columns)
    if missing:
        raise ValueError(f"Staging file missing columns: {missing}")
    if raw["symbol"].isna().any() or raw["trade_date"].isna().any():
        raise ValueError("Staging file has null symbol or trade_date")
    logger.info("validate_task passed for %s rows", len(raw))
    return str(STAGING_PATH)


def transform_task(**_context) -> str:
    staging = pd.read_csv(STAGING_PATH) if STAGING_PATH.exists() else pd.DataFrame()
    history = pd.DataFrame()
    if not staging.empty and "symbol" in staging.columns:
        history = fetch_raw_for_symbols(staging["symbol"].dropna().unique().tolist())
    metrics = compute_metrics_for_staging(staging, history)
    metrics.to_csv(METRICS_PATH, index=False)
    logger.info("transform_task wrote %s metric rows", len(metrics))
    return str(METRICS_PATH)


def load_task(**_context) -> int:
    raw = pd.read_csv(STAGING_PATH) if STAGING_PATH.exists() else pd.DataFrame()
    metrics = pd.read_csv(METRICS_PATH) if METRICS_PATH.exists() else pd.DataFrame()
    inserted_raw = load_raw(raw)
    load_transformed(metrics)
    logger.info("load_task inserted %s new raw rows", inserted_raw)
    return inserted_raw


def quality_check_task(**_context) -> dict:
    return run_quality_checks()


def run_pipeline(record_run: bool = True) -> dict:
    """Run extract → validate → transform → load → quality_check once."""
    started = time.perf_counter()
    run_id: Optional[int] = None
    if record_run:
        try:
            run_id = start_pipeline_run()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not write pipeline_runs start: %s", exc)

    try:
        extract_task()
        validate_task()
        transform_task()
        inserted = load_task()
        checks = quality_check_task()
        duration = time.perf_counter() - started
        if run_id is not None:
            finish_pipeline_run(
                run_id,
                status="success",
                symbols_processed=int(checks.get("symbol_count") or 0),
                rows_inserted=int(inserted),
                duration_seconds=duration,
            )
        logger.info("Pipeline complete in %.2fs", duration)
        return {"status": "success", "quality": checks, "duration_seconds": duration}
    except Exception as exc:
        duration = time.perf_counter() - started
        if run_id is not None:
            finish_pipeline_run(
                run_id,
                status="failed",
                symbols_processed=0,
                rows_inserted=0,
                duration_seconds=duration,
                error_message=str(exc),
            )
        logger.exception("Pipeline failed")
        raise


if __name__ == "__main__":
    result = run_pipeline()
    print("Pipeline complete.", result)
