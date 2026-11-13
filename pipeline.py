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
from ingestion.extractor import ExtractionError, run_extraction
from transform.quality import run_quality_checks
from transform.staged_metrics import compute_metrics_for_staging
from utils.logger import get_logger

logger = get_logger("pipeline")

STAGING_PATH = RAW_DATA_DIR / "_staging.csv"
METRICS_PATH = RAW_DATA_DIR / "_metrics.csv"


def extract_task(**_context) -> str:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    try:
        raw = run_extraction(incremental=True)
    except ExtractionError:
        raise
    if getattr(raw, "attrs", {}).get("no_new_data"):
        # Remove any stale staging/metrics to avoid loading old data
        try:
            if STAGING_PATH.exists():
                STAGING_PATH.unlink()
        except Exception:  # noqa: BLE001
            pass
        try:
            if METRICS_PATH.exists():
                METRICS_PATH.unlink()
        except Exception:  # noqa: BLE001
            pass
        logger.info("extract_task: no new data available (incremental up to date)")
        return "NO_NEW_DATA"
    raw.to_csv(STAGING_PATH, index=False)
    logger.info("extract_task wrote %s rows to %s", len(raw), STAGING_PATH.name)
    return str(STAGING_PATH)


def validate_task(**_context) -> str:
    if not STAGING_PATH.exists():
        # No staging file likely means no-new-data from extraction
        logger.info("validate_task: no staging file (likely no new data)")
        return "SKIP_NO_NEW_DATA"
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
        raise ValueError("validate_task: no usable rows to validate after extraction")
    missing = required - set(raw.columns)
    if missing:
        raise ValueError(f"Staging file missing columns: {missing}")
    if raw["symbol"].isna().any() or raw["trade_date"].isna().any():
        raise ValueError("Staging file has null symbol or trade_date")
    logger.info("validate_task passed for %s rows", len(raw))
    return str(STAGING_PATH)


def transform_task(**_context) -> str:
    if not STAGING_PATH.exists():
        logger.info("transform_task: skipping (no new data)")
        return "SKIP_NO_NEW_DATA"
    staging = pd.read_csv(STAGING_PATH)
    history = pd.DataFrame()
    if not staging.empty and "symbol" in staging.columns:
        history = fetch_raw_for_symbols(staging["symbol"].dropna().unique().tolist())
    metrics = compute_metrics_for_staging(staging, history)
    metrics.to_csv(METRICS_PATH, index=False)
    logger.info("transform_task wrote %s metric rows", len(metrics))
    return str(METRICS_PATH)


def load_task(**_context) -> int:
    if not STAGING_PATH.exists():
        logger.info("load_task: skipping (no new data)")
        return 0
    raw = pd.read_csv(STAGING_PATH)
    metrics = pd.read_csv(METRICS_PATH) if METRICS_PATH.exists() else pd.DataFrame()
    inserted_raw = load_raw(raw)
    load_transformed(metrics)
    logger.info("load_task inserted %s new raw rows", inserted_raw)
    return inserted_raw


def quality_check_task(**_context) -> dict:
    if not STAGING_PATH.exists():
        logger.info("quality_check_task: skipping (no new data)")
        return {"status": "skipped_no_new_data"}
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
        extract_result = extract_task()
        if extract_result == "NO_NEW_DATA":
            duration = time.perf_counter() - started
            if run_id is not None:
                try:
                    finish_pipeline_run(
                        run_id,
                        status="success",
                        symbols_processed=0,
                        rows_inserted=0,
                        duration_seconds=duration,
                    )
                except Exception:  # noqa: BLE001
                    logger.warning("Could not write pipeline_runs finish state")
            logger.info("Pipeline complete with no new data in %.2fs", duration)
            return {"status": "success", "no_new_data": True, "duration_seconds": duration}
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
            try:
                finish_pipeline_run(
                    run_id,
                    status="failed",
                    symbols_processed=0,
                    rows_inserted=0,
                    duration_seconds=duration,
                    error_message=str(exc),
                )
            except Exception:  # noqa: BLE001
                logger.warning("Could not write pipeline_runs finish state")
        logger.exception("Pipeline failed")
        raise


if __name__ == "__main__":
    result = run_pipeline()
    print("Pipeline complete.", result)
