"""Airflow DAG: weekday run after US market close."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline import (
    extract_task,
    load_task,
    quality_check_task,
    transform_task,
    validate_task,
)

default_args = {
    "owner": "depi-financial-pipeline",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="financial_pipeline_dag",
    description="Extract, validate, transform, and load daily equity OHLCV data",
    default_args=default_args,
    start_date=datetime(2026, 10, 26),
    schedule="0 22 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["depi", "finance", "etl"],
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=extract_task)
    validate = PythonOperator(task_id="validate", python_callable=validate_task)
    transform = PythonOperator(task_id="transform", python_callable=transform_task)
    load = PythonOperator(task_id="load", python_callable=load_task)
    quality_check = PythonOperator(task_id="quality_check", python_callable=quality_check_task)

    extract >> validate >> transform >> load >> quality_check
