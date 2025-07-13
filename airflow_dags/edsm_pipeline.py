"""Airflow DAG to run the EDSM ingest pipeline without Databricks Jobs."""

from __future__ import annotations

import glob
import json
from datetime import datetime
from pathlib import Path
from subprocess import check_call

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from functions.utility import create_spark_session, apply_job_type

from functions.sanity import (
    validate_settings,
    initialize_schemas_and_volumes,
    initialize_empty_tables,
)


def _generate_job_settings() -> dict:
    """Load settings files for each layer and return a dictionary."""
    job_settings = {"bronze": [], "silver": []}
    for color in ["bronze", "silver"]:
        for path in glob.glob(f"layer_*_{color}/*.json"):
            table = Path(path).stem
            with open(path) as f:
                settings = json.load(f)
            settings = apply_job_type(settings)
            job_settings[color].append({"table": table})
    return job_settings


def _init_job() -> dict:
    """Validate settings and create schemas/volumes."""
    spark = create_spark_session("local[*]", "edsm-job-settings")
    try:
        validate_settings()
        initialize_schemas_and_volumes(spark)
        initialize_empty_tables(spark)
    finally:
        spark.stop()
    return _generate_job_settings()


def _run_ingest(color: str, table: str) -> None:
    """Invoke ``scripts/run_ingest.py`` for the given table."""
    check_call(["python", "scripts/run_ingest.py", color, table])


def _bronze_ingest(ti) -> None:
    job_settings = ti.xcom_pull(task_ids="job_settings")
    for item in job_settings["bronze"]:
        _run_ingest("bronze", item["table"])


def _silver_ingest(ti) -> None:
    job_settings = ti.xcom_pull(task_ids="job_settings")
    for item in job_settings["silver"]:
        _run_ingest("silver", item["table"])


default_args = {"start_date": datetime(2023, 1, 1)}

with DAG(
    "edsm_pipeline",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:
    downloader = BashOperator(
        task_id="downloader",
        bash_command="python utilities/downloader.py",
    )

    job_settings_task = PythonOperator(
        task_id="job_settings",
        python_callable=_init_job,
    )

    bronze_task = PythonOperator(
        task_id="bronze_loop",
        python_callable=_bronze_ingest,
    )

    silver_task = PythonOperator(
        task_id="silver_loop",
        python_callable=_silver_ingest,
    )

    downloader >> job_settings_task >> bronze_task >> silver_task
