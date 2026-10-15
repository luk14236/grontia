"""
Bronze -> silver (PySpark) -> marts (dbt), triggered when DAG 30 publishes
the BRONZE_DAILY asset.

Reprocess a specific date by triggering manually with
{"ingestion_date": "YYYY-MM-DD"}.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

from grontia_assets import BRONZE_DAILY

logger = logging.getLogger(__name__)

PROCESSING_DIR = "/opt/grontia/processing/local"
DBT_DIR        = "/opt/grontia/dbt"


@task
def resolve_ingestion_date(*, params, triggering_asset_events) -> str:
    """Manual param first, then the date sent by DAG 30, then today (UTC)."""
    if params.get("ingestion_date"):
        return params["ingestion_date"]
    for events in triggering_asset_events.values():
        for event in events:
            if event.extra.get("ingestion_date"):
                return event.extra["ingestion_date"]
    return datetime.utcnow().strftime("%Y-%m-%d")


default_args = {
    "owner": "grontia",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="40_silver_and_marts",
    default_args=default_args,
    description="Bronze -> silver (PySpark) -> dbt marts, after each bronze ingestion",
    schedule=[BRONZE_DAILY],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    params={
        "ingestion_date": Param(
            None, type=["null", "string"],
            description="YYYY-MM-DD to reprocess; empty = date sent by DAG 30",
        ),
    },
    tags=["grontia", "silver", "dbt"],
) as dag:

    ing_date = resolve_ingestion_date()

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROCESSING_DIR} && "
            "$PIPELINE_VENV/bin/python bronze_to_silver.py "
            "{{ ti.xcom_pull(task_ids='resolve_ingestion_date') }}"
        ),
        execution_timeout=timedelta(hours=1),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {DBT_DIR} && $PIPELINE_VENV/bin/dbt build "
            "--profiles-dir . --target $GRONTIA_DBT_TARGET "
            "--target-path /tmp/dbt/target --log-path /tmp/dbt/logs"
        ),
        execution_timeout=timedelta(hours=1),
    )

    ing_date >> bronze_to_silver >> dbt_build
