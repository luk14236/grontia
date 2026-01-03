from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="ndw_streaming_ingestion_v0",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ndw", "streaming", "grondia"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> end
