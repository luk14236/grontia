from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="pdok_batch_ingestion_v0",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["pdok", "batch", "grondia"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> end
