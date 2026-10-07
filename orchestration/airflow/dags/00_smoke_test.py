from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("✅ Airflow is running, DAG parsing and execution OK.")

with DAG(
    dag_id="00_smoke_test",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["grontia", "smoke"],
) as dag:
    PythonOperator(task_id="hello", python_callable=hello)
