from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
import yaml

def read_catalog():
    catalog_path = Path("/usr/local/airflow/include/configs/datasets.yml")
    data = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))

    datasets = data["cbs"]["datasets"]
    print("📌 CBS datasets:")
    for d in datasets:
        print(f"- {d['dataset_name']} (table_id={d['table_id']}, silver={d['silver_table']})")

with DAG(
    dag_id="01_read_datasets_catalog",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["grondia", "catalog"],
) as dag:
    PythonOperator(
        task_id="read_catalog",
        python_callable=read_catalog
    )
