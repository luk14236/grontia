from __future__ import annotations

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from pathlib import Path
import yaml


CATALOG_PATH = Path("/usr/local/airflow/include/configs/datasets.yml")


def load_catalog() -> dict:
    if not CATALOG_PATH.exists():
        raise FileNotFoundError(f"Catalog not found at: {CATALOG_PATH}")
    return yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))


def print_dataset_plan(dataset: dict) -> None:
    # This is the placeholder that will later become:
    # - trigger databricks job (bronze)
    # - trigger databricks job (silver)
    # - optionally trigger dbt
    dataset_name = dataset["dataset_name"]
    table_id = dataset["table_id"]
    silver_table = dataset["silver_table"]

    endpoints = dataset.get("endpoints", {})
    bronze_endpoints = endpoints.get("bronze", [])
    dims_endpoints = endpoints.get("silver_dimensions", [])

    print("======================================")
    print(f"📦 Dataset: {dataset_name}")
    print(f"🔎 CBS table_id: {table_id}")
    print(f"🟦 Target silver table: {silver_table}")
    print(f"⬇️  Bronze endpoints: {bronze_endpoints}")
    print(f"🧩 Silver dimensions: {dims_endpoints}")

    cols = dataset.get("columns_en", {})
    print(f"🧾 Columns mapping count: {len(cols)}")
    if cols:
        sample = list(cols.items())[:8]
        print("🔤 Sample mapping:")
        for src, dst in sample:
            print(f"  - {src} -> {dst}")

    print("✅ Plan OK (stub). Replace this task with Databricks/dbt later.")
    print("======================================")


with DAG(
    dag_id="10_cbs_pipeline_v0",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["grondia", "cbs", "v0"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    catalog = load_catalog()
    datasets = catalog.get("cbs", {}).get("datasets", [])

    if not datasets:
        # If catalog is empty, still load DAG with a visible task that fails clearly
        raise ValueError("No CBS datasets found in catalog (cbs.datasets).")

    # Create one task per dataset (static at parse time, based on the YAML)
    tasks = []
    for ds in datasets:
        t = PythonOperator(
            task_id=f"plan__{ds['dataset_name']}",
            python_callable=print_dataset_plan,
            op_kwargs={"dataset": ds},
        )
        tasks.append(t)

    start >> tasks >> end
