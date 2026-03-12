from __future__ import annotations

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup

from datetime import datetime
import json
import yaml
import logging
import os
import requests

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

logger = logging.getLogger(__name__)

CATALOG_PATH = "/usr/local/airflow/include/configs/datasets.yml"

AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "").strip()
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "").strip()
ADLS_BRONZE_FILESYSTEM = os.getenv("ADLS_BRONZE_FILESYSTEM", "bronze").strip()
ADLS_BRONZE_PREFIX = os.getenv("ADLS_BRONZE_PREFIX", "bronze").strip()

logging.getLogger("azure").setLevel(logging.WARNING)


def load_catalog() -> dict:
    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def http_get_json(url: str, timeout: int = 60) -> dict:
    logger.info("GET %s", url)
    response = requests.get(url, timeout=timeout)
    if response.status_code >= 400:
        logger.error("HTTP %s for %s", response.status_code, url)
        logger.error("Response text (first 2000 chars): %s", response.text[:2000])
    response.raise_for_status()
    return response.json()


def adls_client() -> DataLakeServiceClient:
    account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=AZURE_STORAGE_ACCOUNT_KEY)


def adls_write_text(path_in_fs: str, content: str) -> None:
    svc = adls_client()
    fs = svc.get_file_system_client(ADLS_BRONZE_FILESYSTEM)
    file_client = fs.get_file_client(path_in_fs)

    try:
        file_client.delete_file()
    except ResourceNotFoundError:
        pass

    try:
        file_client.create_file()
    except ResourceExistsError:
        try:
            file_client.delete_file()
        except Exception:
            pass
        file_client.create_file()

    file_client.append_data(data=content, offset=0, length=len(content))
    file_client.flush_data(len(content))


def ingest_pdok_dataset(dataset: dict, ingestion_date: str) -> None:
    dataset_name = dataset["dataset_name"]
    url = dataset["url"]

    base_dir = f"{ADLS_BRONZE_PREFIX}/pdok/{dataset_name}/ingestion_date={ingestion_date}"

    payload = http_get_json(url)

    features = payload.get("features", [])
    if "features" not in payload:
        raise ValueError(f"PDOK dataset {dataset_name} does not contain 'features'")

    raw_path = f"{base_dir}/data.json"
    adls_write_text(raw_path, json.dumps(payload, ensure_ascii=False, indent=2))

    summary = {
        "source": "pdok",
        "dataset_name": dataset_name,
        "ingestion_date": ingestion_date,
        "adls": {
            "filesystem": ADLS_BRONZE_FILESYSTEM,
            "base_dir": base_dir,
        },
        "paths": {
            "raw": raw_path,
        },
        "counts": {
            "features": len(features),
        },
    }

    adls_write_text(f"{base_dir}/_summary.json", json.dumps(summary, ensure_ascii=False, indent=2))
    logger.info("✅ Ingested PDOK dataset %s to %s", dataset_name, base_dir)


with DAG(
    dag_id="13_pdok_bronze_ingestion_adls",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["grondia", "pdok", "bronze", "adls"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    catalog = load_catalog()
    datasets = catalog.get("pdok", {}).get("datasets", [])
    if not datasets:
        raise ValueError("No PDOK datasets found in catalog (pdok.datasets).")

    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")

    with TaskGroup(group_id="pdok_datasets") as tg:
        for ds in datasets:
            PythonOperator(
                task_id=f"ingest__{ds['dataset_name']}",
                python_callable=ingest_pdok_dataset,
                op_kwargs={"dataset": ds, "ingestion_date": ingestion_date},
            )

    start >> tg >> end