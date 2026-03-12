from __future__ import annotations

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup

from datetime import datetime, timedelta
import json
import yaml
import requests
import logging
import os

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

logger = logging.getLogger(__name__)

CATALOG_PATH = "/usr/local/airflow/include/configs/datasets.yml"

AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "").strip()
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "").strip()
ADLS_BRONZE_FILESYSTEM = os.getenv("ADLS_BRONZE_FILESYSTEM", "bronze").strip()
ADLS_BRONZE_PREFIX = os.getenv("ADLS_BRONZE_PREFIX", "knmi/knmi_bronze").strip()

if not AZURE_STORAGE_ACCOUNT_NAME or not AZURE_STORAGE_ACCOUNT_KEY:
    logger.warning(
        "Missing AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_ACCOUNT_KEY. "
        "Set them in orchestration/airflow/.env"
    )

logging.getLogger("azure").setLevel(logging.WARNING)


def load_catalog() -> dict:
    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def format_yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def http_post_text(url: str, data: dict, timeout: int = 120) -> str:
    try:
        logger.info("POST %s with data=%s", url, data)
        response = requests.post(url, data=data, timeout=timeout)
        if response.status_code >= 400:
            logger.error("HTTP %s for %s", response.status_code, url)
            logger.error("Response text (first 2000 chars): %s", response.text[:2000])
        response.raise_for_status()
        return response.text
    except Exception:
        logger.exception("Request failed for url=%s", url)
        raise


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


def infer_extension(file_format: str) -> str:
    normalized = file_format.strip().lower()
    if normalized == "json":
        return "json"
    if normalized == "xml":
        return "xml"
    return "csv"


def count_data_lines(raw_text: str) -> int:
    lines = raw_text.splitlines()
    data_lines = [
        line for line in lines
        if line.strip() and not line.startswith("#")
    ]
    return len(data_lines)


def ingest_knmi_dataset(dataset: dict, ingestion_date: str) -> None:
    dataset_name = dataset["dataset_name"]
    endpoint = dataset["endpoint"]
    file_format = dataset.get("format", "csv")
    params = dataset.get("params", {})

    today = datetime.utcnow().date()

    start_days_ago = int(params.get("start_days_ago", 7))
    end_days_ago = int(params.get("end_days_ago", 1))

    start_date = format_yyyymmdd(datetime.combine(today - timedelta(days=start_days_ago), datetime.min.time()))
    end_date = format_yyyymmdd(datetime.combine(today - timedelta(days=end_days_ago), datetime.min.time()))

    post_data = {
        "start": start_date,
        "end": end_date,
        "fmt": file_format,
        "vars": params.get("vars", "ALL"),
        "stns": params.get("stns", "ALL"),
    }

    raw_text = http_post_text(endpoint, post_data)

    if not raw_text.strip():
        raise ValueError(f"KNMI dataset {dataset_name} returned empty response")

    base_dir = f"{ADLS_BRONZE_PREFIX}/{dataset_name}/ingestion_date={ingestion_date}"
    extension = infer_extension(file_format)

    raw_path = f"{base_dir}/raw_data.{extension}"
    adls_write_text(raw_path, raw_text)

    summary = {
        "dataset_name": dataset_name,
        "source": "knmi",
        "ingestion_date": ingestion_date,
        "request": {
            "endpoint": endpoint,
            "method": "POST",
            "payload": post_data,
        },
        "adls": {
            "filesystem": ADLS_BRONZE_FILESYSTEM,
            "base_dir": base_dir,
        },
        "paths": {
            "raw": raw_path,
        },
        "counts": {
            "non_comment_lines": count_data_lines(raw_text),
            "raw_bytes": len(raw_text.encode("utf-8")),
        },
    }

    adls_write_text(
        f"{base_dir}/_summary.json",
        json.dumps(summary, ensure_ascii=False, indent=2),
    )

    logger.info("✅ Ingested KNMI dataset %s to ADLS %s", dataset_name, base_dir)
    print(summary)


with DAG(
    dag_id="14_knmi_bronze_ingestion_adls",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["grondia", "knmi", "bronze", "adls"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    catalog = load_catalog()
    datasets = catalog.get("knmi", {}).get("datasets", [])
    if not datasets:
        raise ValueError("No KNMI datasets found in catalog (knmi.datasets).")

    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")

    with TaskGroup(group_id="knmi_datasets") as tg:
        for ds in datasets:
            PythonOperator(
                task_id=f"ingest__{ds['dataset_name']}",
                python_callable=ingest_knmi_dataset,
                op_kwargs={"dataset": ds, "ingestion_date": ingestion_date},
            )

    start >> tg >> end