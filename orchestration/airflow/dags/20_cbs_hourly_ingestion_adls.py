from __future__ import annotations

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup
from airflow.decorators import task

from datetime import datetime, timedelta
import json
import time
import yaml
import requests
import logging
import os
from urllib.parse import urlencode
from pathlib import Path

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

logger = logging.getLogger(__name__)

CBS_BASE = "https://opendata.cbs.nl/ODataApi/odata"
CATALOG_PATH = "/usr/local/airflow/include/configs/datasets.yml"

# ADLS config via env vars (Astro .env)
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "").strip()
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "").strip()
ADLS_BRONZE_FILESYSTEM = os.getenv("ADLS_BRONZE_FILESYSTEM", "bronze").strip()
ADLS_BRONZE_PREFIX = os.getenv("ADLS_BRONZE_PREFIX", "cbs/cbs_bronze").strip()

if not AZURE_STORAGE_ACCOUNT_NAME or not AZURE_STORAGE_ACCOUNT_KEY:
    logger.warning(
        "Missing AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_ACCOUNT_KEY. "
        "Set them in orchestration/airflow/.env"
    )

# Reduce Azure SDK log noise
logging.getLogger("azure").setLevel(logging.WARNING)


def load_catalog() -> dict:
    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def http_get_json(url: str, timeout: int = 60) -> dict:
    try:
        logger.info("GET %s", url)
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.error("HTTP %s for %s", r.status_code, url)
            logger.error("Response text (first 2000 chars): %s", r.text[:2000])
        r.raise_for_status()
        return r.json()
    except Exception:
        logger.exception("Request failed for url=%s", url)
        raise


def http_get_json_optional(url: str, timeout: int = 60) -> dict | None:
    """
    Returns None on 404 so we can skip optional endpoints like Regions/Perioden.
    """
    try:
        return http_get_json(url, timeout=timeout)
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status == 404:
            logger.warning("Optional endpoint not found (404), skipping: %s", url)
            return None
        raise


def adls_client() -> DataLakeServiceClient:
    account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=AZURE_STORAGE_ACCOUNT_KEY)


def adls_write_text(path_in_fs: str, content: str) -> None:
    """
    Write a text file to ADLS Gen2 (filesystem = container).
    path_in_fs is the full path inside the filesystem, without leading slash.

    This implementation is version-safe: delete if exists, then create, append, flush.
    """
    svc = adls_client()
    fs = svc.get_file_system_client(ADLS_BRONZE_FILESYSTEM)
    file_client = fs.get_file_client(path_in_fs)

    # Overwrite behavior: delete if exists, then create
    try:
        file_client.delete_file()
    except ResourceNotFoundError:
        pass

    try:
        file_client.create_file()
    except ResourceExistsError:
        # If a race happens, try delete again then create
        try:
            file_client.delete_file()
        except ResourceNotFoundError:
            pass
        file_client.create_file()

    file_client.append_data(content, offset=0)
    file_client.flush_data(len(content))


def get_ingestion_date() -> str:
    """Return current date in YYYY-MM-DD format for partitioning."""
    return datetime.now().strftime("%Y-%m-%d")


def get_ingestion_datetime() -> str:
    """Return current datetime in YYYY-MM-DD_HH-MM-SS format."""
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


@task
def check_new_data_available(table_id: str) -> bool:
    """
    Check if new data is available for the given table.
    This is a simplified check - in production you'd compare against last ingestion.
    """
    try:
        # Check if TableInfos endpoint is accessible (indicates data availability)
        url = f"{CBS_BASE}/{table_id}/TableInfos"
        response = http_get_json(url, timeout=30)
        return len(response.get("value", [])) > 0
    except Exception as e:
        logger.warning(f"Failed to check data availability for {table_id}: {e}")
        return False


@task
def ingest_cbs_dataset(table_id: str, dataset_name: str) -> dict:
    """
    Ingest a complete CBS dataset including all required endpoints.
    Returns metadata about what was ingested.
    """
    logger.info(f"Starting ingestion for {dataset_name} (table_id: {table_id})")

    catalog = load_catalog()
    dataset_config = None

    # Find dataset config
    for ds in catalog["cbs"]["datasets"]:
        if ds["table_id"] == table_id:
            dataset_config = ds
            break

    if not dataset_config:
        raise ValueError(f"Dataset {table_id} not found in catalog")

    ingestion_date = get_ingestion_date()
    ingestion_datetime = get_ingestion_datetime()

    base_path = f"{ADLS_BRONZE_PREFIX}/{dataset_name}/ingestion_date={ingestion_date}"

    # Summary metadata
    summary = {
        "table_id": table_id,
        "dataset_name": dataset_name,
        "ingestion_datetime": ingestion_datetime,
        "ingestion_date": ingestion_date,
        "endpoints_ingested": [],
        "total_records": 0,
        "status": "success"
    }

    try:
        # Ingest bronze endpoints
        bronze_endpoints = dataset_config.get("endpoints", {}).get("bronze", [])
        for endpoint in bronze_endpoints:
            logger.info(f"Ingesting endpoint: {endpoint}")
            url = f"{CBS_BASE}/{table_id}/{endpoint}"

            # For large datasets, we might need pagination
            data = http_get_json(url)

            # Write to ADLS
            file_path = f"{base_path}/{endpoint.lower()}.json"
            adls_write_text(file_path, json.dumps(data, indent=2, ensure_ascii=False))

            summary["endpoints_ingested"].append(endpoint)

            # Count records if available
            if "value" in data:
                summary["total_records"] += len(data["value"])

        # Write summary
        summary_path = f"{base_path}/_summary.json"
        adls_write_text(summary_path, json.dumps(summary, indent=2, ensure_ascii=False))

        logger.info(f"Successfully ingested {dataset_name}: {summary['total_records']} records")
        return summary

    except Exception as e:
        logger.exception(f"Failed to ingest {dataset_name}")
        summary["status"] = "failed"
        summary["error"] = str(e)

        # Write error summary
        summary_path = f"{base_path}/_summary.json"
        adls_write_text(summary_path, json.dumps(summary, indent=2, ensure_ascii=False))

        raise


@task
def validate_ingestion(summary: dict) -> None:
    """Validate that ingestion was successful."""
    if summary.get("status") != "success":
        raise ValueError(f"Ingestion failed: {summary.get('error', 'Unknown error')}")

    if summary.get("total_records", 0) == 0:
        logger.warning(f"No records ingested for {summary['dataset_name']}")

    logger.info(f"Validation passed for {summary['dataset_name']}: {summary['total_records']} records")


# Default DAG arguments
default_args = {
    "owner": "grondia",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,  # Don't backfill
}

# Create DAG that runs once a day
with DAG(
    dag_id="20_cbs_hourly_ingestion_adls",
    default_args=default_args,
    description="Daily ingestion of CBS datasets to Azure Data Lake Storage",
    schedule_interval="@daily",  # Once a day at midnight UTC
    max_active_runs=1,  # Only one instance running at a time
    catchup=False,
    tags=["grondia", "cbs", "bronze", "daily", "azure"],
) as dag:

    start_task = EmptyOperator(task_id="start")

    # Load catalog and get available datasets
    catalog = load_catalog()
    cbs_datasets = catalog["cbs"]["datasets"]

    # Create tasks for each dataset
    dataset_tasks = []
    for dataset in cbs_datasets:
        table_id = dataset["table_id"]
        dataset_name = dataset["dataset_name"]

        # Check if new data is available
        check_task = check_new_data_available(table_id)

        # Ingest dataset
        ingest_task = ingest_cbs_dataset(table_id, dataset_name)

        # Validate ingestion
        validate_task = validate_ingestion(ingest_task)

        # Chain tasks
        check_task >> ingest_task >> validate_task

        dataset_tasks.append(validate_task)

    end_task = EmptyOperator(task_id="end")

    # All dataset ingestions must complete before ending
    start_task >> dataset_tasks >> end_task