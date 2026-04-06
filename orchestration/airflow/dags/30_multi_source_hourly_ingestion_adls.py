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
from typing import Dict, List, Any

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

logger = logging.getLogger(__name__)

# Configuration
CATALOG_PATH = "/usr/local/airflow/include/configs/datasets.yml"

# ADLS config via env vars (Astro .env)
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "").strip()
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "").strip()
ADLS_BRONZE_FILESYSTEM = os.getenv("ADLS_BRONZE_FILESYSTEM", "bronze").strip()

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


def adls_client() -> DataLakeServiceClient:
    account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=AZURE_STORAGE_ACCOUNT_KEY)


def adls_write_text(path_in_fs: str, content: str) -> None:
    """
    Write a text file to ADLS Gen2 (filesystem = container).
    path_in_fs is the full path inside the filesystem, without leading slash.
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


# CBS-specific functions
CBS_BASE = "https://opendata.cbs.nl/ODataApi/odata"

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


@task
def check_cbs_data_available(table_id: str) -> bool:
    """Check if new CBS data is available."""
    try:
        url = f"{CBS_BASE}/{table_id}/TableInfos"
        response = http_get_json(url, timeout=30)
        return len(response.get("value", [])) > 0
    except Exception as e:
        logger.warning(f"Failed to check CBS data availability for {table_id}: {e}")
        return False


@task
def ingest_cbs_dataset(table_id: str, dataset_name: str) -> dict:
    """Ingest CBS dataset."""
    logger.info(f"Starting CBS ingestion for {dataset_name} (table_id: {table_id})")

    catalog = load_catalog()
    dataset_config = None

    for ds in catalog["cbs"]["datasets"]:
        if ds["table_id"] == table_id:
            dataset_config = ds
            break

    if not dataset_config:
        raise ValueError(f"CBS Dataset {table_id} not found in catalog")

    ingestion_date = get_ingestion_date()
    ingestion_datetime = get_ingestion_datetime()
    base_path = f"cbs/cbs_bronze/{dataset_name}/ingestion_date={ingestion_date}"

    summary = {
        "source": "cbs",
        "table_id": table_id,
        "dataset_name": dataset_name,
        "ingestion_datetime": ingestion_datetime,
        "ingestion_date": ingestion_date,
        "endpoints_ingested": [],
        "total_records": 0,
        "status": "success"
    }

    try:
        bronze_endpoints = dataset_config.get("endpoints", {}).get("bronze", [])
        for endpoint in bronze_endpoints:
            logger.info(f"Ingesting CBS endpoint: {endpoint}")
            url = f"{CBS_BASE}/{table_id}/{endpoint}"
            data = http_get_json(url)

            file_path = f"{base_path}/{endpoint.lower()}.json"
            adls_write_text(file_path, json.dumps(data, indent=2, ensure_ascii=False))

            summary["endpoints_ingested"].append(endpoint)
            if "value" in data:
                summary["total_records"] += len(data["value"])

        summary_path = f"{base_path}/_summary.json"
        adls_write_text(summary_path, json.dumps(summary, indent=2, ensure_ascii=False))

        logger.info(f"Successfully ingested CBS {dataset_name}: {summary['total_records']} records")
        return summary

    except Exception as e:
        logger.exception(f"Failed to ingest CBS {dataset_name}")
        summary["status"] = "failed"
        summary["error"] = str(e)
        summary_path = f"{base_path}/_summary.json"
        adls_write_text(summary_path, json.dumps(summary, indent=2, ensure_ascii=False))
        raise


# PDOK-specific functions
PDOK_BASE = "https://service.pdok.nl"

@task
def check_pdok_data_available(dataset_name: str) -> bool:
    """Check if new PDOK data is available."""
    try:
        # PDOK uses different APIs, this is a placeholder
        # In real implementation, check PDOK service status
        logger.info(f"Checking PDOK data availability for {dataset_name}")
        return True  # Placeholder - implement actual check
    except Exception as e:
        logger.warning(f"Failed to check PDOK data availability for {dataset_name}: {e}")
        return False


@task
def ingest_pdok_dataset(dataset_name: str) -> dict:
    """Ingest PDOK dataset."""
    logger.info(f"Starting PDOK ingestion for {dataset_name}")

    ingestion_date = get_ingestion_date()
    ingestion_datetime = get_ingestion_datetime()
    base_path = f"pdok/pdok_bronze/{dataset_name}/ingestion_date={ingestion_date}"

    summary = {
        "source": "pdok",
        "dataset_name": dataset_name,
        "ingestion_datetime": ingestion_datetime,
        "ingestion_date": ingestion_date,
        "files_ingested": [],
        "total_records": 0,
        "status": "success"
    }

    try:
        # Placeholder PDOK ingestion logic
        # In real implementation, download from PDOK services
        logger.warning(f"PDOK ingestion for {dataset_name} is placeholder - implement actual logic")

        summary_path = f"{base_path}/_summary.json"
        adls_write_text(summary_path, json.dumps(summary, indent=2, ensure_ascii=False))

        logger.info(f"PDOK ingestion placeholder completed for {dataset_name}")
        return summary

    except Exception as e:
        logger.exception(f"Failed to ingest PDOK {dataset_name}")
        summary["status"] = "failed"
        summary["error"] = str(e)
        summary_path = f"{base_path}/_summary.json"
        adls_write_text(summary_path, json.dumps(summary, indent=2, ensure_ascii=False))
        raise


# KNMI-specific functions
KNMI_BASE = "https://www.daggegevens.knmi.nl"

@task
def check_knmi_data_available(dataset_name: str) -> bool:
    """Check if new KNMI data is available."""
    try:
        # KNMI uses different APIs, this is a placeholder
        logger.info(f"Checking KNMI data availability for {dataset_name}")
        return True  # Placeholder - implement actual check
    except Exception as e:
        logger.warning(f"Failed to check KNMI data availability for {dataset_name}: {e}")
        return False


@task
def ingest_knmi_dataset(dataset_name: str) -> dict:
    """Ingest KNMI dataset."""
    logger.info(f"Starting KNMI ingestion for {dataset_name}")

    ingestion_date = get_ingestion_date()
    ingestion_datetime = get_ingestion_datetime()
    base_path = f"knmi/knmi_bronze/{dataset_name}/ingestion_date={ingestion_date}"

    summary = {
        "source": "knmi",
        "dataset_name": dataset_name,
        "ingestion_datetime": ingestion_datetime,
        "ingestion_date": ingestion_date,
        "files_ingested": [],
        "total_records": 0,
        "status": "success"
    }

    try:
        # Placeholder KNMI ingestion logic
        logger.warning(f"KNMI ingestion for {dataset_name} is placeholder - implement actual logic")

        summary_path = f"{base_path}/_summary.json"
        adls_write_text(summary_path, json.dumps(summary, indent=2, ensure_ascii=False))

        logger.info(f"KNMI ingestion placeholder completed for {dataset_name}")
        return summary

    except Exception as e:
        logger.exception(f"Failed to ingest KNMI {dataset_name}")
        summary["status"] = "failed"
        summary["error"] = str(e)
        summary_path = f"{base_path}/_summary.json"
        adls_write_text(summary_path, json.dumps(summary, indent=2, ensure_ascii=False))
        raise


# NDW-specific functions
NDW_BASE = "https://opendata.ndw.nu"

@task
def check_ndw_data_available(dataset_name: str) -> bool:
    """Check if new NDW data is available."""
    try:
        # NDW uses different APIs, this is a placeholder
        logger.info(f"Checking NDW data availability for {dataset_name}")
        return True  # Placeholder - implement actual check
    except Exception as e:
        logger.warning(f"Failed to check NDW data availability for {dataset_name}: {e}")
        return False


@task
def ingest_ndw_dataset(dataset_name: str) -> dict:
    """Ingest NDW dataset."""
    logger.info(f"Starting NDW ingestion for {dataset_name}")

    ingestion_date = get_ingestion_date()
    ingestion_datetime = get_ingestion_datetime()
    base_path = f"ndw/ndw_bronze/{dataset_name}/ingestion_date={ingestion_date}"

    summary = {
        "source": "ndw",
        "dataset_name": dataset_name,
        "ingestion_datetime": ingestion_datetime,
        "ingestion_date": ingestion_date,
        "files_ingested": [],
        "total_records": 0,
        "status": "success"
    }

    try:
        # Placeholder NDW ingestion logic
        logger.warning(f"NDW ingestion for {dataset_name} is placeholder - implement actual logic")

        summary_path = f"{base_path}/_summary.json"
        adls_write_text(summary_path, json.dumps(summary, indent=2, ensure_ascii=False))

        logger.info(f"NDW ingestion placeholder completed for {dataset_name}")
        return summary

    except Exception as e:
        logger.exception(f"Failed to ingest NDW {dataset_name}")
        summary["status"] = "failed"
        summary["error"] = str(e)
        summary_path = f"{base_path}/_summary.json"
        adls_write_text(summary_path, json.dumps(summary, indent=2, ensure_ascii=False))
        raise


@task
def validate_ingestion(summary: dict) -> None:
    """Validate that ingestion was successful."""
    if summary.get("status") != "success":
        raise ValueError(f"Ingestion failed: {summary.get('error', 'Unknown error')}")

    source = summary.get("source", "unknown")
    dataset_name = summary.get("dataset_name", "unknown")
    logger.info(f"Validation passed for {source} {dataset_name}")


# Default DAG arguments
default_args = {
    "owner": "grondia",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

# Create DAG that runs once a day
with DAG(
    dag_id="30_multi_source_hourly_ingestion_adls",
    default_args=default_args,
    description="Daily ingestion from all data sources (CBS, PDOK, KNMI, NDW) to Azure Data Lake Storage",
    schedule_interval="@daily",  # Once a day at midnight UTC
    max_active_runs=1,
    catchup=False,
    tags=["grondia", "multi-source", "bronze", "daily", "azure"],
) as dag:

    start_task = EmptyOperator(task_id="start")

    # Load catalog and get all datasets from all sources
    catalog = load_catalog()

    # Create tasks for each data source
    all_tasks = []

    # CBS datasets
    if "cbs" in catalog and "datasets" in catalog["cbs"]:
        for dataset in catalog["cbs"]["datasets"]:
            table_id = dataset["table_id"]
            dataset_name = dataset["dataset_name"]

            check_task = check_cbs_data_available(table_id)
            ingest_task = ingest_cbs_dataset(table_id, dataset_name)
            validate_task = validate_ingestion(ingest_task)

            check_task >> ingest_task >> validate_task
            all_tasks.append(validate_task)

    # PDOK datasets (placeholder)
    if "pdok" in catalog and "datasets" in catalog["pdok"]:
        for dataset in catalog["pdok"]["datasets"]:
            dataset_name = dataset["dataset_name"]

            check_task = check_pdok_data_available(dataset_name)
            ingest_task = ingest_pdok_dataset(dataset_name)
            validate_task = validate_ingestion(ingest_task)

            check_task >> ingest_task >> validate_task
            all_tasks.append(validate_task)

    # KNMI datasets (placeholder)
    if "knmi" in catalog and "datasets" in catalog["knmi"]:
        for dataset in catalog["knmi"]["datasets"]:
            dataset_name = dataset["dataset_name"]

            check_task = check_knmi_data_available(dataset_name)
            ingest_task = ingest_knmi_dataset(dataset_name)
            validate_task = validate_ingestion(ingest_task)

            check_task >> ingest_task >> validate_task
            all_tasks.append(validate_task)

    # NDW datasets (placeholder)
    if "ndw" in catalog and "datasets" in catalog["ndw"]:
        for dataset in catalog["ndw"]["datasets"]:
            dataset_name = dataset["dataset_name"]

            check_task = check_ndw_data_available(dataset_name)
            ingest_task = ingest_ndw_dataset(dataset_name)
            validate_task = validate_ingestion(ingest_task)

            check_task >> ingest_task >> validate_task
            all_tasks.append(validate_task)

    end_task = EmptyOperator(task_id="end")

    start_task >> all_tasks >> end_task