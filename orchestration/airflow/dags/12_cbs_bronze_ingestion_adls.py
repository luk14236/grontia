from __future__ import annotations

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup

from datetime import datetime
import json
import time
import yaml
import requests
import logging
import os
from urllib.parse import urlencode

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
        except Exception:
            pass
        file_client.create_file()

    # Write content (append + flush)
    file_client.append_data(data=content, offset=0, length=len(content))
    file_client.flush_data(len(content))


def paged_rows(
    base_url: str,
    timeout: int = 60,
    page_size: int = 10000,
    sleep_s: float = 0.0,
    optional_404: bool = False,
):
    """
    Generator that yields (page_index, rows) for $top/$skip pagination.
    If optional_404=True and endpoint returns 404, yields nothing and returns.
    """
    page_idx = 0
    skip = 0

    while True:
        params = {"$top": page_size, "$skip": skip}
        url = f"{base_url}?{urlencode(params)}"

        payload = http_get_json_optional(url, timeout=timeout) if optional_404 else http_get_json(url, timeout=timeout)
        if payload is None:
            return

        rows = payload.get("value", [])
        yield page_idx, rows

        if sleep_s:
            time.sleep(sleep_s)

        if len(rows) < page_size:
            break

        skip += page_size
        page_idx += 1


def fetch_all_rows(
    base_url: str,
    timeout: int = 60,
    page_size: int = 10000,
    sleep_s: float = 0.0,
    optional_404: bool = False,
) -> list[dict]:
    all_rows: list[dict] = []
    for _, rows in paged_rows(
        base_url,
        timeout=timeout,
        page_size=page_size,
        sleep_s=sleep_s,
        optional_404=optional_404,
    ):
        all_rows.extend(rows)
    return all_rows


def ingest_dataset_bronze(dataset: dict, ingestion_date: str) -> None:
    dataset_name = dataset["dataset_name"]
    table_id = dataset["table_id"]

    base_dir = f"{ADLS_BRONZE_PREFIX}/{dataset_name}/ingestion_date={ingestion_date}"

    def write_json(name: str, rows: list[dict]) -> str:
        path = f"{base_dir}/{name}"
        adls_write_text(path, json.dumps(rows, ensure_ascii=False, indent=2))
        return path

    # 1) TableInfos
    tableinfos_url = f"{CBS_BASE}/{table_id}/TableInfos"
    tableinfos = fetch_all_rows(tableinfos_url, page_size=2000)
    tableinfos_path = write_json("table_infos.json", tableinfos)

    # 2) DataProperties
    dataprops_url = f"{CBS_BASE}/{table_id}/DataProperties"
    dataprops = fetch_all_rows(dataprops_url, page_size=5000)
    dataprops_path = write_json("data_properties.json", dataprops)

    # 3) TypedDataSet as paged parts (Option A)
    typed_base = f"{CBS_BASE}/{table_id}/TypedDataSet"
    typed_total = 0
    typed_parts = 0
    typed_errors: list[str] = []

    for ps in [10000, 5000, 2000, 1000, 500]:
        try:
            logger.info("Fetching TypedDataSet with page_size=%s for %s", ps, typed_base)
            typed_total = 0
            typed_parts = 0

            for page_idx, rows in paged_rows(typed_base, page_size=ps):
                if page_idx == 0 and not rows:
                    break

                typed_total += len(rows)
                part_path = f"{base_dir}/typed_dataset/part-{page_idx:05d}.json"
                adls_write_text(part_path, json.dumps(rows, ensure_ascii=False))
                typed_parts += 1

            break

        except requests.HTTPError as e:
            msg = f"TypedDataSet failed with page_size={ps}, error={str(e)}"
            logger.warning(msg)
            typed_errors.append(msg)
            continue

    if typed_parts == 0 and typed_errors:
        raise RuntimeError("TypedDataSet could not be fetched after retries. " + " | ".join(typed_errors))

    # 4) Regions (optional)
    regions_url = f"{CBS_BASE}/{table_id}/Regions"
    regions = fetch_all_rows(regions_url, page_size=10000, optional_404=True)
    regions_path = write_json("regions.json", regions)

    # 5) Perioden (optional)
    perioden_url = f"{CBS_BASE}/{table_id}/Perioden"
    perioden = fetch_all_rows(perioden_url, page_size=10000, optional_404=True)
    perioden_path = write_json("perioden.json", perioden)

    summary = {
        "dataset_name": dataset_name,
        "table_id": table_id,
        "ingestion_date": ingestion_date,
        "adls": {"filesystem": ADLS_BRONZE_FILESYSTEM, "base_dir": base_dir},
        "paths": {
            "table_infos": tableinfos_path,
            "data_properties": dataprops_path,
            "regions": regions_path,
            "perioden": perioden_path,
            "typed_dataset_prefix": f"{base_dir}/typed_dataset/",
        },
        "counts": {
            "table_infos": len(tableinfos),
            "data_properties": len(dataprops),
            "regions": len(regions),
            "perioden": len(perioden),
            "typed_dataset_rows": typed_total,
            "typed_dataset_parts": typed_parts,
        },
    }

    adls_write_text(f"{base_dir}/_summary.json", json.dumps(summary, ensure_ascii=False, indent=2))

    logger.info("✅ Ingested %s (%s) to ADLS %s", dataset_name, table_id, base_dir)
    print(summary)


with DAG(
    dag_id="12_cbs_bronze_ingestion_adls",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["grondia", "cbs", "bronze", "adls"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    catalog = load_catalog()
    datasets = catalog.get("cbs", {}).get("datasets", [])
    if not datasets:
        raise ValueError("No CBS datasets found in catalog (cbs.datasets).")

    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")

    with TaskGroup(group_id="cbs_datasets") as tg:
        for ds in datasets:
            PythonOperator(
                task_id=f"ingest__{ds['dataset_name']}",
                python_callable=ingest_dataset_bronze,
                op_kwargs={"dataset": ds, "ingestion_date": ingestion_date},
            )

    start >> tg >> end
