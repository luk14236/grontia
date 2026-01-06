from __future__ import annotations

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup

from datetime import datetime
from pathlib import Path
import json
import time
import yaml
import requests
import logging
from urllib.parse import urlencode

logger = logging.getLogger(__name__)


CBS_BASE = "https://opendata.cbs.nl/ODataApi/odata"
CATALOG_PATH = Path("/usr/local/airflow/include/configs/datasets.yml")

# Inside the container, this is where your repo root is mounted in Astro.
# Astro project lives at /usr/local/airflow, but your Windows repo isn't mounted fully.
# We'll write to /usr/local/airflow/include/output for now (mounted), then you can copy out.
OUTPUT_BASE = Path("/usr/local/airflow/include/output/cbs_bronze")


def load_catalog() -> dict:
    return yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))


def http_get_json(url: str, timeout: int = 60) -> dict:
    try:
        logger.info("GET %s", url)
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            # Log response body because CBS returns helpful messages
            logger.error("HTTP %s for %s", r.status_code, url)
            logger.error("Response text (first 2000 chars): %s", r.text[:2000])
        r.raise_for_status()
        return r.json()
    except Exception:
        logger.exception("Request failed for url=%s", url)
        raise


def fetch_paged_values(first_url: str, timeout: int = 60, page_size: int = 10000, sleep_s: float = 0.0) -> list[dict]:
    base_url = first_url.split("?", 1)[0]

    def run_with_pagesize(ps: int) -> list[dict]:
        all_rows: list[dict] = []
        skip = 0
        while True:
            params = {"$top": ps, "$skip": skip}
            url = f"{base_url}?{urlencode(params)}"
            payload = http_get_json(url, timeout=timeout)
            rows = payload.get("value", [])
            all_rows.extend(rows)

            if sleep_s:
                time.sleep(sleep_s)

            # Stop condition: last page
            if len(rows) < ps:
                break
            skip += ps
        return all_rows

    # Try progressively smaller pages if CBS rejects
    for ps in [page_size, 5000, 2000, 1000]:
        try:
            logger.info("Fetching pages with page_size=%s for %s", ps, base_url)
            return run_with_pagesize(ps)
        except requests.HTTPError as e:
            # CBS sometimes replies 500 with a descriptive message, try smaller.
            logger.warning("Failed with page_size=%s for %s, error=%s", ps, base_url, str(e))
            continue

    # If all attempts fail, raise the last error
    return run_with_pagesize(500)  # last resort (will likely raise)


def ingest_dataset_bronze(dataset: dict, ingestion_date: str) -> None:
    dataset_name = dataset["dataset_name"]
    table_id = dataset["table_id"]

    out_dir = OUTPUT_BASE / dataset_name / f"ingestion_date={ingestion_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) TableInfos
    tableinfos_url = f"{CBS_BASE}/{table_id}/TableInfos"
    tableinfos = fetch_paged_values(tableinfos_url)
    (out_dir / "table_infos.json").write_text(json.dumps(tableinfos, ensure_ascii=False, indent=2), encoding="utf-8")

    # 2) DataProperties
    dataprops_url = f"{CBS_BASE}/{table_id}/DataProperties"
    dataprops = fetch_paged_values(dataprops_url)
    (out_dir / "data_properties.json").write_text(json.dumps(dataprops, ensure_ascii=False, indent=2), encoding="utf-8")

    # 3) TypedDataSet (paged)
    typed_url = f"{CBS_BASE}/{table_id}/TypedDataSet"
    typed_rows = fetch_paged_values(typed_url)
    (out_dir / "typed_dataset.json").write_text(json.dumps(typed_rows, ensure_ascii=False), encoding="utf-8")

    # 4) Regions
    regions_url = f"{CBS_BASE}/{table_id}/Regions"
    regions = fetch_paged_values(regions_url, page_size=10000)
    (out_dir / "regions.json").write_text(json.dumps(regions, ensure_ascii=False), encoding="utf-8")

    # 5) Perioden
    perioden_url = f"{CBS_BASE}/{table_id}/Perioden"
    perioden = fetch_paged_values(perioden_url, page_size=10000)
    (out_dir / "perioden.json").write_text(json.dumps(perioden, ensure_ascii=False), encoding="utf-8")


    # small run summary
    summary = {
        "dataset_name": dataset_name,
        "table_id": table_id,
        "ingestion_date": ingestion_date,
        "counts": {
            "table_infos": len(tableinfos),
            "data_properties": len(dataprops),
            "typed_dataset": len(typed_rows),
        },
        "output_dir": str(out_dir),
    }
    (out_dir / "_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"✅ Ingested {dataset_name} ({table_id}) to {out_dir}")
    print(summary)


with DAG(
    dag_id="11_cbs_bronze_ingestion_local",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["grondia", "cbs", "bronze", "mvp"],
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
