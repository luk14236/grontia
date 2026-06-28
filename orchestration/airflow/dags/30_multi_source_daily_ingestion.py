from __future__ import annotations

import gzip
import io
import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any

import boto3
import requests
import yaml
from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)

CATALOG_PATH = "/opt/airflow/include/configs/datasets.yml"

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_BRONZE   = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

CBS_BASE = "https://opendata.cbs.nl/ODataApi/odata"


def load_catalog() -> dict:
    with open(CATALOG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )


def minio_put(key: str, body: str) -> None:
    s3_client().put_object(
        Bucket=BUCKET_BRONZE,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )


def http_get_json(url: str, timeout: int = 60) -> dict:
    logger.info("GET %s", url)
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def http_get_json_optional(url: str, timeout: int = 60) -> dict | None:
    try:
        return http_get_json(url, timeout=timeout)
    except requests.HTTPError as e:
        if getattr(e.response, "status_code", None) == 404:
            logger.warning("Optional endpoint 404, skipping: %s", url)
            return None
        raise


def ingestion_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def ingestion_datetime() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


# ---------------------------------------------------------------------------
# CBS
# ---------------------------------------------------------------------------

@task
def ingest_cbs_dataset(table_id: str, dataset_name: str) -> dict:
    logger.info("CBS ingestion: %s (%s)", dataset_name, table_id)

    catalog = load_catalog()
    dataset_config = next(
        (d for d in catalog["cbs"]["datasets"] if d["table_id"] == table_id), None
    )
    if not dataset_config:
        raise ValueError(f"CBS dataset {table_id} not found in catalog")

    ing_date = ingestion_date()
    ing_dt   = ingestion_datetime()
    base     = f"cbs/cbs_bronze/{dataset_name}/ingestion_date={ing_date}"

    summary: dict[str, Any] = {
        "source": "cbs", "table_id": table_id,
        "dataset_name": dataset_name,
        "ingestion_datetime": ing_dt, "ingestion_date": ing_date,
        "endpoints_ingested": [], "total_records": 0, "status": "success",
    }

    try:
        for endpoint in dataset_config.get("endpoints", {}).get("bronze", []):
            url  = f"{CBS_BASE}/{table_id}/{endpoint}"
            data = http_get_json(url)
            key  = f"{base}/{endpoint.lower()}.json"
            minio_put(key, json.dumps(data, indent=2, ensure_ascii=False))
            summary["endpoints_ingested"].append(endpoint)
            summary["total_records"] += len(data.get("value", []))

        minio_put(f"{base}/_summary.json", json.dumps(summary, indent=2, ensure_ascii=False))
        logger.info("CBS %s done — %d records", dataset_name, summary["total_records"])
        return summary

    except Exception as e:
        logger.exception("CBS %s failed", dataset_name)
        summary.update({"status": "failed", "error": str(e)})
        minio_put(f"{base}/_summary.json", json.dumps(summary, indent=2, ensure_ascii=False))
        raise


# ---------------------------------------------------------------------------
# PDOK — WFS com paginação completa
# ---------------------------------------------------------------------------

PDOK_PAGE_SIZE = 1000


def _pdok_base_url(url: str) -> str:
    """Remove parâmetros de paginação da URL base para controle manual."""
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    for p in ("startIndex", "count"):
        params.pop(p, None)
    new_query = urlencode({k: v[0] for k, v in params.items()})
    return urlunparse(parsed._replace(query=new_query))


@task
def ingest_pdok_dataset(dataset_name: str, url: str) -> dict:
    logger.info("PDOK ingestion (paginated): %s", dataset_name)

    ing_date = ingestion_date()
    base     = f"pdok/pdok_bronze/{dataset_name}/ingestion_date={ing_date}"

    summary: dict[str, Any] = {
        "source": "pdok", "dataset_name": dataset_name,
        "ingestion_date": ing_date, "total_records": 0,
        "pages": 0, "status": "success",
    }

    try:
        base_url    = _pdok_base_url(url)
        start_index = 0
        page        = 0
        all_features: list = []

        while True:
            paged_url = f"{base_url}&startIndex={start_index}&count={PDOK_PAGE_SIZE}"
            logger.info("PDOK %s page %d (startIndex=%d)", dataset_name, page, start_index)
            data = http_get_json(paged_url, timeout=120)

            features = data.get("features", [])
            if not features:
                break

            key = f"{base}/page_{page:04d}.json"
            minio_put(key, json.dumps(data, indent=2, ensure_ascii=False))

            all_features.extend(features)
            page        += 1
            start_index += len(features)

            # WFS indica fim quando retorna menos que o page size
            if len(features) < PDOK_PAGE_SIZE:
                break

        summary["total_records"] = len(all_features)
        summary["pages"]         = page
        minio_put(f"{base}/_summary.json", json.dumps(summary, indent=2, ensure_ascii=False))
        logger.info("PDOK %s done — %d records em %d páginas", dataset_name, len(all_features), page)
        return summary

    except Exception as e:
        logger.exception("PDOK %s failed", dataset_name)
        summary.update({"status": "failed", "error": str(e)})
        minio_put(f"{base}/_summary.json", json.dumps(summary, indent=2, ensure_ascii=False))
        raise


# ---------------------------------------------------------------------------
# KNMI — CSV com header de metadados (linhas iniciadas com #)
# ---------------------------------------------------------------------------

def _knmi_csv_to_json(raw_csv: str) -> list[dict]:
    """
    Converte o CSV do KNMI em lista de dicts.
    O formato real tem linhas de metadados prefixadas com '#' no início,
    seguidas de uma linha de cabeçalho com colunas separadas por vírgula,
    e depois os dados.
    """
    lines = raw_csv.splitlines()

    # Separa metadados do header/dados
    meta_lines  = [l for l in lines if l.startswith("#")]
    data_lines  = [l for l in lines if l and not l.startswith("#")]

    if not data_lines:
        return []

    # Primeira linha de dados é o cabeçalho real
    header = [col.strip() for col in data_lines[0].split(",")]
    records = []
    for row in data_lines[1:]:
        values = [v.strip() for v in row.split(",")]
        if len(values) == len(header):
            records.append(dict(zip(header, values)))

    logger.info("KNMI parsed %d data rows, %d meta lines", len(records), len(meta_lines))
    return records


@task
def ingest_knmi_dataset(dataset_name: str, endpoint: str, params: dict) -> dict:
    logger.info("KNMI ingestion: %s", dataset_name)

    ing_date = ingestion_date()
    base     = f"knmi/knmi_bronze/{dataset_name}/ingestion_date={ing_date}"

    summary: dict[str, Any] = {
        "source": "knmi", "dataset_name": dataset_name,
        "ingestion_date": ing_date, "status": "success",
    }

    try:
        today = datetime.now()
        # A API KNMI daggegevens aceita POST com form data
        form_data = {
            "start": (today - timedelta(days=params.get("start_days_ago", 7))).strftime("%Y%m%d"),
            "end":   (today - timedelta(days=params.get("end_days_ago", 1))).strftime("%Y%m%d"),
            "vars":  params.get("vars", "ALL"),
            "stns":  params.get("stns", "ALL"),
        }
        logger.info("KNMI POST %s params=%s", endpoint, form_data)
        r = requests.post(endpoint, data=form_data, timeout=180)
        r.raise_for_status()

        raw_csv = r.text

        # Salva CSV raw no bronze
        s3_client().put_object(
            Bucket=BUCKET_BRONZE,
            Key=f"{base}/data.csv",
            Body=raw_csv.encode("utf-8"),
            ContentType="text/csv",
        )

        # Converte para JSON e salva também (facilita leitura pelo PySpark)
        records = _knmi_csv_to_json(raw_csv)
        s3_client().put_object(
            Bucket=BUCKET_BRONZE,
            Key=f"{base}/data.json",
            Body=json.dumps(records, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )

        summary["rows"] = len(records)
        minio_put(f"{base}/_summary.json", json.dumps(summary, indent=2, ensure_ascii=False))
        logger.info("KNMI %s done — %d rows", dataset_name, len(records))
        return summary

    except Exception as e:
        logger.exception("KNMI %s failed", dataset_name)
        summary.update({"status": "failed", "error": str(e)})
        minio_put(f"{base}/_summary.json", json.dumps(summary, indent=2, ensure_ascii=False))
        raise


# ---------------------------------------------------------------------------
# NDW — download XML.gz, descomprime e converte para JSON
# ---------------------------------------------------------------------------

def _ndw_xml_to_records(xml_bytes: bytes) -> list[dict]:
    """
    Parseia o XML do NDW e retorna lista de dicts com os campos principais.
    O schema NDW usa namespaces DATEX II / NTCIP.
    Extraímos os campos essenciais de forma genérica.
    """
    root = ET.fromstring(xml_bytes)

    # Remove namespace prefix para facilitar o acesso
    def strip_ns(tag: str) -> str:
        return tag.split("}")[-1] if "}" in tag else tag

    records = []
    for elem in root.iter():
        tag = strip_ns(elem.tag)
        # Captura elementos folha com texto relevante
        if elem.text and elem.text.strip() and len(list(elem)) == 0:
            record = {
                "tag": tag,
                "text": elem.text.strip(),
                "attribs": {strip_ns(k): v for k, v in elem.attrib.items()},
                "path": "/".join(strip_ns(a.tag) for a in root.iter() if elem in list(a))
            }
            records.append(record)

    # Tenta extrair estrutura de measurementSiteRecord se existir (schema comum NDW)
    site_records = []
    for site in root.iter():
        if strip_ns(site.tag) in ("measurementSiteRecord", "siteMeasurements", "trafficSpeed", "trafficFlow"):
            site_dict: dict[str, Any] = {"element_type": strip_ns(site.tag)}
            site_dict.update({strip_ns(k): v for k, v in site.attrib.items()})
            for child in site:
                child_tag = strip_ns(child.tag)
                site_dict[child_tag] = child.text.strip() if child.text else None
            site_records.append(site_dict)

    return site_records if site_records else records


@task
def ingest_ndw_dataset(dataset_name: str, url: str) -> dict:
    logger.info("NDW ingestion: %s", dataset_name)

    ing_date = ingestion_date()
    base     = f"ndw/ndw_bronze/{dataset_name}/ingestion_date={ing_date}"

    summary: dict[str, Any] = {
        "source": "ndw", "dataset_name": dataset_name,
        "ingestion_date": ing_date, "status": "success",
    }

    try:
        logger.info("NDW GET %s", url)
        r = requests.get(url, timeout=180, stream=True)
        r.raise_for_status()

        raw_bytes = r.content

        # Descomprime se for .gz
        is_gzip = url.endswith(".gz") or raw_bytes[:2] == b"\x1f\x8b"
        if is_gzip:
            xml_bytes = gzip.decompress(raw_bytes)
        else:
            xml_bytes = raw_bytes

        # Salva XML descomprimido no bronze
        s3_client().put_object(
            Bucket=BUCKET_BRONZE,
            Key=f"{base}/data.xml",
            Body=xml_bytes,
            ContentType="application/xml",
        )

        # Converte para JSON e salva também
        records = _ndw_xml_to_records(xml_bytes)
        s3_client().put_object(
            Bucket=BUCKET_BRONZE,
            Key=f"{base}/data.json",
            Body=json.dumps(records, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )

        summary["bytes_compressed"]   = len(raw_bytes)
        summary["bytes_decompressed"] = len(xml_bytes)
        summary["records"]            = len(records)
        minio_put(f"{base}/_summary.json", json.dumps(summary, indent=2, ensure_ascii=False))
        logger.info(
            "NDW %s done — %d bytes comprimido → %d bytes XML → %d records JSON",
            dataset_name, len(raw_bytes), len(xml_bytes), len(records),
        )
        return summary

    except Exception as e:
        logger.exception("NDW %s failed", dataset_name)
        summary.update({"status": "failed", "error": str(e)})
        minio_put(f"{base}/_summary.json", json.dumps(summary, indent=2, ensure_ascii=False))
        raise


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

@task
def validate_ingestion(summary: dict) -> None:
    if summary.get("status") != "success":
        raise ValueError(f"Ingestion failed: {summary.get('error', 'unknown error')}")
    logger.info("Validation OK — %s/%s", summary.get("source"), summary.get("dataset_name"))


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "grontia",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="30_multi_source_daily_ingestion",
    default_args=default_args,
    description="Daily ingestion from all sources (CBS, PDOK, KNMI, NDW) to MinIO bronze layer",
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    tags=["grontia", "multi-source", "bronze", "daily", "minio"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    catalog = load_catalog()
    all_validate_tasks = []

    # CBS
    for ds in catalog.get("cbs", {}).get("datasets", []):
        ingest = ingest_cbs_dataset(ds["table_id"], ds["dataset_name"])
        validate = validate_ingestion(ingest)
        ingest >> validate
        all_validate_tasks.append(validate)

    # PDOK
    for ds in catalog.get("pdok", {}).get("datasets", []):
        ingest = ingest_pdok_dataset(ds["dataset_name"], ds["url"])
        validate = validate_ingestion(ingest)
        ingest >> validate
        all_validate_tasks.append(validate)

    # KNMI
    for ds in catalog.get("knmi", {}).get("datasets", []):
        ingest = ingest_knmi_dataset(
            ds["dataset_name"], ds["endpoint"], ds.get("params", {})
        )
        validate = validate_ingestion(ingest)
        ingest >> validate
        all_validate_tasks.append(validate)

    # NDW
    for ds in catalog.get("ndw", {}).get("datasets", []):
        ingest = ingest_ndw_dataset(ds["dataset_name"], ds["url"])
        validate = validate_ingestion(ingest)
        ingest >> validate
        all_validate_tasks.append(validate)

    start >> all_validate_tasks >> end
