from __future__ import annotations

import gzip
import io
import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

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

CBS_BASE      = "https://opendata.cbs.nl/ODataApi/odata"
CBS_FEED_BASE = "https://opendata.cbs.nl/ODataFeed/odata"


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


def clear_prefix(prefix: str) -> None:
    """Delete everything under a bronze prefix so a re-run for the same date
    replaces the partition instead of mixing old and new files."""
    client = s3_client()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET_BRONZE, Prefix=f"{prefix}/"
    ):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=BUCKET_BRONZE, Delete={"Objects": keys})


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

def _cbs_fetch_typed_dataset(table_id: str, odata_filter: str | None) -> dict:
    """
    Fetch all rows of a CBS TypedDataSet.

    The ODataApi endpoint rejects queries returning more than 10k rows and does
    not support $skip, so we use the ODataFeed endpoint and follow
    odata.nextLink until the last page.
    """
    params = {"$format": "json"}
    if odata_filter:
        params["$filter"] = odata_filter
    url = f"{CBS_FEED_BASE}/{table_id}/TypedDataSet?{urlencode(params)}"

    rows: list = []
    while url:
        page = http_get_json(url, timeout=180)
        rows.extend(page.get("value", []))
        url = page.get("odata.nextLink")
    return {"value": rows}


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
    clear_prefix(base)

    summary: dict[str, Any] = {
        "source": "cbs", "table_id": table_id,
        "dataset_name": dataset_name,
        "ingestion_datetime": ing_dt, "ingestion_date": ing_date,
        "endpoints_ingested": [], "total_records": 0, "status": "success",
    }

    try:
        for endpoint in dataset_config.get("endpoints", {}).get("bronze", []):
            if endpoint == "TypedDataSet":
                data = _cbs_fetch_typed_dataset(table_id, dataset_config.get("odata_filter"))
            else:
                data = http_get_json(f"{CBS_BASE}/{table_id}/{endpoint}")
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
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    for p in ("startIndex", "count"):
        params.pop(p, None)
    new_query = urlencode({k: v[0] for k, v in params.items()})
    return urlunparse(parsed._replace(query=new_query))


@task
def ingest_pdok_dataset(dataset_name: str, url: str, bbox: str | None = None) -> dict:
    logger.info("PDOK ingestion (paginated): %s", dataset_name)

    ing_date = ingestion_date()
    base     = f"pdok/pdok_bronze/{dataset_name}/ingestion_date={ing_date}"
    clear_prefix(base)

    summary: dict[str, Any] = {
        "source": "pdok", "dataset_name": dataset_name,
        "ingestion_date": ing_date, "total_records": 0,
        "pages": 0, "status": "success",
    }

    try:
        base_url    = _pdok_base_url(url)
        if bbox:
            base_url += "&" + urlencode({"bbox": bbox})
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

def _knmi_clean_csv(raw_csv: str) -> str:
    """
    Turn the KNMI daggegevens response into a plain CSV.

    The response starts with metadata lines prefixed with '#'. The column header
    is also a comment line ("# STN,YYYYMMDD,..."), so it is recovered from the
    last '#' line that contains STN. Values are padded with spaces.
    """
    lines = raw_csv.splitlines()
    header_line = next(
        (l for l in reversed(lines) if l.startswith("#") and "STN," in l), None
    )
    if header_line is None:
        raise ValueError("KNMI response has no '# STN,...' header line")

    header = ",".join(col.strip() for col in header_line.lstrip("#").split(","))
    rows = [
        ",".join(v.strip() for v in l.split(","))
        for l in lines
        if l.strip() and not l.startswith("#")
    ]
    logger.info("KNMI parsed %d data rows", len(rows))
    return "\n".join([header, *rows]) + "\n"


@task
def ingest_knmi_dataset(dataset_name: str, endpoint: str, query_params: dict) -> dict:
    logger.info("KNMI ingestion: %s", dataset_name)

    ing_date = ingestion_date()
    base     = f"knmi/knmi_bronze/{dataset_name}/ingestion_date={ing_date}"
    clear_prefix(base)

    summary: dict[str, Any] = {
        "source": "knmi", "dataset_name": dataset_name,
        "ingestion_date": ing_date, "status": "success",
    }

    try:
        today = datetime.now()
        # A API KNMI daggegevens aceita POST com form data
        form_data = {
            "start": (today - timedelta(days=query_params.get("start_days_ago", 7))).strftime("%Y%m%d"),
            "end":   (today - timedelta(days=query_params.get("end_days_ago", 1))).strftime("%Y%m%d"),
            "vars":  query_params.get("vars", "ALL"),
            "stns":  query_params.get("stns", "ALL"),
        }
        logger.info("KNMI POST %s params=%s", endpoint, form_data)
        r = requests.post(endpoint, data=form_data, timeout=180)
        r.raise_for_status()

        raw_csv = r.text

        # Salva a resposta original e um CSV limpo (header real) no bronze
        s3_client().put_object(
            Bucket=BUCKET_BRONZE,
            Key=f"{base}/raw.txt",
            Body=raw_csv.encode("utf-8"),
            ContentType="text/plain",
        )
        clean_csv = _knmi_clean_csv(raw_csv)
        s3_client().put_object(
            Bucket=BUCKET_BRONZE,
            Key=f"{base}/data.csv",
            Body=clean_csv.encode("utf-8"),
            ContentType="text/csv",
        )

        n_rows = clean_csv.count("\n") - 1
        summary["rows"] = n_rows
        minio_put(f"{base}/_summary.json", json.dumps(summary, indent=2, ensure_ascii=False))
        logger.info("KNMI %s done — %d rows", dataset_name, n_rows)
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
    Parse an NDW DATEX II MeasuredDataPublication (e.g. trafficspeed.xml.gz)
    into one record per measurement site.

    Each <siteMeasurements> holds several <measuredValue> entries per lane /
    vehicle class: TrafficFlow values carry vehicleFlowRate (vehicles/hour) and
    TrafficSpeed values carry averageVehicleSpeed/speed (km/h). Negative values
    mean "no data". We sum the flows and average the valid speeds per site.
    """
    def local(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]

    records = []
    for _, elem in ET.iterparse(io.BytesIO(xml_bytes), events=("end",)):
        if local(elem.tag) != "siteMeasurements":
            continue

        site_id, measured_at = None, None
        flows: list[float] = []
        speeds: list[float] = []
        for child in elem.iter():
            name = local(child.tag)
            if name == "measurementSiteReference":
                site_id = child.get("id")
            elif name == "measurementTimeDefault":
                measured_at = child.text
            elif name == "vehicleFlowRate" and child.text:
                if float(child.text) >= 0:
                    flows.append(float(child.text))
            elif name == "speed" and child.text:
                if float(child.text) > 0:
                    speeds.append(float(child.text))

        records.append({
            "id": site_id,
            "element_type": "siteMeasurements",
            "measurementSiteReference": site_id,
            "period": measured_at,
            "vehicleFlowRate": sum(flows) if flows else None,
            "averageVehicleSpeed": round(sum(speeds) / len(speeds), 1) if speeds else None,
        })
        elem.clear()

    return records


@task
def ingest_ndw_dataset(dataset_name: str, url: str) -> dict:
    logger.info("NDW ingestion: %s", dataset_name)

    ing_date = ingestion_date()
    base     = f"ndw/ndw_bronze/{dataset_name}/ingestion_date={ing_date}"
    clear_prefix(base)

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
        ingest = ingest_pdok_dataset(ds["dataset_name"], ds["url"], ds.get("bbox"))
        validate = validate_ingestion(ingest)
        ingest >> validate
        all_validate_tasks.append(validate)

    # KNMI
    for ds in catalog.get("knmi", {}).get("datasets", []):
        ingest = ingest_knmi_dataset(
            ds["dataset_name"], ds["endpoint"], ds.get("query_params", {})
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
