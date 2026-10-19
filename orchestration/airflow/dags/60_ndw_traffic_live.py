"""
Near real-time NDW traffic, every 5 minutes:

1. snapshot the national traffic speed feed (~20k measurement sites, refreshed
   every minute) into silver/ndw/traffic_live/date=YYYY-MM-DD/HHMMSS.parquet;
2. roll every completed hour up to one row per site in
   silver/ndw/traffic_hourly/date=YYYY-MM-DD/hour=HH/data.parquet;
3. prune snapshots after SNAPSHOT_RETENTION_DAYS and hourly files after
   HOURLY_RETENTION_DAYS.

Snapshots skip bronze on purpose: ~1 MB compressed every 5 minutes would make
bronze the bulk of the storage, and DAG 30 already keeps a daily raw copy.
DAG 40 turns the hourly files into mart_traffic_site_profile once a day, so
MotherDuck only receives a small, stable table.

No failure alert per task: an NDW outage would alert every 5 minutes. The
freshness test on mart_traffic_site_profile reports it once a day instead.
"""
from __future__ import annotations

import gzip
import io
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
from airflow.decorators import task
from airflow.sdk import DAG

from grontia_ndw import ndw_xml_to_records

logger = logging.getLogger(__name__)

FEED_URL      = "https://opendata.ndw.nu/trafficspeed.xml.gz"
BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")
LIVE_PREFIX   = "ndw/traffic_live"
HOURLY_PREFIX = "ndw/traffic_hourly"

SNAPSHOT_RETENTION_DAYS = 2
HOURLY_RETENTION_DAYS   = 35
# Sites report with a small delay: an hour is rolled up only after this margin
HOUR_CLOSE_MARGIN = timedelta(minutes=10)

SNAPSHOT_SCHEMA = pa.schema([
    ("site_id", pa.string()),
    ("measured_at", pa.timestamp("us", tz="UTC")),
    ("vehicle_flow_rate", pa.float64()),
    ("avg_speed_kmh", pa.float64()),
    ("fetched_at", pa.timestamp("us", tz="UTC")),
])


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://s3:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    )


def list_keys(s3, prefix: str) -> list[str]:
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET_SILVER, Prefix=prefix):
        keys += [obj["Key"] for obj in page.get("Contents", [])]
    return keys


def put_parquet(s3, key: str, table: pa.Table) -> None:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    s3.put_object(Bucket=BUCKET_SILVER, Key=key, Body=buf.getvalue())


def read_parquet(s3, key: str) -> pa.Table:
    body = s3.get_object(Bucket=BUCKET_SILVER, Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body))


def snapshot_time(key: str) -> datetime:
    # .../date=2026-10-19/194500.parquet -> 2026-10-19 19:45:00 UTC
    date_part, file_part = key.rsplit("/", 2)[-2:]
    return datetime.strptime(date_part[5:] + file_part[:6], "%Y-%m-%d%H%M%S").replace(tzinfo=timezone.utc)


def hourly_key(hour: datetime) -> str:
    return f"{HOURLY_PREFIX}/date={hour:%Y-%m-%d}/hour={hour:%H}/data.parquet"


def day_prefixes(prefix: str, now: datetime, days: int) -> list[str]:
    return [f"{prefix}/date={now - timedelta(days=d):%Y-%m-%d}/" for d in range(days, -1, -1)]


def aggregate_hour(data: pa.Table, hour: datetime) -> pa.Table:
    """One row per site for the measurements taken in [hour, hour + 1h)."""
    hour_end = hour + timedelta(hours=1)
    ts_type = data["measured_at"].type
    in_hour = pc.and_(pc.greater_equal(data["measured_at"], pa.scalar(hour, ts_type)),
                      pc.less(data["measured_at"], pa.scalar(hour_end, ts_type)))
    data = data.filter(in_hour).select(["site_id", "measured_at", "vehicle_flow_rate", "avg_speed_kmh"])
    # the same measurement shows up in consecutive snapshots when a site is slow to update
    data = data.group_by(["site_id", "measured_at"]).aggregate(
        [("vehicle_flow_rate", "max"), ("avg_speed_kmh", "max")]
    )
    # aggregate() names outputs <column>_<function>; select by name, not position
    agg = data.group_by("site_id").aggregate([
        ("avg_speed_kmh_max", "mean"),
        ("vehicle_flow_rate_max", "mean"),
        ("measured_at", "count"),
    ])
    agg = pa.table({
        "site_id": agg["site_id"],
        "hour_start": pa.array([hour] * len(agg), pa.timestamp("us", tz="UTC")),
        "avg_speed_kmh": agg["avg_speed_kmh_max_mean"],
        "avg_flow_per_hour": agg["vehicle_flow_rate_max_mean"],
        "n_measurements": agg["measured_at_count"],
    })
    return agg


@task
def fetch_snapshot() -> int:
    fetched_at = datetime.now(timezone.utc).replace(microsecond=0)
    r = requests.get(FEED_URL, timeout=60)
    r.raise_for_status()
    records = ndw_xml_to_records(gzip.decompress(r.content))

    rows = [
        {
            "site_id": rec["id"],
            "measured_at": datetime.fromisoformat(rec["period"].replace("Z", "+00:00")),
            "vehicle_flow_rate": rec["vehicleFlowRate"],
            "avg_speed_kmh": rec["averageVehicleSpeed"],
            "fetched_at": fetched_at,
        }
        for rec in records
        # sites without any valid lane value carry no information
        if rec["id"] and rec["period"]
        and (rec["vehicleFlowRate"] is not None or rec["averageVehicleSpeed"] is not None)
    ]
    table = pa.Table.from_pylist(rows, schema=SNAPSHOT_SCHEMA)
    key = f"{LIVE_PREFIX}/date={fetched_at:%Y-%m-%d}/{fetched_at:%H%M%S}.parquet"
    put_parquet(s3_client(), key, table)
    logger.info("NDW snapshot: %d of %d sites with data -> %s", len(rows), len(records), key)
    return len(rows)


@task
def rollup_hours() -> list[str]:
    """Aggregate every closed hour that has snapshots but no hourly file yet,
    so a scheduler outage of up to SNAPSHOT_RETENTION_DAYS is caught up."""
    s3 = s3_client()
    now = datetime.now(timezone.utc)

    snapshots = [k for p in day_prefixes(LIVE_PREFIX, now, SNAPSHOT_RETENTION_DAYS) for k in list_keys(s3, p)]
    done = {k for p in day_prefixes(HOURLY_PREFIX, now, SNAPSHOT_RETENTION_DAYS) for k in list_keys(s3, p)}
    by_time = {k: snapshot_time(k) for k in snapshots}

    hours = sorted({t.replace(minute=0, second=0) for t in by_time.values()})
    written = []
    for hour in hours:
        end = hour + timedelta(hours=1)
        if now < end + HOUR_CLOSE_MARGIN or hourly_key(hour) in done:
            continue
        # measurements of this hour can still arrive in the first snapshots of the next one
        keys = [k for k, t in by_time.items() if hour <= t < end + HOUR_CLOSE_MARGIN]
        data = pa.concat_tables([read_parquet(s3, k) for k in keys])
        agg = aggregate_hour(data, hour)

        put_parquet(s3, hourly_key(hour), agg)
        written.append(hourly_key(hour))
        logger.info("Rolled up %s: %d sites from %d snapshots", hour, len(agg), len(keys))
    return written


@task
def prune() -> int:
    s3 = s3_client()
    today = datetime.now(timezone.utc).date()
    removed = 0
    for prefix, keep_days in [(LIVE_PREFIX, SNAPSHOT_RETENTION_DAYS), (HOURLY_PREFIX, HOURLY_RETENTION_DAYS)]:
        cutoff = f"{prefix}/date={today - timedelta(days=keep_days):%Y-%m-%d}"
        old = [k for k in list_keys(s3, prefix + "/") if k < cutoff]
        for i in range(0, len(old), 1000):
            s3.delete_objects(Bucket=BUCKET_SILVER,
                              Delete={"Objects": [{"Key": k} for k in old[i:i + 1000]]})
        removed += len(old)
    if removed:
        logger.info("Pruned %d old NDW objects", removed)
    return removed


with DAG(
    dag_id="60_ndw_traffic_live",
    description="NDW traffic every 5 minutes: snapshot -> hourly rollup per site -> prune",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={"owner": "grontia", "retries": 0},
    dagrun_timeout=timedelta(minutes=10),
    tags=["grontia", "ndw", "silver", "near-real-time"],
) as dag:

    fetch_snapshot() >> rollup_hours() >> prune()
