"""Airflow assets shared between the Grontia DAGs (no DAG is defined here)."""
from airflow.sdk import Asset

# Emitted by 30_multi_source_daily_ingestion once every bronze dataset landed;
# triggers 40_silver_and_marts. The event extra carries the ingestion_date.
BRONZE_DAILY = Asset("s3://bronze/daily")
