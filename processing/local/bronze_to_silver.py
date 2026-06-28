"""
Bronze → Silver transformation (MinIO → MinIO).

Reads raw JSON/CSV from the MinIO bronze bucket, cleans/normalises,
then writes Parquet to the MinIO silver bucket.
dbt-duckdb later reads these Parquet files via httpfs to build the DW.

Run:
    python bronze_to_silver.py [ingestion_date]   # e.g. 2025-01-15
    python bronze_to_silver.py                    # defaults to today

Required env vars (optional — defaults work with local docker-compose):
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
"""

import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession, functions as F, Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BRONZE = "s3a://bronze"
SILVER = "s3a://silver"

HADOOP_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze_to_silver")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", HADOOP_PACKAGE)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def _dedup(df, partition_cols: list[str]):
    return (
        df.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy(*partition_cols).orderBy(F.desc("ingestion_timestamp"))
            ),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def _add_meta(df, source: str, dataset: str, ingestion_date: str):
    return (
        df.withColumn("ingestion_date", F.lit(ingestion_date))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source", F.lit(source))
        .withColumn("dataset_name", F.lit(dataset))
    )


def _write_silver(df, path: str) -> None:
    df.write.mode("append").option("mergeSchema", "true").parquet(path)
    logger.info("Written to %s — %d rows", path, df.count())


# ---------------------------------------------------------------------------
# CBS
# ---------------------------------------------------------------------------

def transform_cbs(spark, ingestion_date: str, dataset_name: str, partition_cols: list[str]) -> None:
    bronze_path = f"{BRONZE}/cbs/cbs_bronze/{dataset_name}/ingestion_date={ingestion_date}/typeddataset.json"
    silver_path = f"{SILVER}/cbs/{dataset_name}"

    logger.info("CBS %s: %s → %s", dataset_name, bronze_path, silver_path)

    try:
        df = spark.read.option("multiline", "true").json(bronze_path)
    except Exception:
        logger.warning("CBS %s — bronze file not found, skipping.", dataset_name)
        return

    if "value" in df.columns:
        df = df.select(F.explode("value").alias("r")).select("r.*")

    df = _add_meta(df, "cbs", dataset_name, ingestion_date)
    df = _dedup(df, partition_cols)
    _write_silver(df, silver_path)


# ---------------------------------------------------------------------------
# PDOK
# ---------------------------------------------------------------------------

def transform_pdok(spark, ingestion_date: str, dataset_name: str) -> None:
    bronze_path = f"{BRONZE}/pdok/pdok_bronze/{dataset_name}/ingestion_date={ingestion_date}/data.json"
    silver_path = f"{SILVER}/pdok/{dataset_name}"

    logger.info("PDOK %s: %s → %s", dataset_name, bronze_path, silver_path)

    try:
        df = spark.read.option("multiline", "true").json(bronze_path)
    except Exception:
        logger.warning("PDOK %s — bronze file not found, skipping.", dataset_name)
        return

    if "features" in df.columns:
        df = df.select(F.explode("features").alias("f")).select(
            "f.id",
            F.col("f.geometry").cast("string").alias("geometry"),
            "f.properties.*",
        )

    df = _add_meta(df, "pdok", dataset_name, ingestion_date)
    _write_silver(df, silver_path)


# ---------------------------------------------------------------------------
# KNMI
# ---------------------------------------------------------------------------

def transform_knmi(spark, ingestion_date: str, dataset_name: str) -> None:
    bronze_path = f"{BRONZE}/knmi/knmi_bronze/{dataset_name}/ingestion_date={ingestion_date}/data.csv"
    silver_path = f"{SILVER}/knmi/{dataset_name}"

    logger.info("KNMI %s: %s → %s", dataset_name, bronze_path, silver_path)

    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(bronze_path)
    except Exception:
        logger.warning("KNMI %s — bronze file not found, skipping.", dataset_name)
        return

    df = _add_meta(df, "knmi", dataset_name, ingestion_date)
    _write_silver(df, silver_path)


# ---------------------------------------------------------------------------
# NDW
# ---------------------------------------------------------------------------

def transform_ndw(spark, ingestion_date: str, dataset_name: str) -> None:
    logger.info("NDW %s — XML parsing not yet implemented, skipping.", dataset_name)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ing_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    logger.info("Bronze → Silver for ingestion_date=%s", ing_date)

    spark = build_spark()

    transform_cbs(spark, ing_date, "neighbourhood_key_figures", ["RegioS", "Perioden"])
    transform_cbs(spark, ing_date, "average_woz_value",         ["RegioS", "Perioden"])
    transform_cbs(spark, ing_date, "housing_stock",             ["RegioS", "Perioden", "TypeWoning_3"])
    transform_cbs(spark, ing_date, "household_income",          ["RegioS", "Perioden"])

    transform_pdok(spark, ing_date, "bag_pand")
    transform_pdok(spark, ing_date, "bag_verblijfsobject")

    transform_knmi(spark, ing_date, "daily_weather_all_stations")

    transform_ndw(spark, ing_date, "current_traffic_flow")

    logger.info("All bronze → silver transformations complete.")
    spark.stop()
