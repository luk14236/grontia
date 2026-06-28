"""
Silver → Gold transformation (local / MinIO).

Reads clean Parquet from the silver bucket and creates aggregated, analytical
tables in the gold bucket.

Run:
    python silver_to_gold.py
"""

import logging
import os

from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")

SILVER = "s3a://silver"
GOLD   = "s3a://gold"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("silver_to_gold")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def _read(spark, path: str):
    try:
        return spark.read.parquet(path)
    except Exception:
        logger.warning("Could not read %s — skipping.", path)
        return None


def gold_neighbourhood_analysis(spark) -> None:
    df = _read(spark, f"{SILVER}/cbs/silver_cbs_neighbourhood_key_figures")
    if df is None:
        return

    gold = (
        df.groupBy("RegioS", "RegioNaam", "Perioden")
        .agg(
            F.avg("Inwoners_5").alias("avg_population"),
            F.avg("AantalHuishoudens_10").alias("avg_households"),
            F.avg("GemiddeldInkomenPerInwoner_25").alias("avg_income_per_resident"),
            F.max("ingestion_date").alias("last_updated"),
        )
        .withColumn("gold_table", F.lit("gold_cbs_neighbourhood_analysis"))
    )
    gold.write.mode("overwrite").parquet(f"{GOLD}/cbs/gold_cbs_neighbourhood_analysis")
    logger.info("gold_cbs_neighbourhood_analysis — %d records", gold.count())


def gold_property_value_trends(spark) -> None:
    df = _read(spark, f"{SILVER}/cbs/silver_cbs_average_woz_value")
    if df is None:
        return

    gold = (
        df.groupBy("RegioS", "RegioNaam", "Perioden")
        .agg(
            F.avg("GemiddeldeWOZWaarde_1").alias("avg_property_value"),
            F.max("GemiddeldeWOZWaarde_1").alias("max_property_value"),
            F.min("GemiddeldeWOZWaarde_1").alias("min_property_value"),
            F.max("ingestion_date").alias("last_updated"),
        )
        .withColumn("gold_table", F.lit("gold_cbs_property_value_trends"))
    )
    gold.write.mode("overwrite").parquet(f"{GOLD}/cbs/gold_cbs_property_value_trends")
    logger.info("gold_cbs_property_value_trends — %d records", gold.count())


def gold_housing_market(spark) -> None:
    housing = _read(spark, f"{SILVER}/cbs/silver_cbs_housing_stock")
    woz     = _read(spark, f"{SILVER}/cbs/silver_cbs_average_woz_value")
    if housing is None or woz is None:
        return

    gold = (
        housing.join(
            woz.select("RegioS", "Perioden", "GemiddeldeWOZWaarde_1"),
            on=["RegioS", "Perioden"],
            how="left",
        )
        .groupBy("RegioS", "RegioNaam", "Perioden", "TypeWoning_3")
        .agg(
            F.sum("AantalWoningen_1").alias("total_dwellings"),
            F.avg("GemiddeldeWOZWaarde_1").alias("avg_property_value"),
            F.max("ingestion_date").alias("last_updated"),
        )
        .withColumn("gold_table", F.lit("gold_cbs_housing_market"))
    )
    gold.write.mode("overwrite").parquet(f"{GOLD}/cbs/gold_cbs_housing_market")
    logger.info("gold_cbs_housing_market — %d records", gold.count())


def gold_socioeconomic_indicators(spark) -> None:
    income       = _read(spark, f"{SILVER}/cbs/silver_cbs_household_income")
    neighbourhood = _read(spark, f"{SILVER}/cbs/silver_cbs_neighbourhood_key_figures")
    if income is None or neighbourhood is None:
        return

    gold = (
        income.join(
            neighbourhood.select("RegioS", "Perioden", "GemiddeldInkomenPerInwoner_25"),
            on=["RegioS", "Perioden"],
            how="inner",
        )
        .groupBy("RegioS", "RegioNaam", "Perioden")
        .agg(
            F.avg("GemiddeldInkomenHuishouden_6").alias("avg_household_income"),
            F.avg("MediaanInkomenHuishouden_7").alias("median_household_income"),
            F.avg("GemiddeldInkomenPerInwoner_25").alias("avg_income_per_resident"),
            F.max("ingestion_date").alias("last_updated"),
        )
        .withColumn("gold_table", F.lit("gold_cbs_socioeconomic_indicators"))
    )
    gold.write.mode("overwrite").parquet(f"{GOLD}/cbs/gold_cbs_socioeconomic_indicators")
    logger.info("gold_cbs_socioeconomic_indicators — %d records", gold.count())


def gold_regional_dashboard(spark) -> None:
    neighbourhood = _read(spark, f"{SILVER}/cbs/silver_cbs_neighbourhood_key_figures")
    woz           = _read(spark, f"{SILVER}/cbs/silver_cbs_average_woz_value")
    income        = _read(spark, f"{SILVER}/cbs/silver_cbs_household_income")
    if any(df is None for df in [neighbourhood, woz, income]):
        logger.warning("regional_dashboard: missing silver table(s), skipping.")
        return

    gold = (
        neighbourhood.select("RegioS", "RegioNaam", "Perioden", "Inwoners_5", "AantalHuishoudens_10")
        .join(woz.select("RegioS", "Perioden", "GemiddeldeWOZWaarde_1"),   on=["RegioS", "Perioden"], how="left")
        .join(income.select("RegioS", "Perioden", "GemiddeldInkomenHuishouden_6"), on=["RegioS", "Perioden"], how="left")
        .withColumn("gold_table", F.lit("gold_regional_dashboard"))
        .withColumn("created_timestamp", F.current_timestamp())
    )
    gold.write.mode("overwrite").parquet(f"{GOLD}/consolidated/gold_regional_dashboard")
    logger.info("gold_regional_dashboard — %d records", gold.count())


if __name__ == "__main__":
    spark = build_spark()

    gold_neighbourhood_analysis(spark)
    gold_property_value_trends(spark)
    gold_housing_market(spark)
    gold_socioeconomic_indicators(spark)
    gold_regional_dashboard(spark)

    logger.info("All silver → gold transformations complete.")
    spark.stop()
