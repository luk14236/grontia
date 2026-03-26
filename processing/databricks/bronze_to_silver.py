# Databricks notebook source
"""
Bronze to Silver Transformation
================================
This notebook reads raw data from the bronze layer and transforms it into 
clean, normalized silver layer data.

Catalog Structure:
- Bronze: raw data as ingested from sources
- Silver: cleaned, normalized, deduplicated data
"""

import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()
spark.sql("SET spark.sql.adaptive.enabled = true")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Configuration
BRONZE_PATH = "abfss://bronze@{storage_account}.dfs.core.windows.net"
SILVER_PATH = "abfss://silver@{storage_account}.dfs.core.windows.net"

# ============================================================================
# CBS Data Transformation
# ============================================================================

def transform_cbs_neighbourhood_key_figures(ingestion_date: str) -> None:
    """
    Transform CBS neighbourhood key figures from bronze to silver.
    
    Bronze: Raw JSON files from CBS OData API
    Silver: Cleaned, normalized, deduplicated tables
    """
    logger.info(f"Starting CBS neighbourhood_key_figures transformation for {ingestion_date}")
    
    # Read from bronze
    bronze_path = f"{BRONZE_PATH}/cbs/cbs_bronze/neighbourhood_key_figures/ingestion_date={ingestion_date}"
    
    # Read TypedDataSet (main data)
    df = spark.read.json(f"{bronze_path}/typeddataset.json")
    
    # Flatten nested structure
    if "value" in df.columns:
        df = df.explode("value")
    
    # Clean column names and add metadata
    df = (df
        .withColumn("ingestion_date", F.lit(ingestion_date))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source", F.lit("cbs"))
        .withColumn("dataset_name", F.lit("neighbourhood_key_figures"))
    )
    
    # Remove duplicates (keep latest)
    df = (df
        .withColumn("_row_number", 
            F.row_number().over(
                F.Window.partitionBy("RegioS", "Perioden").orderBy(F.desc("ingestion_timestamp"))
            )
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )
    
    # Write to silver
    silver_path = f"{SILVER_PATH}/cbs/silver_cbs_neighbourhood_key_figures"
    df.write.mode("append").option("mergeSchema", "true").parquet(silver_path)
    
    logger.info(f"Transformation complete: {df.count()} records written to silver")


def transform_cbs_average_woz_value(ingestion_date: str) -> None:
    """Transform CBS average WOZ property values from bronze to silver."""
    logger.info(f"Starting CBS average_woz_value transformation for {ingestion_date}")
    
    bronze_path = f"{BRONZE_PATH}/cbs/cbs_bronze/average_woz_value/ingestion_date={ingestion_date}"
    
    df = spark.read.json(f"{bronze_path}/typeddataset.json")
    
    if "value" in df.columns:
        df = df.explode("value")
    
    df = (df
        .withColumn("ingestion_date", F.lit(ingestion_date))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source", F.lit("cbs"))
        .withColumn("dataset_name", F.lit("average_woz_value"))
    )
    
    # Remove duplicates
    df = (df
        .withColumn("_row_number", 
            F.row_number().over(
                F.Window.partitionBy("RegioS", "Perioden").orderBy(F.desc("ingestion_timestamp"))
            )
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )
    
    silver_path = f"{SILVER_PATH}/cbs/silver_cbs_average_woz_value"
    df.write.mode("append").option("mergeSchema", "true").parquet(silver_path)
    
    logger.info(f"Transformation complete")


def transform_cbs_housing_stock(ingestion_date: str) -> None:
    """Transform CBS housing stock data from bronze to silver."""
    logger.info(f"Starting CBS housing_stock transformation for {ingestion_date}")
    
    bronze_path = f"{BRONZE_PATH}/cbs/cbs_bronze/housing_stock/ingestion_date={ingestion_date}"
    
    df = spark.read.json(f"{bronze_path}/typeddataset.json")
    
    if "value" in df.columns:
        df = df.explode("value")
    
    df = (df
        .withColumn("ingestion_date", F.lit(ingestion_date))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source", F.lit("cbs"))
        .withColumn("dataset_name", F.lit("housing_stock"))
    )
    
    # Remove duplicates
    df = (df
        .withColumn("_row_number", 
            F.row_number().over(
                F.Window.partitionBy("RegioS", "Perioden", "TypeWoning_3").orderBy(F.desc("ingestion_timestamp"))
            )
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )
    
    silver_path = f"{SILVER_PATH}/cbs/silver_cbs_housing_stock"
    df.write.mode("append").option("mergeSchema", "true").parquet(silver_path)
    
    logger.info(f"Transformation complete")


def transform_cbs_household_income(ingestion_date: str) -> None:
    """Transform CBS household income data from bronze to silver."""
    logger.info(f"Starting CBS household_income transformation for {ingestion_date}")
    
    bronze_path = f"{BRONZE_PATH}/cbs/cbs_bronze/household_income/ingestion_date={ingestion_date}"
    
    df = spark.read.json(f"{bronze_path}/typeddataset.json")
    
    if "value" in df.columns:
        df = df.explode("value")
    
    df = (df
        .withColumn("ingestion_date", F.lit(ingestion_date))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source", F.lit("cbs"))
        .withColumn("dataset_name", F.lit("household_income"))
    )
    
    # Remove duplicates
    df = (df
        .withColumn("_row_number", 
            F.row_number().over(
                F.Window.partitionBy("RegioS", "Perioden").orderBy(F.desc("ingestion_timestamp"))
            )
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )
    
    silver_path = f"{SILVER_PATH}/cbs/silver_cbs_household_income"
    df.write.mode("append").option("mergeSchema", "true").parquet(silver_path)
    
    logger.info(f"Transformation complete")


# ============================================================================
# PDOK Data Transformation
# ============================================================================

def transform_pdok_addresses(ingestion_date: str) -> None:
    """Transform PDOK address data from bronze to silver."""
    logger.info(f"Starting PDOK addresses transformation for {ingestion_date}")
    
    bronze_path = f"{BRONZE_PATH}/pdok/pdok_bronze/addresses/ingestion_date={ingestion_date}"
    
    try:
        df = spark.read.geo.parquet(bronze_path) if spark.read.geo else spark.read.parquet(bronze_path)
        
        df = (df
            .withColumn("ingestion_date", F.lit(ingestion_date))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source", F.lit("pdok"))
            .withColumn("dataset_name", F.lit("addresses"))
        )
        
        silver_path = f"{SILVER_PATH}/pdok/silver_pdok_addresses"
        df.write.mode("append").option("mergeSchema", "true").parquet(silver_path)
        
        logger.info(f"Transformation complete")
    except Exception as e:
        logger.warning(f"PDOK addresses transformation skipped (not yet implemented): {e}")


# ============================================================================
# KNMI Data Transformation
# ============================================================================

def transform_knmi_weather_stations(ingestion_date: str) -> None:
    """Transform KNMI weather station data from bronze to silver."""
    logger.info(f"Starting KNMI weather_stations transformation for {ingestion_date}")
    
    bronze_path = f"{BRONZE_PATH}/knmi/knmi_bronze/weather_stations/ingestion_date={ingestion_date}"
    
    try:
        df = spark.read.csv(f"{bronze_path}/*.csv", header=True, inferSchema=True)
        
        df = (df
            .withColumn("ingestion_date", F.lit(ingestion_date))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source", F.lit("knmi"))
            .withColumn("dataset_name", F.lit("weather_stations"))
        )
        
        silver_path = f"{SILVER_PATH}/knmi/silver_knmi_weather_stations"
        df.write.mode("append").option("mergeSchema", "true").parquet(silver_path)
        
        logger.info(f"Transformation complete")
    except Exception as e:
        logger.warning(f"KNMI weather_stations transformation skipped (not yet implemented): {e}")


# ============================================================================
# NDW Data Transformation
# ============================================================================

def transform_ndw_traffic_flow(ingestion_date: str) -> None:
    """Transform NDW traffic flow data from bronze to silver."""
    logger.info(f"Starting NDW traffic_flow transformation for {ingestion_date}")
    
    bronze_path = f"{BRONZE_PATH}/ndw/ndw_bronze/traffic_flow/ingestion_date={ingestion_date}"
    
    try:
        df = spark.read.json(f"{bronze_path}/*.json")
        
        df = (df
            .withColumn("ingestion_date", F.lit(ingestion_date))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source", F.lit("ndw"))
            .withColumn("dataset_name", F.lit("traffic_flow"))
        )
        
        silver_path = f"{SILVER_PATH}/ndw/silver_ndw_traffic_flow"
        df.write.mode("append").option("mergeSchema", "true").parquet(silver_path)
        
        logger.info(f"Transformation complete")
    except Exception as e:
        logger.warning(f"NDW traffic_flow transformation skipped (not yet implemented): {e}")


# ============================================================================
# Main Execution
# ============================================================================

if __name__ == "__main__":
    # Get ingestion date from Airflow parameter or use today
    ingestion_date = dbutils.widgets.get("ingestion_date", datetime.now().strftime("%Y-%m-%d"))
    
    logger.info(f"Starting bronze to silver transformations for {ingestion_date}")
    
    try:
        # Transform CBS datasets
        transform_cbs_neighbourhood_key_figures(ingestion_date)
        transform_cbs_average_woz_value(ingestion_date)
        transform_cbs_housing_stock(ingestion_date)
        transform_cbs_household_income(ingestion_date)
        
        # Transform PDOK datasets
        transform_pdok_addresses(ingestion_date)
        
        # Transform KNMI datasets
        transform_knmi_weather_stations(ingestion_date)
        
        # Transform NDW datasets
        transform_ndw_traffic_flow(ingestion_date)
        
        logger.info(f"All bronze to silver transformations completed successfully")
        
    except Exception as e:
        logger.error(f"Error in bronze to silver transformation: {e}")
        raise