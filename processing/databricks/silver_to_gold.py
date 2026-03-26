# Databricks notebook source
"""
Silver to Gold Transformation
==============================
This notebook reads cleaned data from the silver layer and creates
analytical tables in the gold layer for business consumption.

Catalog Structure:
- Silver: cleaned, normalized data
- Gold: aggregated, business-ready analytical tables
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()
spark.sql("SET spark.sql.adaptive.enabled = true")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Configuration
SILVER_PATH = "abfss://silver@{storage_account}.dfs.core.windows.net"
GOLD_PATH = "abfss://gold@{storage_account}.dfs.core.windows.net"

# ============================================================================
# CBS Analytical Tables
# ============================================================================

def create_gold_cbs_neighbourhood_analysis() -> None:
    """Create analytical table for neighbourhood key figures."""
    logger.info("Creating gold_cbs_neighbourhood_analysis")
    
    df = spark.read.parquet(f"{SILVER_PATH}/cbs/silver_cbs_neighbourhood_key_figures")
    
    # Create aggregated analytical table
    gold_df = (df
        .groupBy("RegioS", "RegioNaam", "Perioden")
        .agg(
            F.avg("Inwoners_5").alias("avg_population"),
            F.max("Inwoners_5").alias("max_population"),
            F.avg("AantalHuishoudens_10").alias("avg_households"),
            F.avg("GemiddeldInkomenPerInwoner_25").alias("avg_income_per_resident"),
            F.max("ingestion_date").alias("last_updated")
        )
        .withColumn("gold_table", F.lit("gold_cbs_neighbourhood_analysis"))
    )
    
    gold_path = f"{GOLD_PATH}/cbs/gold_cbs_neighbourhood_analysis"
    gold_df.write.mode("overwrite").parquet(gold_path)
    
    logger.info(f"Table created: {gold_df.count()} records")


def create_gold_cbs_property_value_trends() -> None:
    """Create analytical table for property value trends."""
    logger.info("Creating gold_cbs_property_value_trends")
    
    df = spark.read.parquet(f"{SILVER_PATH}/cbs/silver_cbs_average_woz_value")
    
    # Create trend analysis
    gold_df = (df
        .groupBy("RegioS", "RegioNaam", "Perioden")
        .agg(
            F.avg("GemiddeldeWOZWaarde_1").alias("avg_property_value"),
            F.max("GemiddeldeWOZWaarde_1").alias("max_property_value"),
            F.min("GemiddeldeWOZWaarde_1").alias("min_property_value"),
            F.max("ingestion_date").alias("last_updated")
        )
        .withColumn("gold_table", F.lit("gold_cbs_property_value_trends"))
    )
    
    gold_path = f"{GOLD_PATH}/cbs/gold_cbs_property_value_trends"
    gold_df.write.mode("overwrite").parquet(gold_path)
    
    logger.info(f"Table created: {gold_df.count()} records")


def create_gold_cbs_housing_market_analysis() -> None:
    """Create analytical table for housing market analysis."""
    logger.info("Creating gold_cbs_housing_market_analysis")
    
    housing_df = spark.read.parquet(f"{SILVER_PATH}/cbs/silver_cbs_housing_stock")
    woz_df = spark.read.parquet(f"{SILVER_PATH}/cbs/silver_cbs_average_woz_value")
    
    # Join housing stock with property values
    gold_df = (housing_df
        .join(
            woz_df.select("RegioS", "Perioden", "GemiddeldeWOZWaarde_1"),
            on=["RegioS", "Perioden"],
            how="left"
        )
        .groupBy("RegioS", "RegioNaam_1", "Perioden", "TypeWoning_3")
        .agg(
            F.avg("AantalWoningen_1").alias("avg_dwellings"),
            F.sum("AantalWoningen_1").alias("total_dwellings"),
            F.avg("GemiddeldeWOZWaarde_1").alias("avg_property_value"),
            F.max("ingestion_date").alias("last_updated")
        )
        .withColumn("gold_table", F.lit("gold_cbs_housing_market_analysis"))
    )
    
    gold_path = f"{GOLD_PATH}/cbs/gold_cbs_housing_market_analysis"
    gold_df.write.mode("overwrite").parquet(gold_path)
    
    logger.info(f"Table created: {gold_df.count()} records")


def create_gold_cbs_socioeconomic_indicators() -> None:
    """Create analytical table for socioeconomic indicators."""
    logger.info("Creating gold_cbs_socioeconomic_indicators")
    
    income_df = spark.read.parquet(f"{SILVER_PATH}/cbs/silver_cbs_household_income")
    neighbourhood_df = spark.read.parquet(f"{SILVER_PATH}/cbs/silver_cbs_neighbourhood_key_figures")
    
    # Join income with neighbourhood data
    gold_df = (income_df
        .join(
            neighbourhood_df.select("RegioS", "Perioden", "GemiddeldInkomenPerInwoner_25"),
            on=["RegioS", "Perioden"],
            how="inner"
        )
        .groupBy("RegioS", "RegioNaam_1", "Perioden")
        .agg(
            F.avg("GemiddeldInkomenHuishouden_6").alias("avg_household_income"),
            F.avg("MediaanInkomenHuishouden_7").alias("median_household_income"),
            F.avg("GemiddeldInkomenPerInwoner_25").alias("avg_income_per_resident"),
            F.max("ingestion_date").alias("last_updated")
        )
        .withColumn("gold_table", F.lit("gold_cbs_socioeconomic_indicators"))
    )
    
    gold_path = f"{GOLD_PATH}/cbs/gold_cbs_socioeconomic_indicators"
    gold_df.write.mode("overwrite").parquet(gold_path)
    
    logger.info(f"Table created: {gold_df.count()} records")


# ============================================================================
# Multi-Source Aggregations
# ============================================================================

def create_gold_regional_dashboard() -> None:
    """Create consolidated regional dashboard table."""
    logger.info("Creating gold_regional_dashboard")
    
    try:
        # Read from all available silver tables
        neighbourhood_df = spark.read.parquet(f"{SILVER_PATH}/cbs/silver_cbs_neighbourhood_key_figures")
        woz_df = spark.read.parquet(f"{SILVER_PATH}/cbs/silver_cbs_average_woz_value")
        housing_df = spark.read.parquet(f"{SILVER_PATH}/cbs/silver_cbs_housing_stock")
        income_df = spark.read.parquet(f"{SILVER_PATH}/cbs/silver_cbs_household_income")
        
        # Create consolidated view
        gold_df = (neighbourhood_df
            .select("RegioS", "RegioNaam", "Perioden", "Inwoners_5", "AantalHuishoudens_10")
            .join(
                woz_df.select("RegioS", "Perioden", "GemiddeldeWOZWaarde_1"),
                on=["RegioS", "Perioden"],
                how="left"
            )
            .join(
                income_df.select("RegioS", "Perioden", "GemiddeldInkomenPerInwoner_25"),
                on=["RegioS", "Perioden"],
                how="left"
            )
            .withColumn("gold_table", F.lit("gold_regional_dashboard"))
            .withColumn("created_timestamp", F.current_timestamp())
        )
        
        gold_path = f"{GOLD_PATH}/consolidated/gold_regional_dashboard"
        gold_df.write.mode("overwrite").parquet(gold_path)
        
        logger.info(f"Regional dashboard created: {gold_df.count()} records")
    except Exception as e:
        logger.warning(f"Regional dashboard creation had issues: {e}")


# ============================================================================
# Main Execution
# ============================================================================

if __name__ == "__main__":
    logger.info("Starting silver to gold transformations")
    
    try:
        # Create CBS analytical tables
        create_gold_cbs_neighbourhood_analysis()
        create_gold_cbs_property_value_trends()
        create_gold_cbs_housing_market_analysis()
        create_gold_cbs_socioeconomic_indicators()
        
        # Create multi-source aggregations
        create_gold_regional_dashboard()
        
        logger.info("All silver to gold transformations completed successfully")
        
    except Exception as e:
        logger.error(f"Error in silver to gold transformation: {e}")
        raise