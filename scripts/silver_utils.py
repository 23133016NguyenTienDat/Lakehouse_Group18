#!/usr/bin/env python3
from __future__ import annotations

"""
Silver Layer Utilities - Common functions for all Silver transformations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, trim, when, current_timestamp, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    """Create Spark session with Delta Lake config"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def read_bronze(spark: SparkSession, dataset: str, process_date: str | None = None) -> DataFrame:
    """Read from Bronze layer with optional ingest_date filtering for incremental runs."""
    path = f"hdfs://namenode:8020/lakehouse/bronze/{dataset}"
    logger.info(f"Reading Bronze: {path}")
    df = spark.read.format("delta").load(path)
    if process_date:
        logger.info(f"Applying Bronze ingest_date filter: {process_date}")
        df = df.filter(col("ingest_date") == lit(process_date))
    return df


def filter_valid_records(df: DataFrame) -> DataFrame:
    """Filter records with valid record_id"""
    return df.filter(col("is_valid_record_id") == True)


def safe_trim(df: DataFrame, columns: list) -> DataFrame:
    """Trim strings and convert empty to NULL"""
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, 
                when(trim(col(c)) == "", None).otherwise(trim(col(c)))
            )
    return df


def deduplicate(df: DataFrame, key_cols: list, order_cols: list = None) -> DataFrame:
    """Deduplicate by key, keeping latest record"""
    if order_cols is None:
        order_cols = [col("ingestion_timestamp").desc()]
    
    window = Window.partitionBy(*key_cols).orderBy(*order_cols)
    return df.withColumn("_rn", row_number().over(window)) \
             .filter(col("_rn") == 1) \
             .drop("_rn")


def add_silver_metadata(df: DataFrame, process_date: str) -> DataFrame:
    """Add Silver layer metadata columns"""
    return df.withColumn("silver_processed_at", current_timestamp()) \
             .withColumn("silver_process_date", lit(process_date))


def drop_bronze_columns(df: DataFrame) -> DataFrame:
    """Remove Bronze internal columns"""
    bronze_cols = ["record_id", "record_id_source", "dq_error", 
                   "is_valid_record_id", "ingest_date", "ingestion_timestamp", "source_file"]
    return df.drop(*[c for c in bronze_cols if c in df.columns])


def write_silver_merge(df: DataFrame, spark: SparkSession, path: str, key_cols: list):
    """
    Write to Silver using Delta MERGE (upsert) - idempotent & incremental
    """
    logger.info(f"Writing Silver (MERGE): {path}")
    
    # Check if table exists by trying to read it
    table_exists = False
    try:
        spark.read.format("delta").load(path).limit(1).collect()
        table_exists = True
    except Exception:
        table_exists = False
    
    if table_exists:
        delta_table = DeltaTable.forPath(spark, path)
        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in key_cols])
        
        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        
        logger.info("MERGE completed (upsert)")
    else:
        logger.info("Table not found, creating new...")
        df.write.mode("overwrite").format("delta").save(path)
        logger.info("Initial write completed")


def write_silver_overwrite(df: DataFrame, path: str, partition_col: str = None):
    """Write to Silver with overwrite (for dimension tables)"""
    logger.info(f"Writing Silver (OVERWRITE): {path}")
    writer = df.write.mode("overwrite").format("delta")
    if partition_col and partition_col in df.columns:
        writer = writer.partitionBy(partition_col)
    writer.save(path)
