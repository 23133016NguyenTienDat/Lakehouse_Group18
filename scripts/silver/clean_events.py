#!/usr/bin/env python3
"""Silver Layer: Events - Bronze to Silver transformation"""

import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql.functions import col, when, to_timestamp, to_date, hour, concat_ws
from pyspark.sql.types import IntegerType
from silver_utils import (
    create_spark_session, read_bronze, filter_valid_records,
    safe_trim, deduplicate, add_silver_metadata, drop_bronze_columns,
    write_silver_merge, logger
)

SILVER_PATH = "hdfs://namenode:8020/lakehouse/silver/events"


def clean_events(process_date: str):
    logger.info("=== SILVER: EVENTS ===")
    spark = create_spark_session("Silver_Events")
    
    try:
        df = read_bronze(spark, "events", process_date)
        df = filter_valid_records(df)
        
        # Parse timestamp
        df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
        
        # Cast product_id from float to int (raw data has 93.0, 1005.0 format)
        df = df.withColumn("product_id", col("product_id").cast(IntegerType()))
        
        df = safe_trim(df, ["event_type", "payment"])
        
        df = deduplicate(df, ["event_id"], 
                        [col("timestamp").desc(), col("ingestion_timestamp").desc()])
        
        # Validations: base + event-type specific
        df = df.withColumn("validation_errors", concat_ws(",",
            # Base validations
            when(col("event_id").isNull(), "NULL_EVENT_ID"),
            when(col("session_id").isNull(), "NULL_SESSION_ID"),
            when(col("event_type").isNull(), "NULL_EVENT_TYPE"),
            when(col("timestamp").isNull(), "NULL_TIMESTAMP"),
            # Event-type specific validations
            when((col("event_type") == "add_to_cart") & col("qty").isNull(), "ADD_TO_CART_MISSING_QTY"),
            when((col("event_type") == "checkout") & col("cart_size").isNull(), "CHECKOUT_MISSING_CART_SIZE"),
            when((col("event_type") == "purchase") & col("amount_usd").isNull(), "PURCHASE_MISSING_AMOUNT"),
            when((col("event_type") == "purchase") & col("payment").isNull(), "PURCHASE_MISSING_PAYMENT"),
            when((col("event_type").isin("page_view", "add_to_cart")) & col("product_id").isNull(), "MISSING_PRODUCT_ID")
        ))
        df = df.withColumn("validation_errors",
            when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
        
        # Derived columns
        df = df.withColumn("event_date", to_date(col("timestamp")))
        df = df.withColumn("event_hour", hour(col("timestamp")))
        
        df = add_silver_metadata(df, process_date)
        df = drop_bronze_columns(df)
        
        write_silver_merge(df, spark, SILVER_PATH, ["event_id"])
        
        logger.info("=== EVENTS COMPLETED ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    clean_events(process_date)
