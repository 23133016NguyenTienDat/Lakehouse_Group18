#!/usr/bin/env python3
"""Silver Layer: Sessions - Bronze to Silver transformation"""

import sys
from datetime import datetime
from pyspark.sql.functions import col, when, to_timestamp, to_date, year, month, hour, dayofweek, concat_ws, lit
from silver_utils import (
    create_spark_session, read_bronze, filter_valid_records,
    safe_trim, deduplicate, add_silver_metadata, drop_bronze_columns,
    write_silver_merge, logger
)

SILVER_PATH = "hdfs://namenode:8020/lakehouse/silver/sessions"


def clean_sessions(process_date: str):
    logger.info("=== SILVER: SESSIONS ===")
    spark = create_spark_session("Silver_Sessions")
    
    try:
        df = read_bronze(spark, "sessions")
        df = filter_valid_records(df)
        
        df = df.withColumn("start_time", to_timestamp(col("start_time")))
        
        df = safe_trim(df, ["device", "source", "country"])
        
        df = deduplicate(df, ["session_id"],
                        [col("start_time").desc(), col("ingestion_timestamp").desc()])
        
        # Use process_date for reproducible validation
        process_date_col = lit(process_date).cast("date")
        
        # Validations (including future timestamp check)
        df = df.withColumn("validation_errors", concat_ws(",",
            when(col("session_id").isNull(), "NULL_SESSION_ID"),
            when(col("customer_id").isNull(), "NULL_CUSTOMER_ID"),
            when(col("start_time").isNull(), "NULL_START_TIME"),
            when(to_date(col("start_time")) > process_date_col, "FUTURE_SESSION")
        ))
        df = df.withColumn("validation_errors",
            when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
        
        # Derived columns
        df = df.withColumn("session_date", to_date(col("start_time")))
        df = df.withColumn("session_year", year(col("start_time")))
        df = df.withColumn("session_month", month(col("start_time")))
        df = df.withColumn("session_hour", hour(col("start_time")))
        df = df.withColumn("is_weekend", dayofweek(col("start_time")).isin(1, 7))
        
        df = add_silver_metadata(df, process_date)
        df = drop_bronze_columns(df)
        
        write_silver_merge(df, spark, SILVER_PATH, ["session_id"], "session_date")
        
        logger.info("=== SESSIONS COMPLETED ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    clean_sessions(process_date)
