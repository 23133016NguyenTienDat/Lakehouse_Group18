#!/usr/bin/env python3
"""Silver Layer: Reviews - Bronze to Silver transformation"""

import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql.functions import col, when, to_timestamp, to_date, year, length, concat_ws
from silver_utils import (
    create_spark_session, read_bronze, filter_valid_records,
    safe_trim, deduplicate, add_silver_metadata, drop_bronze_columns,
    write_silver_merge, logger
)

SILVER_PATH = "hdfs://namenode:8020/lakehouse/silver/reviews"


def clean_reviews(process_date: str):
    logger.info("=== SILVER: REVIEWS ===")
    spark = create_spark_session("Silver_Reviews")
    
    try:
        df = read_bronze(spark, "reviews", process_date)
        df = filter_valid_records(df)
        
        # Parse timestamp
        df = df.withColumn("review_time", to_timestamp(col("review_time")))
        
        df = safe_trim(df, ["review_text"])
        
        df = deduplicate(df, ["review_id"],
                        [col("review_time").desc(), col("ingestion_timestamp").desc()])
        
        # Validations (including NULL_REVIEW_TIME)
        df = df.withColumn("validation_errors", concat_ws(",",
            when(col("review_id").isNull(), "NULL_REVIEW_ID"),
            when(col("order_id").isNull(), "NULL_ORDER_ID"),
            when(col("product_id").isNull(), "NULL_PRODUCT_ID"),
            when(col("review_time").isNull(), "NULL_REVIEW_TIME"),
            when(col("rating").isNull(), "NULL_RATING"),
            when(~((col("rating") >= 1) & (col("rating") <= 5)), "INVALID_RATING")
        ))
        df = df.withColumn("validation_errors",
            when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
        
        # Derived columns (safe sentiment - handle null rating)
        df = df.withColumn("review_date", to_date(col("review_time")))
        df = df.withColumn("review_year", year(col("review_time")))
        df = df.withColumn("sentiment",
            when(col("rating").isNull(), None)
            .when(col("rating") >= 4, "positive")
            .when(col("rating") == 3, "neutral")
            .when(col("rating") >= 1, "negative")
            .otherwise(None))  # Invalid rating -> null sentiment
        df = df.withColumn("review_length", length(col("review_text")))
        
        df = add_silver_metadata(df, process_date)
        df = drop_bronze_columns(df)
        
        write_silver_merge(df, spark, SILVER_PATH, ["review_id"])
        
        logger.info("=== REVIEWS COMPLETED ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    clean_reviews(process_date)
