#!/usr/bin/env python3
"""Silver Layer: Products - Bronze to Silver transformation"""

import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql.functions import col, when, concat_ws, round as spark_round, abs as spark_abs
from silver_utils import (
    create_spark_session, read_bronze, filter_valid_records,
    safe_trim, deduplicate, add_silver_metadata, drop_bronze_columns,
    write_silver_overwrite, logger
)

SILVER_PATH = "hdfs://namenode:8020/lakehouse/silver/products"


def clean_products(process_date: str):
    logger.info("=== SILVER: PRODUCTS ===")
    spark = create_spark_session("Silver_Products")
    
    try:
        # Keep full-scan for products because this job uses overwrite semantics.
        df = read_bronze(spark, "products")
        df = filter_valid_records(df)
        
        df = safe_trim(df, ["category", "name"])
        
        df = deduplicate(df, ["product_id"])
        
        # Validations (including margin consistency check)
        df = df.withColumn("validation_errors", concat_ws(",",
            when(col("product_id").isNull(), "NULL_PRODUCT_ID"),
            when(col("name").isNull(), "NULL_NAME"),
            when(col("price_usd").isNull(), "NULL_PRICE"),
            when(col("cost_usd").isNull(), "NULL_COST"),
            when(col("price_usd") < 0, "NEGATIVE_PRICE"),
            when(col("cost_usd") < 0, "NEGATIVE_COST"),
            when(col("cost_usd") > col("price_usd"), "COST_EXCEEDS_PRICE"),
            when(col("margin_usd") < 0, "NEGATIVE_MARGIN"),
            when(spark_abs(col("margin_usd") - (col("price_usd") - col("cost_usd"))) > 0.01, "MARGIN_MISMATCH")
        ))
        df = df.withColumn("validation_errors",
            when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
        
        # Derived columns (safe handling for null/invalid)
        df = df.withColumn("margin_pct", 
            when(col("price_usd").isNull() | (col("price_usd") <= 0), None)
            .otherwise(spark_round(col("margin_usd") / col("price_usd") * 100, 2)))
        df = df.withColumn("price_tier",
            when(col("price_usd").isNull(), None)
            .when(col("price_usd") < 0, None)
            .when(col("price_usd") < 50, "budget")
            .when(col("price_usd") < 200, "mid_range")
            .when(col("price_usd") < 500, "premium")
            .otherwise("luxury"))
        
        df = add_silver_metadata(df, process_date)
        df = drop_bronze_columns(df)
        
        write_silver_overwrite(df, SILVER_PATH)
        
        logger.info("=== PRODUCTS COMPLETED ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    clean_products(process_date)
