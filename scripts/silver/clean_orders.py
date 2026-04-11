#!/usr/bin/env python3
"""Silver Layer: Orders & Order Items - Bronze to Silver transformation"""

import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql.functions import (
    col, when, to_timestamp, to_date, year, month, hour, concat_ws, sum as spark_sum, abs as spark_abs
)
from silver_utils import (
    create_spark_session, read_bronze, filter_valid_records,
    safe_trim, deduplicate, add_silver_metadata, drop_bronze_columns,
    write_silver_merge, logger
)

ORDERS_SILVER = "hdfs://namenode:8020/lakehouse/silver/orders"
ORDER_ITEMS_SILVER = "hdfs://namenode:8020/lakehouse/silver/order_items"


def clean_orders(spark, process_date: str):
    logger.info("--- Processing: ORDERS ---")
    
    df = read_bronze(spark, "orders", process_date)
    df = filter_valid_records(df)
    
    # Parse & normalize
    df = df.withColumn("order_time", to_timestamp(col("order_time")))
    df = safe_trim(df, ["payment_method", "country", "device", "source"])
    
    # Deduplicate by order_id, prefer latest order_time
    df = deduplicate(df, ["order_id"], 
                    [col("order_time").desc(), col("ingestion_timestamp").desc()])
    
    # Validations
    df = df.withColumn("validation_errors", concat_ws(",",
        when(col("order_id").isNull(), "NULL_ORDER_ID"),
        when(col("customer_id").isNull(), "NULL_CUSTOMER_ID"),
        when(col("order_time").isNull(), "NULL_ORDER_TIME"),
        when(col("total_usd").isNull(), "NULL_TOTAL"),
        when(col("total_usd") < 0, "NEGATIVE_TOTAL"),
        when(col("subtotal_usd") < col("total_usd"), "SUBTOTAL_LESS_THAN_TOTAL"),
        when(~((col("discount_pct") >= 0) & (col("discount_pct") <= 100)), "INVALID_DISCOUNT")
    ))
    df = df.withColumn("validation_errors",
        when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
    
    # Derived columns
    df = df.withColumn("order_date", to_date(col("order_time")))
    df = df.withColumn("order_year", year(col("order_time")))
    df = df.withColumn("order_month", month(col("order_time")))
    df = df.withColumn("order_hour", hour(col("order_time")))
    df = df.withColumn("discount_amount", col("subtotal_usd") - col("total_usd"))
    
    df = add_silver_metadata(df, process_date)
    df = drop_bronze_columns(df)
    
    write_silver_merge(df, spark, ORDERS_SILVER, ["order_id"])


def clean_order_items(spark, process_date: str):
    """
    Order items: fact table with potential duplicate (order_id, product_id) keys.
    Strategy: Aggregate duplicates by summing quantity and line_total.
    """
    logger.info("--- Processing: ORDER_ITEMS ---")
    
    df = read_bronze(spark, "order_items", process_date)
    df = filter_valid_records(df)
    
    # Aggregate duplicates: same (order_id, product_id) -> sum quantity & line_total
    # Keep first unit_price (should be same for same product in same order)
    from pyspark.sql.functions import first
    df = df.groupBy("order_id", "product_id").agg(
        first("unit_price_usd").alias("unit_price_usd"),
        spark_sum("quantity").alias("quantity"),
        spark_sum("line_total_usd").alias("line_total_usd")
    )
    
    # Validations (including NULL checks)
    df = df.withColumn("validation_errors", concat_ws(",",
        when(col("order_id").isNull(), "NULL_ORDER_ID"),
        when(col("product_id").isNull(), "NULL_PRODUCT_ID"),
        when(col("quantity").isNull(), "NULL_QUANTITY"),
        when(col("unit_price_usd").isNull(), "NULL_UNIT_PRICE"),
        when(col("line_total_usd").isNull(), "NULL_LINE_TOTAL"),
        when(col("quantity") <= 0, "INVALID_QUANTITY"),
        when(col("unit_price_usd") < 0, "NEGATIVE_PRICE"),
        when(spark_abs(col("line_total_usd") - col("unit_price_usd") * col("quantity")) > 0.01, "LINE_TOTAL_MISMATCH")
    ))
    df = df.withColumn("validation_errors",
        when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
    
    # Derived columns
    df = df.withColumn("computed_line_total", col("unit_price_usd") * col("quantity"))
    
    df = add_silver_metadata(df, process_date)
    
    # Order items is a fact table (not dimension), no partition needed for small-medium size
    write_silver_merge(df, spark, ORDER_ITEMS_SILVER, ["order_id", "product_id"])


def main(process_date: str):
    logger.info("=== SILVER: ORDERS & ORDER_ITEMS ===")
    spark = create_spark_session("Silver_Orders")
    
    try:
        clean_orders(spark, process_date)
        clean_order_items(spark, process_date)
        logger.info("=== ORDERS & ORDER_ITEMS COMPLETED ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    main(process_date)
