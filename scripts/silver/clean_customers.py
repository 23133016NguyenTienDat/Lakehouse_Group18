#!/usr/bin/env python3
"""Silver Layer: Customers - Bronze to Silver transformation"""

import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql.functions import (
    col, when, lower, trim, to_date, year, split, concat_ws, lit
)
from pyspark.sql.types import BooleanType
from silver_utils import (
    create_spark_session, read_bronze, filter_valid_records,
    safe_trim, deduplicate, add_silver_metadata, drop_bronze_columns,
    write_silver_merge, logger
)

SILVER_PATH = "hdfs://namenode:8020/lakehouse/silver/customers"


def clean_customers(process_date: str):
    logger.info("=== SILVER: CUSTOMERS ===")
    spark = create_spark_session("Silver_Customers")
    
    try:
        df = read_bronze(spark, "customers", process_date)
        df = filter_valid_records(df)
        
        # Parse date
        df = df.withColumn("signup_date", to_date(col("signup_date")))
        
        # Parse boolean (trim first to handle " true ", "FALSE " etc)
        df = df.withColumn("marketing_opt_in",
            when(lower(trim(col("marketing_opt_in"))).isin("true", "1", "yes"), True)
            .when(lower(trim(col("marketing_opt_in"))).isin("false", "0", "no"), False)
            .otherwise(None).cast(BooleanType()))
        
        df = safe_trim(df, ["name", "email", "country"])
        
        df = deduplicate(df, ["customer_id"])
        
        # Use process_date for reproducible validation
        process_date_col = lit(process_date).cast("date")
        
        # Validations
        df = df.withColumn("validation_errors", concat_ws(",",
            when(col("customer_id").isNull(), "NULL_CUSTOMER_ID"),
            when(col("email").isNull(), "NULL_EMAIL"),
            when(col("signup_date").isNull(), "NULL_SIGNUP_DATE"),
            when(~col("email").rlike("^[A-Za-z0-9+_.-]+@(.+)$"), "INVALID_EMAIL_FORMAT"),
            when(col("age").isNull(), "NULL_AGE"),
            when(~((col("age") >= 0) & (col("age") <= 150)), "INVALID_AGE"),
            when(col("signup_date") > process_date_col, "FUTURE_SIGNUP_DATE")
        ))
        df = df.withColumn("validation_errors",
            when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
        
        # Derived columns (safe handling for nulls)
        df = df.withColumn("signup_year", year(col("signup_date")))
        df = df.withColumn("email_domain",
            when(col("email").contains("@"), split(col("email"), "@")[1]).otherwise(None))
        df = df.withColumn("age_group",
            when(col("age").isNull(), None)
            .when(col("age") < 0, None)
            .when(col("age") < 18, "under_18")
            .when(col("age") <= 25, "18-25")
            .when(col("age") <= 35, "26-35")
            .when(col("age") <= 50, "36-50")
            .when(col("age") <= 65, "51-65")
            .otherwise("over_65"))
        
        df = add_silver_metadata(df, process_date)
        df = drop_bronze_columns(df)
        
        write_silver_merge(df, spark, SILVER_PATH, ["customer_id"])
        
        logger.info("=== CUSTOMERS COMPLETED ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    clean_customers(process_date)
