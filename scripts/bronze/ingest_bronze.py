#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    TimestampType, LongType
)
from pyspark.sql.functions import col, lit, trim, when, current_timestamp, input_file_name, concat_ws
import sys
from datetime import datetime


# NEW: Deterministic dataset -> primary key mapping for canonical record_id generation.
dataset_primary_key = {
    "events": "event_id",
    "customers": "customer_id",
    "orders": "order_id",
    "products": "product_id",
    "reviews": "review_id",
    "sessions": "session_id",
    "order_items": None,
}


def create_events_schema():
    """Định nghĩa schema cố định cho events dataset"""
    return StructType([
        StructField("event_id", IntegerType(), True),
        StructField("session_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", DoubleType(), True),
        StructField("qty", IntegerType(), True),
        StructField("cart_size", IntegerType(), True),
        StructField("payment", StringType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("amount_usd", DoubleType(), True),
    ])


def create_customers_schema():
    """Định nghĩa schema cố định cho customers dataset"""
    return StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("signup_date", StringType(), True),
        StructField("marketing_opt_in", StringType(), True),
    ])


def create_orders_schema():
    """Định nghĩa schema cố định cho orders dataset"""
    return StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_time", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("subtotal_usd", DoubleType(), True),
        StructField("total_usd", DoubleType(), True),
        StructField("country", StringType(), True),
        StructField("device", StringType(), True),
        StructField("source", StringType(), True),
    ])


def create_products_schema():
    """Định nghĩa schema cố định cho products dataset"""
    return StructType([
        StructField("product_id", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price_usd", DoubleType(), True),
        StructField("cost_usd", DoubleType(), True),
        StructField("margin_usd", DoubleType(), True),
    ])


def create_reviews_schema():
    """Định nghĩa schema cố định cho reviews dataset"""
    return StructType([
        StructField("review_id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("review_text", StringType(), True),
        StructField("review_time", StringType(), True),
    ])


def create_sessions_schema():
    """Định nghĩa schema cố định cho sessions dataset"""
    return StructType([
        StructField("session_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("start_time", StringType(), True),
        StructField("device", StringType(), True),
        StructField("source", StringType(), True),
        StructField("country", StringType(), True),
    ])


def create_order_items_schema():
    """Định nghĩa schema cố định cho order_items dataset"""
    return StructType([
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("unit_price_usd", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("line_total_usd", DoubleType(), True),
    ])


def get_schema_for_dataset(dataset_name):
    """Trả về schema phù hợp theo dataset"""
    schema_mapping = {
        "events": create_events_schema(),
        "customers": create_customers_schema(),
        "orders": create_orders_schema(),
        "products": create_products_schema(),
        "reviews": create_reviews_schema(),
        "sessions": create_sessions_schema(),
        "order_items": create_order_items_schema(),
    }
    return schema_mapping.get(dataset_name, None)


def normalize_column_names(df):
    """
    Chuẩn hóa tên cột:
    """
    for column in df.columns:
        # Chuyển về lowercase và replace space với underscore
        normalized_name = column.lower().strip().replace(" ", "_")
        df = df.withColumnRenamed(column, normalized_name)
    return df


def add_canonical_record_id(df, dataset_name):
    """
    Tạo record_id chuẩn dựa trên dataset_name để tránh map sai semantic identity.
    """
    # REMOVED: Generic first-match selection is dangerous because column order/presence
    # can vary across datasets and produce non-deterministic or semantically wrong IDs.

    # UPDATED: order_items has no single natural key. Use composite business key.
    if dataset_name == "order_items":
        if "order_id" in df.columns and "product_id" in df.columns:
            print("   Mapping record_id from composite key: order_id + product_id")
            return (
                df.withColumn(
                    "record_id",
                    concat_ws("_", col("order_id").cast(StringType()), col("product_id").cast(StringType())),
                )
                .withColumn("record_id_source", lit("order_id_product_id"))
            )

        print("   Warning: Composite key columns not found for order_items. record_id will be NULL")
        return (
            df.withColumn("record_id", lit(None).cast(StringType()))
            .withColumn("record_id_source", lit("NOT_FOUND"))
        )

    # UPDATED: Deterministic primary key by dataset_name.
    primary_key_col = dataset_primary_key.get(dataset_name)
    if primary_key_col and primary_key_col in df.columns:
        print(f"   Mapping record_id from {primary_key_col}")
        return (
            df.withColumn("record_id", trim(col(primary_key_col).cast(StringType())))
            .withColumn("record_id_source", lit(primary_key_col))
        )

    # UPDATED: Null-safe fallback for schema drift / missing key column.
    print(
        f"   Warning: Expected primary key not found for dataset '{dataset_name}'. "
        "record_id will be NULL"
    )
    return (
        df.withColumn("record_id", lit(None).cast(StringType()))
        .withColumn("record_id_source", lit("NOT_FOUND"))
    )


def add_data_quality_flags(df):
    return (
        df.withColumn(
            "dq_error",
            when(col("record_id").isNull(), lit("NULL_RECORD_ID"))
            .when(col("record_id") == "", lit("EMPTY_RECORD_ID"))
            .otherwise(lit(None).cast(StringType()))
        )
        .withColumn("is_valid_record_id", col("dq_error").isNull())
    )




def ingest_to_bronze(csv_path, hdfs_output_path, ingest_date, dataset_name="events"):
    print("Starting Bronze Layer Ingestion")

    
    spark = SparkSession.builder \
        .appName(f"Bronze_Ingestion_{dataset_name}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .getOrCreate()
    
    try:
        print(f"\nStep 1: Reading CSV from {csv_path}")
        
        schema = get_schema_for_dataset(dataset_name)
        
        if schema:
            print(f"   Using predefined schema for {dataset_name}")
            df = spark.read \
                .option("header", "true") \
                .schema(schema) \
                .csv(csv_path)
        else:
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(csv_path)

        print("CSV read completed")
        
        print(f"\nStep 2: Normalizing column names")
        df = normalize_column_names(df)
        print(f" Columns: {', '.join(df.columns)}")
        
        print(f"\nStep 3: Creating canonical record_id (no fake identity)")
        df = add_canonical_record_id(df, dataset_name)

        print(f"\nStep 4: Adding Data Quality flags (no record drop)")
        df_with_dq = add_data_quality_flags(df)

        print(f"\nStep 5: Adding Bronze metadata columns")
        df_final = (
            df_with_dq
            .withColumn("ingest_date", lit(ingest_date))
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
        )
        
        print(f"\nSample data (first 5 rows):")
        df_final.show(5, truncate=False)
        

        output_full_path = f"{hdfs_output_path}/{dataset_name}"
        
        print(f"\nStep 7: Writing to Delta Lake")


        df_final.write \
            .mode("append") \
            .partitionBy("ingest_date") \
            .format("delta") \
            .save(output_full_path)
        
        print("Write to Bronze Delta layer completed")
        
        verification_df = spark.read.format("delta").load(output_full_path)
        

        print(f"\nSchema verification:")
        verification_df.printSchema()
        
        print(f"\nPartition structure (ingest_date only):")
        verification_df.select("ingest_date") \
            .distinct() \
            .orderBy("ingest_date") \
            .show(20, truncate=False)
        
        print(f"\nData quality summary:")
        verification_df.groupBy("is_valid_record_id") \
            .count() \
            .show(truncate=False)
        
        print("\n" + "=" * 80)
        print("Bronze Layer Ingestion COMPLETED Successfully")
        print("=" * 80)
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        spark.stop()
        raise e
    
    finally:
        spark.stop()


def main():
    if len(sys.argv) > 1:
        ingest_date = sys.argv[1]
    else:
        ingest_date = datetime.now().strftime("%Y-%m-%d")

    if len(sys.argv) > 2:
        csv_filename = sys.argv[2]
    else:
        csv_filename = "events.csv"

    if len(sys.argv) > 3:
        dataset_name = sys.argv[3]
    else:
        dataset_name = csv_filename.replace(".csv", "")

    CSV_INPUT_PATH = f"file:///opt/project/data/raw/{csv_filename}"
    HDFS_OUTPUT_BASE = "hdfs://namenode:8020/lakehouse/bronze"

    print(f"\n{'='*80}")
    print(f"Starting Bronze Ingestion Job")
    print(f"{'='*80}")
    print(f"   Ingest Date: {ingest_date}")
    print(f"   CSV File: {csv_filename}")
    print(f"   Dataset: {dataset_name}")
    print(f"   Input: {CSV_INPUT_PATH}")
    print(f"   Output: {HDFS_OUTPUT_BASE}/{dataset_name}")
    print(f"{'='*80}\n")
    
    # Chạy ingestion
    ingest_to_bronze(
        csv_path=CSV_INPUT_PATH,
        hdfs_output_path=HDFS_OUTPUT_BASE,
        ingest_date=ingest_date,
        dataset_name=dataset_name
    )


if __name__ == "__main__":
    main()
