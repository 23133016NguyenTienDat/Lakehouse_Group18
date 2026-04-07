#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, lit, trim, when, current_timestamp, input_file_name
import sys
from datetime import datetime


def create_schema():       
    return None


def normalize_column_names(df):
    """
    Chuẩn hóa tên cột:
    """
    for column in df.columns:
        # Chuyển về lowercase và replace space với underscore
        normalized_name = column.lower().strip().replace(" ", "_")
        df = df.withColumnRenamed(column, normalized_name)
    return df


def enforce_user_id(df):
    # Ép kiểu user_id sang string và trim
    df = df.withColumn("user_id", trim(col("user_id").cast(StringType())))
    return df


def add_data_quality_flags(df):
    # NEW: Bronze giữ nguyên dữ liệu thô và chỉ gắn cờ chất lượng dữ liệu, không drop record.
    return (
        df.withColumn(
            "dq_error",
            when(col("user_id").isNull(), lit("NULL_USER_ID"))
            .when(col("user_id") == "", lit("EMPTY_USER_ID"))
            .otherwise(lit(None).cast(StringType()))
        )
        .withColumn("is_valid_user_id", col("dq_error").isNull())
    )


def ingest_to_bronze(csv_path, hdfs_output_path, ingest_date, dataset_name="events"):
    # 1. Khởi tạo SparkSession
    print("=" * 80)
    print("Starting Bronze Layer Ingestion")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName(f"Bronze_Ingestion_{dataset_name}") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .getOrCreate()
    
    print(f"\nConfiguration:")
    print(f"   - CSV Input: {csv_path}")
    print(f"   - HDFS Output: {hdfs_output_path}")
    print(f"   - Dataset: {dataset_name}")
    print(f"   - Ingest Date: {ingest_date}")
    
    try:
        # 2. Đọc CSV với schema inference (auto-detect)
        print(f"\nStep 1: Reading CSV from {csv_path}")
        
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(csv_path)

        # UPDATED: Tránh full scan bằng count() để pipeline scale tốt hơn.
        print("CSV read completed")
        
        # 3. Chuẩn hóa tên cột
        print(f"\nStep 2: Normalizing column names")
        df = normalize_column_names(df)
        print(f" Columns: {', '.join(df.columns)}")
        
        # 4. Enforce user_id (map từ session_id hoặc event_id nếu không có)
        print(f"\nStep 3: Enforcing user_id constraints")
        
        # Check nếu chưa có cột user_id → tạo từ session_id hoặc event_id
        if "user_id" not in df.columns:
            if "session_id" in df.columns:
                print(f"   Mapping user_id from session_id (user_id column not found)")
                df = df.withColumn("user_id", col("session_id").cast(StringType()))
            elif "event_id" in df.columns:
                print(f"   Mapping user_id from event_id (user_id column not found)")
                df = df.withColumn("user_id", col("event_id").cast(StringType()))
            elif "customer_id" in df.columns:
                print(f"    Mapping user_id from customer_id (user_id column not found)")
                df = df.withColumn("user_id", col("customer_id").cast(StringType()))
            elif "order_id" in df.columns:
                print(f"   Mapping user_id from order_id (user_id column not found)")
                df = df.withColumn("user_id", col("order_id").cast(StringType()))
            elif "product_id" in df.columns:
                print(f" Mapping user_id from product_id (user_id column not found)")
                df = df.withColumn("user_id", col("product_id").cast(StringType()))
            elif "review_id" in df.columns:
                print(f"  Mapping user_id from review_id (user_id column not found)")
                df = df.withColumn("user_id", col("review_id").cast(StringType()))
            else:
                # UPDATED: Không fail pipeline, gắn user_id null để đi tiếp và đánh dấu DQ.
                print(" Warning: Cannot find user_id or fallback ID column. user_id will be set to NULL")
                df = df.withColumn("user_id", lit(None).cast(StringType()))
        
        df = enforce_user_id(df)

        # UPDATED: Giữ toàn bộ record, chỉ gắn cờ chất lượng dữ liệu.
        print(f"\nStep 4: Adding Data Quality flags (no record drop)")
        df_with_dq = add_data_quality_flags(df)

        # NEW: Thêm metadata chuẩn Bronze để truy vết nguồn dữ liệu.
        print(f"\nStep 5: Adding Bronze metadata columns")
        df_final = (
            df_with_dq
            .withColumn("ingest_date", lit(ingest_date))
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
        )
        
        # Show sample data
        print(f"\nSample data (first 5 rows):")
        df_final.show(5, truncate=False)
        
        # UPDATED: Ghi Bronze dưới dạng Delta Lake với mergeSchema.
        output_full_path = f"{hdfs_output_path}/{dataset_name}"
        print(f"\nStep 7: Writing to HDFS")
        print(f"   - Output path: {output_full_path}")
        print(f"   - Format: Delta Lake")
        print(f"   - mergeSchema: true")
        print(f"   - Partitions: ingest_date only (region not available in all datasets)")
        print(f"   - Mode: append (immutability)")
        
        # Chỉ partition theo ingest_date (vì không phải dataset nào cũng có region)
        df_final.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("ingest_date") \
            .format("delta") \
            .save(output_full_path)
        
        print("Write to Bronze Delta layer completed")
        
        # 9. Verification - đọc lại để verify
        print(f"\nStep 8: Verification")
        # UPDATED: Đọc lại bằng Delta API, tránh count() để không full scan.
        verification_df = spark.read.format("delta").load(output_full_path)
        print("Delta read verification completed")
        
        # Show partition structure
        print(f"\nPartition structure:")
        verification_df.select("ingest_date") \
            .distinct() \
            .orderBy("ingest_date") \
            .show(20, truncate=False)
        
        print("\n" + "=" * 80)
        print("Bronze Layer Ingestion COMPLETED Successfully")
        print("=" * 80)
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        spark.stop()
        raise e
    
    finally:
        spark.stop()


if __name__ == "__main__":    
    # Parse arguments
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
    
    # UPDATED: Đọc dữ liệu từ thư mục raw để khớp với layout hiện tại của project.
    CSV_INPUT_PATH = f"file:///opt/project/data/raw/{csv_filename}"
    HDFS_OUTPUT_BASE = "hdfs://namenode:8020/lakehouse/bronze_delta"  # UPDATED
    
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
