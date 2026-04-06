#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql.functions import col, lit, lower, trim
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


def data_quality_check(df):
    # Đếm record có user_id null hoặc empty string
    null_count = df.filter(
        (col("user_id").isNull()) | (col("user_id") == "")
    ).count()
    
    if null_count > 0:
        error_msg = f"Data Quality Check FAILED: Found {null_count} records with null/empty user_id"
        print(f" {error_msg}")
        raise Exception(error_msg)
    
    print(f"Data Quality Check PASSED: All {df.count()} records have valid user_id")
    return True


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
        
        initial_count = df.count()
        print(f" Read {initial_count} records")
        
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
                raise Exception("Cannot find user_id or any ID column to map from!")
        
        df = enforce_user_id(df)
        
        # 5. Loại bỏ records có user_id null/empty
        print(f"\n Step 4: Removing records with null/empty user_id")
        df_clean = df.filter(
            (col("user_id").isNotNull()) & (col("user_id") != "")
        )
        clean_count = df_clean.count()
        removed_count = initial_count - clean_count
        
        if removed_count > 0:
            print(f"Removed {removed_count} records with invalid user_id")
        print(f"Clean records: {clean_count}")
        
        # 6. Data Quality Check
        print(f"\nStep 5: Data Quality Check")
        data_quality_check(df_clean)
        
        # 7. Thêm cột ingest_date
        print(f"\nStep 6: Adding ingest_date column")
        df_final = df_clean.withColumn("ingest_date", lit(ingest_date))
        
        # Show sample data
        print(f"\nSample data (first 5 rows):")
        df_final.show(5, truncate=False)
        
        # 8. Ghi ra HDFS dạng Parquet với partition
        output_full_path = f"{hdfs_output_path}/{dataset_name}"
        print(f"\nStep 7: Writing to HDFS")
        print(f"   - Output path: {output_full_path}")
        print(f"   - Format: Parquet (Snappy compression)")
        print(f"   - Partitions: ingest_date only (region not available in all datasets)")
        print(f"   - Mode: append (immutability)")
        
        # Chỉ partition theo ingest_date (vì không phải dataset nào cũng có region)
        df_final.write \
            .mode("append") \
            .partitionBy("ingest_date") \
            .parquet(output_full_path)
        
        print(f"  Successfully wrote {clean_count} records to Bronze layer")
        
        # 9. Verification - đọc lại để verify
        print(f"\nStep 8: Verification")
        verification_df = spark.read.parquet(output_full_path)
        total_records = verification_df.count()
        print(f"  Verified: {total_records} total records in Bronze layer")
        
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
        # Tự động lấy dataset name từ filename (bỏ .csv)
        dataset_name = csv_filename.replace(".csv", "")
    
    # Đường dẫn động theo tham số
    CSV_INPUT_PATH = f"file:///opt/project/data/{csv_filename}"
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
