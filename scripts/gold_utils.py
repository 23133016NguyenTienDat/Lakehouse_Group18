#!/usr/bin/env python3
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import col, lit, row_number, sha2, concat_ws, coalesce, max as spark_max
from pyspark.sql.window import Window

from silver_utils import logger

GOLD_BASE_PATH = "hdfs://namenode:8020/lakehouse/gold"
GOLD_WATERMARK_TABLE = "gold.pipeline_watermark"
GOLD_WATERMARK_PATH = f"{GOLD_BASE_PATH}/pipeline_watermark"

def create_gold_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport()
        .getOrCreate()
    )

def _ensure_gold_watermark_table(spark: SparkSession):
    create_stmt = f"""
        CREATE TABLE IF NOT EXISTS {GOLD_WATERMARK_TABLE} (
            pipeline_name STRING,
            last_process_date STRING,
            updated_at TIMESTAMP
        )
        USING DELTA
        LOCATION '{GOLD_WATERMARK_PATH}'
        """

    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    try:
        spark.sql(create_stmt)
        spark.sql(f"REFRESH TABLE {GOLD_WATERMARK_TABLE}")
        spark.table(GOLD_WATERMARK_TABLE).limit(1).collect()
    except Exception as exc:
        logger.warning(f"Recreating broken watermark table metadata: {exc}")
        spark.sql(f"DROP TABLE IF EXISTS {GOLD_WATERMARK_TABLE}")
        spark.sql(create_stmt)
        spark.sql(f"REFRESH TABLE {GOLD_WATERMARK_TABLE}")

def read_silver(spark: SparkSession, dataset: str, process_date: str | None = None) -> DataFrame:
    path = f"hdfs://namenode:8020/lakehouse/silver/{dataset}"
    logger.info(f"Reading Silver: {path}")
    df = spark.read.format("delta").load(path)
    if process_date and "silver_process_date" in df.columns:
        logger.info(f"Applying Silver process_date filter: {process_date}")
        df = df.filter(col("silver_process_date") == lit(process_date))
    return df

def read_silver_since(spark: SparkSession, dataset: str, watermark_date: str) -> DataFrame:
    path = f"hdfs://namenode:8020/lakehouse/silver/{dataset}"
    logger.info(f"Reading Silver delta since {watermark_date}: {path}")
    df = spark.read.format("delta").load(path)
    if "silver_process_date" in df.columns:
        return df.filter(col("silver_process_date") > lit(watermark_date))
    return df.limit(0)

def delta_table_exists(spark: SparkSession, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(1).collect()
        return True
    except Exception:
        return False

def read_delta_if_exists(spark: SparkSession, path: str) -> DataFrame | None:
    if delta_table_exists(spark, path):
        return spark.read.format("delta").load(path)
    return None

def write_gold_overwrite(
    df: DataFrame,
    spark: SparkSession,
    table_name: str,
    path: str,
    partition_col: str = None,
):
    full_name = f"gold.{table_name}"
    logger.info(f"Writing Gold (OVERWRITE): {full_name} -> {path}")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    writer = df.write.mode("overwrite").format("delta").option("path", path)
    if partition_col and partition_col in df.columns:
        writer = writer.partitionBy(partition_col)
    writer.saveAsTable(full_name)
    spark.sql(f"REFRESH TABLE {full_name}")

def write_gold_merge(
    df: DataFrame,
    spark: SparkSession,
    table_name: str,
    path: str,
    key_cols: list[str],
    surrogate_col: str | None = None,
):
    full_name = f"gold.{table_name}"
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    logger.info(f"Writing Gold (MERGE): {full_name} -> {path}")

    if not delta_table_exists(spark, path):
        (
            df.write.mode("overwrite")
            .format("delta")
            .option("path", path)
            .saveAsTable(full_name)
        )
        spark.sql(f"REFRESH TABLE {full_name}")
        return

    prepared_df = df
    if surrogate_col and surrogate_col in df.columns:
        existing_surrogate = (
            spark.read.format("delta").load(path)
            .select(*key_cols, col(surrogate_col).alias("_existing_surrogate"))
            .dropDuplicates(key_cols)
        )

        prepared_df = (
            df.alias("incoming")
            .join(existing_surrogate.alias("existing"), on=key_cols, how="left")
        )
        max_surrogate_row = (
            spark.read.format("delta").load(path)
            .agg(spark_max(surrogate_col).alias("max_surrogate"))
            .first()
        )
        max_surrogate = max_surrogate_row["max_surrogate"] or 0

        new_rows = (
            prepared_df.where(col("_existing_surrogate").isNull())
            .withColumn(
                surrogate_col,
                row_number().over(Window.orderBy(*[col(k) for k in key_cols])) + lit(max_surrogate),
            )
            .drop("_existing_surrogate")
        )
        existing_rows = (
            prepared_df.where(col("_existing_surrogate").isNotNull())
            .withColumn(surrogate_col, col("_existing_surrogate"))
            .drop("_existing_surrogate")
        )
        prepared_df = existing_rows.unionByName(new_rows)

    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in key_cols])
    source_view = f"tmp_merge_{table_name}"
    prepared_df.createOrReplaceTempView(source_view)
    update_assignments = ", ".join([f"target.{c} = source.{c}" for c in prepared_df.columns])
    insert_columns = ", ".join(prepared_df.columns)
    insert_values = ", ".join([f"source.{c}" for c in prepared_df.columns])
    spark.sql(
        f"""
        MERGE INTO delta.`{path}` AS target
        USING {source_view} AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_name} USING DELTA LOCATION '{path}'")
    spark.sql(f"REFRESH TABLE {full_name}")

def get_latest_silver_process_date(spark: SparkSession, datasets: list[str]) -> str | None:
    latest_values = []
    for dataset in datasets:
        df = read_silver(spark, dataset)
        if "silver_process_date" not in df.columns:
            continue
        row = df.agg(spark_max("silver_process_date").alias("max_date")).first()
        if row and row["max_date"] is not None:
            latest_values.append(str(row["max_date"]))
    if not latest_values:
        return None
    return max(latest_values)

def get_gold_watermark(spark: SparkSession, pipeline_name: str) -> str | None:
    _ensure_gold_watermark_table(spark)

    row = (
        spark.table(GOLD_WATERMARK_TABLE)
        .where(col("pipeline_name") == lit(pipeline_name))
        .agg(spark_max("last_process_date").alias("last_process_date"))
        .first()
    )
    return None if row is None or row["last_process_date"] is None else str(row["last_process_date"])

def upsert_gold_watermark(spark: SparkSession, pipeline_name: str, process_date: str):
    _ensure_gold_watermark_table(spark)

    spark.sql(f"DELETE FROM {GOLD_WATERMARK_TABLE} WHERE pipeline_name = '{pipeline_name}'")
    spark.sql(
        f"""
        INSERT INTO {GOLD_WATERMARK_TABLE} (pipeline_name, last_process_date, updated_at)
        VALUES ('{pipeline_name}', '{process_date}', current_timestamp())
        """
    )


def _normalize_order_cols(order_cols: list) -> list[Column]:
    normalized = []
    for order_col in order_cols:
        if isinstance(order_col, Column):
            normalized.append(order_col)
        else:
            normalized.append(col(order_col))
    return normalized

def assign_surrogate_key(
    df: DataFrame,
    key_col: str,
    order_cols: list,
    start_at: int = 0,
) -> DataFrame:
    window = Window.orderBy(*_normalize_order_cols(order_cols))
    return df.withColumn(key_col, row_number().over(window) + lit(start_at))

def build_static_dimension(
    source_df: DataFrame,
    value_col: str,
    key_col: str,
    existing_df: DataFrame | None = None,
) -> DataFrame:
    source_values = (
        source_df.select(value_col)
        .where(col(value_col).isNotNull())
        .dropDuplicates([value_col])
    )

    if existing_df is None:
        return (
            assign_surrogate_key(source_values, key_col, [value_col])
            .select(key_col, value_col)
        )

    existing_values = existing_df.select(key_col, value_col).dropDuplicates([value_col])
    max_key_row = existing_values.agg(spark_max(key_col).alias("max_key")).first()
    max_key = max_key_row["max_key"] or 0

    new_values = source_values.join(
        existing_values.select(value_col),
        on=value_col,
        how="left_anti",
    )

    new_rows = (
        assign_surrogate_key(new_values, key_col, [value_col], start_at=max_key)
        .select(key_col, value_col)
    )

    return existing_values.unionByName(new_rows)

def _hash_attributes(df: DataFrame, attribute_cols: list[str]) -> DataFrame:
    hash_inputs = [
        coalesce(col(column_name).cast("string"), lit("__NULL__"))
        for column_name in attribute_cols
    ]
    return df.withColumn("_attr_hash", sha2(concat_ws("||", *hash_inputs), 256))

def build_scd2_dimension(
    source_df: DataFrame,
    natural_key: str,
    surrogate_key: str,
    attribute_cols: list[str],
    process_ts: str,
    existing_df: DataFrame | None = None,
) -> DataFrame:
    process_ts_col = lit(process_ts).cast("timestamp")
    source = source_df.select(natural_key, *attribute_cols).dropDuplicates([natural_key])
    final_cols = [
        surrogate_key,
        natural_key,
        *attribute_cols,
        "effective_from",
        "effective_to",
        "is_current",
    ]

    if existing_df is None:
        return (
            assign_surrogate_key(source, surrogate_key, [natural_key])
            .withColumn("effective_from", process_ts_col)
            .withColumn("effective_to", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
            .select(*final_cols)
        )

    existing = existing_df.select(*final_cols)
    existing_current = existing.filter(col("is_current") == True)
    existing_history = existing.filter(col("is_current") == False)

    source_hashed = _hash_attributes(source, attribute_cols)
    current_hashed = _hash_attributes(existing_current, attribute_cols)

    joined = source_hashed.alias("source").join(
        current_hashed.alias("current"),
        on=natural_key,
        how="left",
    )

    new_rows = joined.filter(col(f"current.{surrogate_key}").isNull()).select("source.*")
    changed_rows = joined.filter(
        col(f"current.{surrogate_key}").isNotNull() &
        (col("source._attr_hash") != col("current._attr_hash"))
    ).select("source.*")

    changed_keys = changed_rows.select(natural_key).dropDuplicates([natural_key])

    kept_current = existing_current.join(changed_keys, on=natural_key, how="left_anti")
    expired_current = (
        existing_current.join(changed_keys, on=natural_key, how="inner")
        .withColumn("effective_to", process_ts_col)
        .withColumn("is_current", lit(False))
    )

    max_key_row = existing.agg(spark_max(surrogate_key).alias("max_key")).first()
    max_key = max_key_row["max_key"] or 0

    incoming_rows = (
        new_rows.unionByName(changed_rows)
        .drop("_attr_hash")
        .dropDuplicates([natural_key])
    )

    new_versions = (
        assign_surrogate_key(incoming_rows, surrogate_key, [natural_key], start_at=max_key)
        .withColumn("effective_from", process_ts_col)
        .withColumn("effective_to", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
    )

    return (
        existing_history.select(*final_cols)
        .unionByName(expired_current.select(*final_cols))
        .unionByName(kept_current.select(*final_cols))
        .unionByName(new_versions.select(*final_cols))
    )
