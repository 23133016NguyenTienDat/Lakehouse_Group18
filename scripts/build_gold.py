#!/usr/bin/env python3
from __future__ import annotations

"""Build a single Gold table for DAG-level orchestration."""

import sys
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from gold import (
    dim_category,
    dim_country,
    dim_customers,
    dim_date,
    dim_device,
    dim_payment_method,
    dim_products,
    dim_source,
    fact_customer_funnel,
    fact_order,
    fact_order_item,
    fact_review,
    fact_session,
    fact_web_events,
)
from gold.paths import GOLD_PATHS
from gold_utils import (
    create_gold_spark_session,
    get_gold_watermark,
    get_latest_silver_process_date,
    read_delta_if_exists,
    read_silver as read_silver_base,
    read_silver_since,
    upsert_gold_watermark,
    write_gold_merge,
    write_gold_overwrite,
)
from silver_utils import logger

SILVER_DATASETS = [
    "customers",
    "orders",
    "order_items",
    "products",
    "reviews",
    "sessions",
    "events",
]
PIPELINE_NAME = "gold_star_schema"

SUPPORTED_TABLES = {
    "dim_country",
    "dim_source",
    "dim_device",
    "dim_payment_method",
    "dim_category",
    "dim_date",
    "dim_customers",
    "dim_products",
    "fact_order",
    "fact_order_item",
    "fact_web_events",
    "fact_review",
    "fact_session",
    "fact_customer_funnel",
    "update_watermark",
}

def _write_gold_full(df: DataFrame, spark, table_name: str):
    write_gold_overwrite(df, spark, table_name, GOLD_PATHS[table_name])

def _write_gold_fact_incremental(
    df: DataFrame,
    spark,
    table_name: str,
    key_cols: list[str],
    surrogate_col: str,
):
    write_gold_merge(
        df=df,
        spark=spark,
        table_name=table_name,
        path=GOLD_PATHS[table_name],
        key_cols=key_cols,
        surrogate_col=surrogate_col,
    )

def _read_gold_required(spark, table_name: str) -> DataFrame:
    df = read_delta_if_exists(spark, GOLD_PATHS[table_name])
    if df is None:
        raise RuntimeError(f"Missing dependency gold.{table_name}. Run prerequisite Gold tasks first.")
    return df

def _is_empty(df: DataFrame) -> bool:
    return df.limit(1).count() == 0

def main(process_date: str, table_name: str, mode: str = "incremental"):
    if mode not in {"full", "incremental"}:
        raise ValueError("Mode must be one of: full, incremental")

    if table_name not in SUPPORTED_TABLES:
        supported = ", ".join(sorted(SUPPORTED_TABLES))
        raise ValueError(f"Unsupported table '{table_name}'. Supported tables: {supported}")

    spark = create_gold_spark_session(f"Gold_{table_name}")
    process_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    try:
        if table_name == "update_watermark":
            latest_processed = get_latest_silver_process_date(spark, SILVER_DATASETS)
            if latest_processed is not None:
                upsert_gold_watermark(spark, PIPELINE_NAME, latest_processed)
                logger.info(f"Gold watermark updated to {latest_processed}")
            else:
                logger.info("No Silver watermark found; skip watermark update.")
            return

        effective_mode = mode
        incremental_watermark = None
        has_new_silver = True
        if mode == "incremental":
            latest_silver_process_date = get_latest_silver_process_date(spark, SILVER_DATASETS)
            last_gold_watermark = get_gold_watermark(spark, PIPELINE_NAME)

            if latest_silver_process_date is None:
                logger.info("No Silver process watermark found; skipping table build.")
                return

            if last_gold_watermark is None:
                logger.info("Gold watermark not found; running initial full load for this table.")
                effective_mode = "full"
            elif latest_silver_process_date <= last_gold_watermark:
                logger.info(
                    "No new Silver watermark detected "
                    f"(latest={latest_silver_process_date}, gold={last_gold_watermark}); skipping table build."
                )
                has_new_silver = False
            else:
                incremental_watermark = last_gold_watermark

        if not has_new_silver:
            return

        customers_silver = read_silver_base(spark, "customers")
        orders_silver = read_silver_base(spark, "orders")
        order_items_silver = read_silver_base(spark, "order_items")
        products_silver = read_silver_base(spark, "products")
        reviews_silver = read_silver_base(spark, "reviews")
        sessions_silver = read_silver_base(spark, "sessions")
        events_silver = read_silver_base(spark, "events")

        valid_orders = orders_silver.where(col("validation_errors").isNull())
        valid_order_items = order_items_silver.where(col("validation_errors").isNull())
        valid_reviews = reviews_silver.where(col("validation_errors").isNull())
        valid_sessions = sessions_silver.where(col("validation_errors").isNull())
        valid_events = events_silver.where(col("validation_errors").isNull())
        events_with_customer = valid_events.join(
            valid_sessions.select("session_id", "customer_id"),
            on="session_id",
            how="inner",
        )

        if table_name == "dim_country":
            df = dim_country.build(
                customers_silver,
                orders_silver,
                sessions_silver,
                read_delta_if_exists(spark, GOLD_PATHS["dim_country"]),
            )
            _write_gold_full(df, spark, table_name)
            return

        if table_name == "dim_source":
            df = dim_source.build(
                orders_silver,
                sessions_silver,
                read_delta_if_exists(spark, GOLD_PATHS["dim_source"]),
            )
            _write_gold_full(df, spark, table_name)
            return

        if table_name == "dim_device":
            df = dim_device.build(
                orders_silver,
                sessions_silver,
                read_delta_if_exists(spark, GOLD_PATHS["dim_device"]),
            )
            _write_gold_full(df, spark, table_name)
            return

        if table_name == "dim_payment_method":
            df = dim_payment_method.build(
                orders_silver,
                events_silver,
                read_delta_if_exists(spark, GOLD_PATHS["dim_payment_method"]),
            )
            _write_gold_full(df, spark, table_name)
            return

        if table_name == "dim_category":
            df = dim_category.build(
                products_silver,
                read_delta_if_exists(spark, GOLD_PATHS["dim_category"]),
            )
            _write_gold_full(df, spark, table_name)
            return

        if table_name == "dim_date":
            df = dim_date.build(
                spark,
                customers_silver,
                orders_silver,
                reviews_silver,
                sessions_silver,
                events_silver,
            )
            _write_gold_full(df, spark, table_name)
            return

        if table_name == "dim_customers":
            df = dim_customers.build(
                customers_silver,
                process_date,
                process_ts,
                read_delta_if_exists(spark, GOLD_PATHS["dim_customers"]),
            )
            _write_gold_full(df, spark, table_name)
            return

        if table_name == "dim_products":
            dim_category_df = _read_gold_required(spark, "dim_category")
            df = dim_products.build(
                products_silver,
                dim_category_df,
                process_ts,
                read_delta_if_exists(spark, GOLD_PATHS["dim_products"]),
            )
            _write_gold_full(df, spark, table_name)
            return

        if effective_mode == "incremental" and incremental_watermark is not None and table_name == "fact_order":
            orders_delta = read_silver_since(spark, "orders", incremental_watermark).where(
                col("validation_errors").isNull()
            )
            if _is_empty(orders_delta):
                logger.info("No delta rows for fact_order; skipping.")
                return

            delta_order_ids = orders_delta.select("order_id").dropDuplicates(["order_id"])
            order_items_delta_related = valid_order_items.join(
                delta_order_ids,
                on="order_id",
                how="inner",
            )
            if _is_empty(order_items_delta_related):
                logger.info("No related order_items for fact_order delta; skipping.")
                return

            dim_date_df = _read_gold_required(spark, "dim_date")
            dim_country_df = _read_gold_required(spark, "dim_country")
            dim_source_df = _read_gold_required(spark, "dim_source")
            dim_device_df = _read_gold_required(spark, "dim_device")
            dim_payment_method_df = _read_gold_required(spark, "dim_payment_method")
            dim_customers_df = _read_gold_required(spark, "dim_customers")
            dim_products_df = _read_gold_required(spark, "dim_products")

            current_customers = dim_customers_df.where(col("is_current") == True)
            current_products = dim_products_df.where(col("is_current") == True)

            df = fact_order.build(
                orders_delta,
                order_items_delta_related,
                dim_date_df,
                dim_country_df,
                dim_source_df,
                dim_device_df,
                dim_payment_method_df,
                current_customers,
                current_products,
            )
            _write_gold_fact_incremental(df, spark, table_name, ["order_id"], "order_key")
            return

        if effective_mode == "incremental" and incremental_watermark is not None and table_name == "fact_order_item":
            order_items_delta = read_silver_since(spark, "order_items", incremental_watermark).where(
                col("validation_errors").isNull()
            )
            if _is_empty(order_items_delta):
                logger.info("No delta rows for fact_order_item; skipping.")
                return

            dim_date_df = _read_gold_required(spark, "dim_date")
            dim_customers_df = _read_gold_required(spark, "dim_customers")
            dim_products_df = _read_gold_required(spark, "dim_products")

            current_customers = dim_customers_df.where(col("is_current") == True)
            current_products = dim_products_df.where(col("is_current") == True)

            df = fact_order_item.build(
                valid_orders,
                order_items_delta,
                dim_date_df,
                current_customers,
                current_products,
            )
            _write_gold_fact_incremental(
                df,
                spark,
                table_name,
                ["order_id", "product_key"],
                "order_item_key",
            )
            return

        if effective_mode == "incremental" and incremental_watermark is not None and table_name == "fact_web_events":
            events_delta = read_silver_since(spark, "events", incremental_watermark).where(
                col("validation_errors").isNull()
            )
            if _is_empty(events_delta):
                logger.info("No delta rows for fact_web_events; skipping.")
                return

            delta_session_ids = events_delta.select("session_id").dropDuplicates(["session_id"])
            sessions_related = valid_sessions.join(
                delta_session_ids,
                on="session_id",
                how="inner",
            )

            events_delta_with_customer = events_delta.join(
                sessions_related.select("session_id", "customer_id"),
                on="session_id",
                how="inner",
            )
            if _is_empty(events_delta_with_customer):
                logger.info("No related sessions for fact_web_events delta; skipping.")
                return

            dim_date_df = _read_gold_required(spark, "dim_date")
            dim_customers_df = _read_gold_required(spark, "dim_customers")
            dim_products_df = _read_gold_required(spark, "dim_products")

            current_customers = dim_customers_df.where(col("is_current") == True)
            current_products = dim_products_df.where(col("is_current") == True)

            df = fact_web_events.build(
                events_delta_with_customer,
                dim_date_df,
                current_customers,
                current_products,
            )
            _write_gold_fact_incremental(df, spark, table_name, ["event_id"], "event_key")
            return

        if effective_mode == "incremental" and incremental_watermark is not None and table_name == "fact_review":
            reviews_delta = read_silver_since(spark, "reviews", incremental_watermark).where(
                col("validation_errors").isNull()
            )
            if _is_empty(reviews_delta):
                logger.info("No delta rows for fact_review; skipping.")
                return

            dim_products_df = _read_gold_required(spark, "dim_products")
            current_products = dim_products_df.where(col("is_current") == True)

            df = fact_review.build(reviews_delta, current_products)
            _write_gold_fact_incremental(df, spark, table_name, ["review_id"], "review_key")
            return

        if effective_mode == "incremental" and incremental_watermark is not None and table_name == "fact_session":
            sessions_delta = read_silver_since(spark, "sessions", incremental_watermark).where(
                col("validation_errors").isNull()
            )
            if _is_empty(sessions_delta):
                logger.info("No delta rows for fact_session; skipping.")
                return

            dim_date_df = _read_gold_required(spark, "dim_date")
            dim_country_df = _read_gold_required(spark, "dim_country")
            dim_source_df = _read_gold_required(spark, "dim_source")
            dim_device_df = _read_gold_required(spark, "dim_device")
            dim_customers_df = _read_gold_required(spark, "dim_customers")

            current_customers = dim_customers_df.where(col("is_current") == True)

            df = fact_session.build(
                sessions_delta,
                dim_date_df,
                dim_country_df,
                dim_source_df,
                dim_device_df,
                current_customers,
            )
            _write_gold_fact_incremental(df, spark, table_name, ["session_id"], "session_key")
            return

        dim_date_df = _read_gold_required(spark, "dim_date")
        dim_country_df = _read_gold_required(spark, "dim_country")
        dim_source_df = _read_gold_required(spark, "dim_source")
        dim_device_df = _read_gold_required(spark, "dim_device")
        dim_payment_method_df = _read_gold_required(spark, "dim_payment_method")
        dim_customers_df = _read_gold_required(spark, "dim_customers")
        dim_products_df = _read_gold_required(spark, "dim_products")

        current_customers = dim_customers_df.where(col("is_current") == True)
        current_products = dim_products_df.where(col("is_current") == True)

        if table_name == "fact_order":
            df = fact_order.build(
                valid_orders,
                valid_order_items,
                dim_date_df,
                dim_country_df,
                dim_source_df,
                dim_device_df,
                dim_payment_method_df,
                current_customers,
                current_products,
            )
            _write_gold_full(df, spark, table_name)
            return

        if table_name == "fact_order_item":
            df = fact_order_item.build(
                valid_orders,
                valid_order_items,
                dim_date_df,
                current_customers,
                current_products,
            )
            _write_gold_full(df, spark, table_name)
            return

        if table_name == "fact_web_events":
            df = fact_web_events.build(
                events_with_customer,
                dim_date_df,
                current_customers,
                current_products,
            )
            _write_gold_full(df, spark, table_name)
            return

        if table_name == "fact_review":
            df = fact_review.build(valid_reviews, current_products)
            _write_gold_full(df, spark, table_name)
            return

        if table_name == "fact_session":
            df = fact_session.build(
                valid_sessions,
                dim_date_df,
                dim_country_df,
                dim_source_df,
                dim_device_df,
                current_customers,
            )
            _write_gold_full(df, spark, table_name)
            return

        if table_name == "fact_customer_funnel":
            df = fact_customer_funnel.build(
                events_with_customer,
                valid_orders,
                valid_order_items,
                dim_date_df,
                current_customers,
                current_products,
            )
            _write_gold_full(df, spark, table_name)
            return
    finally:
        spark.stop()

if __name__ == "__main__":
    process_date_arg = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    table_name_arg = sys.argv[2] if len(sys.argv) > 2 else ""
    mode_arg = sys.argv[3] if len(sys.argv) > 3 else "incremental"

    if not table_name_arg:
        raise ValueError("Usage: build_gold.py <process_date> <table_name> [mode]")

    logger.info(f"=== GOLD SINGLE TABLE BUILD: {table_name_arg} ({mode_arg}) ===")
    main(process_date_arg, table_name_arg, mode_arg)
    logger.info("=== GOLD SINGLE TABLE BUILD COMPLETED ===")

