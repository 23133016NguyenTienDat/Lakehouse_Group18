#!/usr/bin/env python3
"""Gold Layer orchestrator: build dimensional and fact tables from Silver Delta tables."""

import sys
from datetime import datetime

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
    create_spark_session,
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


def _write_gold_full(df, spark, table_name: str):
    write_gold_overwrite(df, spark, table_name, GOLD_PATHS[table_name])


def _write_gold_fact_incremental(df, spark, table_name: str, key_cols: list[str], surrogate_col: str):
    write_gold_merge(
        df=df,
        spark=spark,
        table_name=table_name,
        path=GOLD_PATHS[table_name],
        key_cols=key_cols,
        surrogate_col=surrogate_col,
    )


def main(process_date: str, mode: str = "full"):
    if mode not in {"full", "incremental"}:
        raise ValueError("Mode must be one of: full, incremental")

    logger.info("=== GOLD: STAR SCHEMA BUILD ===")
    logger.info(f"GOLD run mode: {mode}")
    spark = create_spark_session("Gold_Star_Schema")
    process_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    try:
        incremental_watermark = None
        if mode == "incremental":
            latest_silver_process_date = get_latest_silver_process_date(spark, SILVER_DATASETS)
            last_gold_watermark = get_gold_watermark(spark, PIPELINE_NAME)

            if latest_silver_process_date is None:
                logger.info("No Silver process watermark found; skipping Gold run.")
                return

            if last_gold_watermark is None:
                logger.info("Gold watermark not found; running initial full load.")
                mode = "full"
            elif latest_silver_process_date <= last_gold_watermark:
                logger.info(
                    "No new Silver watermark detected "
                    f"(latest={latest_silver_process_date}, gold={last_gold_watermark}); skipping."
                )
                return
            else:
                incremental_watermark = last_gold_watermark

        # Full context is always needed for dimensions/SCD2 and fact joins.
        customers_silver = read_silver_base(spark, "customers")
        orders_silver = read_silver_base(spark, "orders")
        order_items_silver = read_silver_base(spark, "order_items")
        products_silver = read_silver_base(spark, "products")
        reviews_silver = read_silver_base(spark, "reviews")
        sessions_silver = read_silver_base(spark, "sessions")
        events_silver = read_silver_base(spark, "events")

        valid_orders = orders_silver.where(col("validation_errors").isNull()).cache()
        valid_order_items = order_items_silver.where(col("validation_errors").isNull()).cache()
        valid_reviews = reviews_silver.where(col("validation_errors").isNull()).cache()
        valid_sessions = sessions_silver.where(col("validation_errors").isNull()).cache()
        valid_events = events_silver.where(col("validation_errors").isNull()).cache()
        events_with_customer = valid_events.join(
            valid_sessions.select("session_id", "customer_id"),
            on="session_id",
            how="inner",
        ).cache()

        dim_country_df = dim_country.build(
            customers_silver,
            orders_silver,
            sessions_silver,
            read_delta_if_exists(spark, GOLD_PATHS["dim_country"]),
        )
        _write_gold_full(dim_country_df, spark, "dim_country")

        dim_source_df = dim_source.build(
            orders_silver,
            sessions_silver,
            read_delta_if_exists(spark, GOLD_PATHS["dim_source"]),
        )
        _write_gold_full(dim_source_df, spark, "dim_source")

        dim_device_df = dim_device.build(
            orders_silver,
            sessions_silver,
            read_delta_if_exists(spark, GOLD_PATHS["dim_device"]),
        )
        _write_gold_full(dim_device_df, spark, "dim_device")

        dim_payment_method_df = dim_payment_method.build(
            orders_silver,
            events_silver,
            read_delta_if_exists(spark, GOLD_PATHS["dim_payment_method"]),
        )
        _write_gold_full(dim_payment_method_df, spark, "dim_payment_method")

        dim_category_df = dim_category.build(
            products_silver,
            read_delta_if_exists(spark, GOLD_PATHS["dim_category"]),
        )
        _write_gold_full(dim_category_df, spark, "dim_category")

        dim_date_df = dim_date.build(
            spark,
            customers_silver,
            orders_silver,
            reviews_silver,
            sessions_silver,
            events_silver,
        )
        _write_gold_full(dim_date_df, spark, "dim_date")

        dim_customers_df = dim_customers.build(
            customers_silver,
            dim_country_df,
            process_date,
            process_ts,
            read_delta_if_exists(spark, GOLD_PATHS["dim_customers"]),
        )
        _write_gold_full(dim_customers_df, spark, "dim_customers")
        current_customers = dim_customers_df.where(col("is_current") == True).cache()

        dim_products_df = dim_products.build(
            products_silver,
            dim_category_df,
            process_ts,
            read_delta_if_exists(spark, GOLD_PATHS["dim_products"]),
        )
        _write_gold_full(dim_products_df, spark, "dim_products")
        current_products = dim_products_df.where(col("is_current") == True).cache()

        if mode == "incremental" and incremental_watermark is not None:
            orders_delta = read_silver_since(spark, "orders", incremental_watermark).where(
                col("validation_errors").isNull()
            )
            order_items_delta = read_silver_since(spark, "order_items", incremental_watermark).where(
                col("validation_errors").isNull()
            )
            events_delta = read_silver_since(spark, "events", incremental_watermark).where(
                col("validation_errors").isNull()
            )
            sessions_delta = read_silver_since(spark, "sessions", incremental_watermark).where(
                col("validation_errors").isNull()
            )
            reviews_delta = read_silver_since(spark, "reviews", incremental_watermark).where(
                col("validation_errors").isNull()
            )

            # Narrow down related context for incremental fact builds.
            delta_order_ids = orders_delta.select("order_id").dropDuplicates(["order_id"])
            order_items_delta_related = valid_order_items.join(
                delta_order_ids,
                on="order_id",
                how="inner",
            )

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

            fact_order_df = fact_order.build(
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
            _write_gold_fact_incremental(
                fact_order_df, spark, "fact_order", ["order_id"], "order_key"
            )

            fact_order_item_df = fact_order_item.build(
                valid_orders,
                order_items_delta,
                dim_date_df,
                current_customers,
                current_products,
            )
            _write_gold_fact_incremental(
                fact_order_item_df,
                spark,
                "fact_order_item",
                ["order_id", "product_key"],
                "order_item_key",
            )

            fact_web_events_df = fact_web_events.build(
                events_delta_with_customer,
                dim_date_df,
                current_customers,
                current_products,
            )
            _write_gold_fact_incremental(
                fact_web_events_df,
                spark,
                "fact_web_events",
                ["event_id"],
                "event_key",
            )

            fact_review_df = fact_review.build(reviews_delta, current_products)
            _write_gold_fact_incremental(
                fact_review_df,
                spark,
                "fact_review",
                ["review_id"],
                "review_key",
            )

            fact_session_df = fact_session.build(
                sessions_delta,
                dim_date_df,
                dim_country_df,
                dim_source_df,
                dim_device_df,
                current_customers,
            )
            _write_gold_fact_incremental(
                fact_session_df,
                spark,
                "fact_session",
                ["session_id"],
                "session_key",
            )

            # Funnel is highly stateful/time-based; keep full rebuild for correctness.
            fact_customer_funnel_df = fact_customer_funnel.build(
                events_with_customer,
                valid_orders,
                valid_order_items,
                dim_date_df,
                current_customers,
                current_products,
            )
            _write_gold_full(fact_customer_funnel_df, spark, "fact_customer_funnel")
        else:
            fact_order_df = fact_order.build(
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
            _write_gold_full(fact_order_df, spark, "fact_order")

            fact_order_item_df = fact_order_item.build(
                valid_orders,
                valid_order_items,
                dim_date_df,
                current_customers,
                current_products,
            )
            _write_gold_full(fact_order_item_df, spark, "fact_order_item")

            fact_web_events_df = fact_web_events.build(
                events_with_customer,
                dim_date_df,
                current_customers,
                current_products,
            )
            _write_gold_full(fact_web_events_df, spark, "fact_web_events")

            fact_review_df = fact_review.build(valid_reviews, current_products)
            _write_gold_full(fact_review_df, spark, "fact_review")

            fact_session_df = fact_session.build(
                valid_sessions,
                dim_date_df,
                dim_country_df,
                dim_source_df,
                dim_device_df,
                current_customers,
            )
            _write_gold_full(fact_session_df, spark, "fact_session")

            fact_customer_funnel_df = fact_customer_funnel.build(
                events_with_customer,
                valid_orders,
                valid_order_items,
                dim_date_df,
                current_customers,
                current_products,
            )
            _write_gold_full(fact_customer_funnel_df, spark, "fact_customer_funnel")

        latest_processed = get_latest_silver_process_date(spark, SILVER_DATASETS)
        if latest_processed is not None:
            upsert_gold_watermark(spark, PIPELINE_NAME, latest_processed)
            logger.info(f"Gold watermark updated to {latest_processed}")

        logger.info("=== GOLD BUILD COMPLETED ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_date_arg = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    mode_arg = sys.argv[2] if len(sys.argv) > 2 else "incremental"
    main(process_date_arg, mode_arg)

