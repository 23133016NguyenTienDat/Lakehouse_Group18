from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit

from gold.common import attach_dimension_key
from gold_utils import assign_surrogate_key


def build(
    orders_df: DataFrame,
    order_items_df: DataFrame,
    dim_date: DataFrame,
    current_customers: DataFrame,
    current_products: DataFrame,
) -> DataFrame:
    fact = (
        order_items_df.join(
            orders_df.select("order_id", "customer_id", "order_date"),
            on="order_id",
            how="inner",
        )
        .join(current_customers.select("customer_id", "customer_key"), on="customer_id", how="left")
        .join(
            current_products.select("product_id", "product_key", "cost_usd"),
            on="product_id",
            how="left",
        )
        .transform(
            lambda df: attach_dimension_key(
                df, "order_date", dim_date, "full_date", "date_key", "date_key", "order_item_date"
            )
        )
        .withColumn("gross_revenue_usd", col("line_total_usd"))
        .withColumn(
            "gross_margin_usd",
            col("line_total_usd") - (coalesce(col("cost_usd"), lit(0.0)) * col("quantity")),
        )
        .select(
            "order_id",
            "customer_key",
            "product_key",
            "date_key",
            "quantity",
            "unit_price_usd",
            "gross_revenue_usd",
            "gross_margin_usd",
            "product_id",
        )
    )

    return (
        assign_surrogate_key(fact, "order_item_key", ["order_id", "product_id"])
        .drop("product_id")
        .select(
            "order_item_key",
            "order_id",
            "customer_key",
            "product_key",
            "date_key",
            "quantity",
            "unit_price_usd",
            "gross_revenue_usd",
            "gross_margin_usd",
        )
    )

