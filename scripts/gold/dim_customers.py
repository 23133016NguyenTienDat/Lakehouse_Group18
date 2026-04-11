from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

from gold.common import attach_dimension_key, country_name_expr
from gold_utils import build_scd2_dimension


def build(
    customers_df: DataFrame,
    dim_country: DataFrame,
    process_date: str,
    process_ts: str,
    existing_df: DataFrame | None,
) -> DataFrame:
    process_year = int(process_date[:4])

    source = (
        customers_df.where(col("customer_id").isNotNull())
        .withColumn("country_name", country_name_expr("country"))
        .transform(
            lambda df: attach_dimension_key(
                df,
                "country_name",
                dim_country,
                "country_name",
                "country_key",
                "country_key",
                "customer_country",
            )
        )
        .withColumn(
            "birth_year",
            when(
                col("age").isNotNull() & (col("age") >= 0) & (col("age") <= 150),
                lit(process_year) - col("age"),
            ).otherwise(lit(None).cast("int")),
        )
        .select(
            "customer_id",
            "name",
            "email",
            "country_key",
            "birth_year",
            "signup_date",
            "marketing_opt_in",
        )
    )

    return build_scd2_dimension(
        source_df=source,
        natural_key="customer_id",
        surrogate_key="customer_key",
        attribute_cols=[
            "name",
            "email",
            "country_key",
            "birth_year",
            "signup_date",
            "marketing_opt_in",
        ],
        process_ts=process_ts,
        existing_df=existing_df,
    )

