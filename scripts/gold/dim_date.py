from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    dayofweek,
    explode,
    expr,
    lit,
    max as spark_max,
    min as spark_min,
    month,
    quarter,
    sequence,
    year,
)


def build(
    spark,
    customers_df: DataFrame,
    orders_df: DataFrame,
    reviews_df: DataFrame,
    sessions_df: DataFrame,
    events_df: DataFrame,
) -> DataFrame:
    all_dates = (
        customers_df.select(col("signup_date").alias("full_date"))
        .unionByName(orders_df.select(col("order_date").alias("full_date")))
        .unionByName(reviews_df.select(col("review_date").alias("full_date")))
        .unionByName(sessions_df.select(col("session_date").alias("full_date")))
        .unionByName(events_df.select(col("event_date").alias("full_date")))
        .where(col("full_date").isNotNull())
    )

    bounds = all_dates.agg(
        spark_min("full_date").alias("min_date"),
        spark_max("full_date").alias("max_date"),
    ).first()

    if bounds["min_date"] is None or bounds["max_date"] is None:
        raise ValueError("Could not derive a date range for DIM_DATE.")

    date_span = spark.range(1).select(
        explode(
            sequence(
                lit(bounds["min_date"]),
                lit(bounds["max_date"]),
                expr("interval 1 day"),
            )
        ).alias("full_date")
    )

    return (
        date_span.withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
        .withColumn("day", dayofmonth(col("full_date")))
        .withColumn("month", month(col("full_date")))
        .withColumn("quarter", quarter(col("full_date")))
        .withColumn("year", year(col("full_date")))
        .withColumn("is_weekend", dayofweek(col("full_date")).isin(1, 7))
        .select("date_key", "full_date", "day", "month", "quarter", "year", "is_weekend")
    )

