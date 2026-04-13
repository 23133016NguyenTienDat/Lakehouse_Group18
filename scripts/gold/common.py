from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col


def attach_dimension_key(
    df: DataFrame,
    source_col: str,
    dimension_df: DataFrame,
    dimension_value_col: str,
    dimension_key_col: str,
    output_key_col: str,
    join_alias: str,
) -> DataFrame:
    lookup_value_col = f"__{join_alias}_{dimension_value_col}"
    lookup_df = broadcast(
        dimension_df.select(
            col(dimension_value_col).alias(lookup_value_col),
            col(dimension_key_col).alias(output_key_col),
        )
    )
    return (
        df.join(lookup_df, col(source_col) == col(lookup_value_col), "left")
        .drop(lookup_value_col)
    )

