from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, coalesce, col, create_map, lit


COUNTRY_CODE_TO_NAME = {
    "AE": "United Arab Emirates",
    "AU": "Australia",
    "BR": "Brazil",
    "CA": "Canada",
    "DE": "Germany",
    "ES": "Spain",
    "FR": "France",
    "GB": "United Kingdom",
    "IN": "India",
    "JP": "Japan",
    "MX": "Mexico",
    "NL": "Netherlands",
    "PL": "Poland",
    "SE": "Sweden",
    "SG": "Singapore",
    "US": "United States",
    "ZA": "South Africa",
}


def country_name_expr(source_col: str):
    mapping_entries = []
    for country_code, country_name in COUNTRY_CODE_TO_NAME.items():
        mapping_entries.extend([lit(country_code), lit(country_name)])
    country_map = create_map(*mapping_entries)
    return coalesce(country_map.getItem(col(source_col)), col(source_col))


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

