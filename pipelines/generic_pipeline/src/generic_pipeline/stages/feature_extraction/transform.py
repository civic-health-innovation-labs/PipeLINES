import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from generic_pipeline.config import FEATURE_EXTRACTION_SUFFIX
from generic_pipeline.common_types import ColumnType, Table


def feature_extraction_transform(
    df: DataFrame, table: Table, feature_extraction_udf
) -> DataFrame:
    """Perform feature extraction on the specified table and configured
    free text column(s).

    Args:
        df (DataFrame): containing free-text columns to enrich
        table (Table): configuration of table.
        feature_extraction_udf: UDF for feature extraction

    Returns:
        DataFrame: with extracted features
    """
    # Skip tables that don't have free-text columns or any column types configured
    if table.skip_feature_extraction:
        logging.info(
            f"No free text columns for {table.name}. Skipping feature extraction."
        )
        return df

    columns = table.column_types[ColumnType.FREE_TEXT]

    for column in columns:
        logging.info(
            f"Extracting features from free-text column: {column} in table: {table.name}."
        )
        extracted_features_column = feature_extraction_udf(col(column))
        df = df.withColumn(
            "".join([column, FEATURE_EXTRACTION_SUFFIX]), extracted_features_column
        )

    return df
