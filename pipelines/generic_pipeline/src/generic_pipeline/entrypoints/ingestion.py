import logging
from pyspark.sql.session import SparkSession
from generic_pipeline.datalake import DatalakeZone, construct_uri
from generic_pipeline.db import generic_pipeline_config
from generic_pipeline.stages.ingestion import (
    load_sql_table,
    validate_sql_source_against_schema,
)
from generic_pipeline.common_types import IngestionType
from generic_pipeline.table_classification import TABLE_CONFIG
from generic_pipeline.config import SKIP_BRONZE


def run_ingestion(
    spark_session, config, table_config=TABLE_CONFIG, uri_constructor=construct_uri
) -> None:
    """Run the main ingestion logic.
    Args:
        spark_session: Spark Session instance.
        config: Database configuration.
        table_config: List of table configuration.
        uri_constructor: Method for constructing URLs.
    """
    # Validate schema
    validate_sql_source_against_schema(spark_session, config, table_config)

    for table in table_config:
        if table.update_logic == IngestionType.NOT_IMPORT or table._skip_import:
            logging.info(
                f"Skipping ingestion for table {table.name} as it is not imported"
            )
            continue

        # Select the columns & table that is a subject of import
        columns = ", ".join(
            '"' + col + '"'
            for col in table.analysed_columns
            if col not in table._skip_columns
        )
        query = f"(SELECT {columns} FROM {table.name}) tbl"

        # Load table
        updated_table = load_sql_table(spark_session, config, query)

        # Update table
        table.upsert_method(
            spark_session,
            updated_table,
            uri_constructor(spark_session, DatalakeZone.BRONZE, table.name),
            table,
        )


if __name__ == "__main__":
    if not SKIP_BRONZE:
        _spark = SparkSession.builder.getOrCreate()
        _config = generic_pipeline_config(spark=_spark)

        run_ingestion(_spark, _config)
