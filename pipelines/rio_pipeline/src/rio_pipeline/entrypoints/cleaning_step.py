import logging

from rio_pipeline.common_types import IngestionType
from pyspark.sql.session import SparkSession
from rio_pipeline.table_classification import TABLE_CONFIG
from rio_pipeline.datalake import (
    DatalakeZone,
    construct_uri,
    vacuum_delta_table,
)


def run_cleaning_step(
    spark_session,
    table_config=TABLE_CONFIG,
    uri_constructor=construct_uri,
) -> None:
    """Clean the pipeline (mainly call the vacuum method).

    Args:
        spark_session: Spark Session instance.
        table_config: List of table configuration.
        uri_constructor: Method for constructing URLs.
    """
    for table in table_config:
        logging.info(f"Processing table for cleaning: {table.name}")

        if table.update_logic == IngestionType.NOT_IMPORT:
            logging.info(
                f"Skipping cleaning for table {table.name} as it is not imported"
            )
            continue

        # Construct paths for delta table to be cleaned
        bronze_path = uri_constructor(spark_session, DatalakeZone.BRONZE, table.name)
        silver_path = uri_constructor(spark_session, DatalakeZone.SILVER, table.name)
        gold_path = uri_constructor(spark_session, DatalakeZone.GOLD, table.name)

        # Clean each delta table by calling VACUUM operation
        vacuum_delta_table(spark_session, bronze_path)
        vacuum_delta_table(spark_session, silver_path)
        vacuum_delta_table(spark_session, gold_path)

        logging.info(f"Cleaning of the table {table.name} has finished")


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()

    run_cleaning_step(spark_session)
