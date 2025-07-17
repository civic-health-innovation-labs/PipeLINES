import logging
from datetime import datetime
from pyspark.sql.session import SparkSession
from rio_pipeline.datalake import DatalakeZone, construct_uri
from rio_pipeline.db import rio_config
from rio_pipeline.stages.ingestion import (
    load_sql_table,
    validate_sql_source_against_schema,
)
from rio_pipeline.common_types import IngestionType
from rio_pipeline.table_classification import TABLE_CONFIG
from rio_pipeline.config import SKIP_BRONZE


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

    row_count: list[tuple[str, str, int]] = []

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

        row_count.append(
            (
                table.name,
                str(datetime.now().strftime("%Y-%m-%dT%H:%M")),
                updated_table.count(),
            )
        )

        # Update table
        table.upsert_method(
            spark_session,
            updated_table,
            uri_constructor(spark_session, DatalakeZone.BRONZE, table.name),
            table,
        )

    # Writing logs into Parquet file
    df_count = spark_session.createDataFrame(
        row_count, ["Row_Count", "Date_Time", "Value"]
    )
    logs_directory = uri_constructor(
        spark_session, DatalakeZone.MONITORING, "UpsertLogs"
    )
    df_count.write.format("parquet").mode("append").save(logs_directory)


if __name__ == "__main__":
    if not SKIP_BRONZE:
        _spark = SparkSession.builder.getOrCreate()
        _config = rio_config(spark=_spark)

        run_ingestion(_spark, _config)
