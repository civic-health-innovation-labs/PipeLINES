import logging

from generic_pipeline.common_types import IngestionType
from pyspark.sql.session import SparkSession
from generic_pipeline.config import (
    WORKER_COUNT,
    CORE_COUNT,
    DATAFRAME_PARTITIONING,
    SKIP_SILVER,
)
from generic_pipeline.table_classification import TABLE_CONFIG
from generic_pipeline.datalake import (
    DatalakeZone,
    construct_uri,
    read_delta_table_change_feed,
    upsert_delta_table,
)
from generic_pipeline.stages.pseudonymisation.presidio import (
    broadcast_presidio_with_anonymise_udf,
)
from generic_pipeline.stages.pseudonymisation.transform import (
    pseudo_transform,
    fake_pseudo_transform,
)


def run_pseudonymisation(
    spark_session,
    anonymise_udf,
    table_config=TABLE_CONFIG,
    uri_constructor=construct_uri,
) -> None:
    """Run the pseudonymisation logic.
    Args:
        spark_session: Spark Session instance.
        anonymise_udf: UDF for anynimisation.
        table_config: List of table configuration.
        uri_constructor: Method for constructing URLs.
    """
    for table in table_config:
        logging.info(f"Processing table for pseudonymisation: {table.name}")

        if table.update_logic == IngestionType.NOT_IMPORT:
            logging.info(
                f"Skipping pseudonymisation for table {table.name} as it is not imported"
            )
            continue

        # Read from bronze (just new inserts)
        source_path = uri_constructor(spark_session, DatalakeZone.BRONZE, table.name)
        df = read_delta_table_change_feed(spark_session, source_path)
        # Fake pseudo is called to have the same structure (needed by upsert method)
        df_deletes = fake_pseudo_transform(
            read_delta_table_change_feed(spark_session, source_path, "delete"),
            table,
            anonymise_udf,
        )

        if df.isEmpty():
            logging.warning(
                f"Skipping pseudonymisation for table {table.name} as DF is empty"
            )
        else:
            if DATAFRAME_PARTITIONING:
                # Increase partition count for parallel processing
                initial_partitions = df.rdd.getNumPartitions()
                df = df.repartition(max(WORKER_COUNT * CORE_COUNT, initial_partitions))

            # Pseudonymise
            df = pseudo_transform(df, table, anonymise_udf)

        # Write to silver
        upsert_delta_table(
            spark_session=spark_session,
            df_inserts=df,
            df_deletes=df_deletes,
            path=uri_constructor(spark_session, DatalakeZone.SILVER, table.name),
            table=table,
        )

        logging.info(
            f"Wrote pseudonymisation outputs to {table.name} in Datalake silver zone"
            f" ({df.count()} rows)."
        )


if __name__ == "__main__":
    if not SKIP_SILVER:
        spark_session = SparkSession.builder.getOrCreate()
        anonymise_udf = broadcast_presidio_with_anonymise_udf(spark_session)

        run_pseudonymisation(spark_session, anonymise_udf)
