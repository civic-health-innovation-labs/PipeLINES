import logging

from rio_pipeline.common_types import IngestionType
from pyspark.sql.session import SparkSession
from rio_pipeline.config import (
    RUN_FEATURE_EXTRACTION,
    WORKER_COUNT,
    CORE_COUNT,
    DATAFRAME_PARTITIONING,
    WRITE_INTO_UNITY_CATALOGUE,
    SKIP_GOLD,
)
from rio_pipeline.table_classification import TABLE_CONFIG
from rio_pipeline.datalake import (
    DatalakeZone,
    construct_uri,
    read_delta_table_change_feed,
    upsert_delta_table,
    create_table_in_unity_catalog,
)
from rio_pipeline.stages.feature_extraction.scispacybr import (
    broadcast_scispacy_with_udf,
)
from rio_pipeline.stages.feature_extraction.transform import (
    feature_extraction_transform,
)


def run_feature_extraction(
    spark_session,
    feature_extraction_udf,
    table_config=TABLE_CONFIG,
    uri_constructor=construct_uri,
    unity_catalogue_creator=create_table_in_unity_catalog,
) -> None:
    """Run the feature extraction logic.
    Args:
        spark_session: Spark Session instance.
        feature_extraction_udf: UDF for feature extraction.
        table_config: List of table configuration.
        uri_constructor: Method for constructing URLs.
        unity_catalogue_creator: Function for creation of unity catalogue entry.
    """
    for table in table_config:
        logging.info(f"Processing table for feature extraction: {table.name}")

        if table.update_logic == IngestionType.NOT_IMPORT:
            logging.info(
                f"Skipping feature extraction for table {table.name} as it is not imported"
            )
            continue

        # Read from silver
        source_path = uri_constructor(spark_session, DatalakeZone.SILVER, table.name)
        df = read_delta_table_change_feed(spark_session, source_path)
        # TODO: add in the future an equivalent of the fake call (to keep structure)
        df_deletes = read_delta_table_change_feed(spark_session, source_path, "delete")

        if df.isEmpty():
            logging.warning(
                f"Skipping feature extraction for table {table.name} as DF is empty"
            )
        elif RUN_FEATURE_EXTRACTION:
            if DATAFRAME_PARTITIONING:
                # Increase partition count for parallel processing
                initial_partitions = df.rdd.getNumPartitions()
                df = df.repartition(max(WORKER_COUNT * CORE_COUNT, initial_partitions))

            # Feature extraction
            df = feature_extraction_transform(df, table, feature_extraction_udf)

        # Write to gold
        gold_uri = uri_constructor(spark_session, DatalakeZone.GOLD, table.name)
        upsert_delta_table(
            spark_session=spark_session,
            df_inserts=df,
            df_deletes=df_deletes,
            path=gold_uri,
            table=table,
        )

        if WRITE_INTO_UNITY_CATALOGUE:
            # Write to Unity Catalogue
            unity_catalogue_creator(
                spark_session,
                gold_uri,
                spark_session.conf.get("spark.secret.unity-catalog-catalog-name"),
                spark_session.conf.get("spark.secret.unity-catalog-schema-name"),
                table.name,
            )

        logging.info(
            f"Wrote feature extraction outputs to {table.name} in Datalake gold zone"
            f" ({df.count()} rows)."
        )


if __name__ == "__main__":
    if not SKIP_GOLD:
        _spark_session = SparkSession.builder.getOrCreate()
        # UDF for feature extraction (if it is about to run)
        _feature_extraction_udf = None
        if RUN_FEATURE_EXTRACTION:
            _feature_extraction_udf = broadcast_scispacy_with_udf(_spark_session)

        run_feature_extraction(_spark_session, _feature_extraction_udf)
