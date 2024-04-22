from delta import DeltaTable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession
from generic_pipeline.common_types import DatalakeZone, Table


def construct_uri(spark: SparkSession, zone: DatalakeZone, table: str) -> str:
    """Construct Azure Datalake URI for reading/writing to specific zone and table.

    Args:
        spark (SparkSession): Spark session.
        zone (str): Datalake zone to target.
        table (str): Table name (schema prefix will be stripped automatically).

    Returns:
        str: Full URI inside storage.
    """
    datalake_uri = spark.conf.get("spark.secret.datalake-uri")
    table_without_db_schema = table.split(".")[-1]
    return f"abfss://{zone.value}@{datalake_uri}/{table_without_db_schema}"


def read_delta_table(spark: SparkSession, path: str) -> DataFrame:
    """Read Datalake Delta table within a specified zone

    Args:
        spark (SparkSession): Spark session.
        path (str): Path to file inside Azure storage.

    Returns:
        DataFrame: Loads file as a Spark DataFrame.
    """
    return spark.read.format("delta").load(path)


def read_delta_table_change_feed(
    spark: SparkSession, path: str, change_type: str = "insert"
) -> DataFrame:
    """Read change feed for delta table (essentially from the last call of the vacuum method).

    Args:
        spark (SparkSession): Spark session.
        path (str): Path to file inside Azure storage.
        change_type (str): Selector for _change_type in DF.

    Warnings:
        This method relies on having VACUUM method called after each change.

    Returns:
        DataFrame: data frame with changes.
    """
    delta_table = DeltaTable.forPath(sparkSession=spark, path=path)
    # Find the version with the last VACUUM END operation
    vacuum_end_version: DataFrame = (
        delta_table.history()
        .filter(col("operation") == "VACUUM END")
        .orderBy("version", ascending=False)
    )

    version_with_latest_vacuum: int = 0
    if vacuum_end_version.count() > 0:
        version_with_latest_vacuum = int(vacuum_end_version.first()[0])  # type: ignore

    # Latest version of Delta Table:
    end_version: int = int(
        delta_table.history().orderBy("version", ascending=False).first()[0]  # type: ignore
    )
    select_all = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", version_with_latest_vacuum)
        .option("endingVersion", end_version)
        .load(path)
        .filter(col("_change_type") == change_type)
    )
    # Remove system columns:
    all_columns = select_all.columns
    cols_no_system = all_columns[:-3]
    return select_all.select(cols_no_system)


def overwrite_delta_table(df: DataFrame, path: str) -> None:
    """Write Datalake Delta table within a specified zone (in overwrite mode).

    Args:
        df (DataFrame): DataFrame that is the subject of being serialized.
        path (str): Path to file inside Azure storage.
    """
    df.write.format("delta").mode("overwrite").save(path)


def upsert_delta_table(
    spark_session: SparkSession,
    df_inserts: DataFrame,
    df_deletes: DataFrame,
    path: str,
    table: Table,
) -> None:
    """Insert new data and removes entities in the target table that are not present in source
        based on the primary key (same logic as upserts methods for ingestion).

    Args:
        spark_session (SparkSession): Spark session.
        df_inserts (DataFrame): New data to be inserted.
        df_deletes (DataFrame): Old data to be removed.
        path (str): Path to delta table inside Azure storage.
        table (Table): Descriptor of the table.
    """
    if DeltaTable.isDeltaTable(spark_session, path):
        target_table = DeltaTable.forPath(sparkSession=spark_session, path=path)
        condition = " and ".join(
            [
                f"target.{key_column} = source.{key_column}"
                for key_column in table.columns_primary_keys
            ]
        )
        if not df_inserts.isEmpty():
            # Insert new entries
            target_table.alias("target").merge(
                df_inserts.alias("source"), condition
            ).whenNotMatchedInsertAll().execute()  # .whenNotMatchedBySourceDelete()
        if not df_deletes.isEmpty():
            # Delete old entries
            target_table.alias("target").merge(
                df_deletes.alias("source"), condition
            ).whenMatchedDelete(
                condition
            ).execute()  # .whenNotMatchedBySourceDelete()
    else:
        df_inserts.write.format("delta").mode("overwrite").save(path)


def vacuum_delta_table(spark_session: SparkSession, path: str) -> None:
    """Clean the full history of the delta table.

    Args:
        spark_session (SparkSession): Spark session
        path (str): Path to delta table inside Azure storage.
    """
    if DeltaTable.isDeltaTable(spark_session, path):
        delta_table = DeltaTable.forPath(sparkSession=spark_session, path=path)
        delta_table.vacuum()
    else:
        raise AttributeError("path does not lead to delta table")


def create_table_in_unity_catalog(
    spark_session: SparkSession,
    path: str,
    catalog_name: str,
    schema_name: str,
    table_name: str,
) -> None:
    """
    Registers table as an External table in Unity Catalog, if it doesn't yet exist.

    Args:
        spark_session (SparkSession): Spark session.
        path (str): Path to a previously saved table in Data lake.
        catalog_name (str): Name of the catalogue.
        schema_name (str): Schema name.
        table_name (str): Table names.
    """
    # cut `dbo.` from the table_name
    _tbl = table_name[4:]
    spark_session.sql(
        f'CREATE TABLE IF NOT EXISTS `{catalog_name}`.`{schema_name}`.{_tbl} LOCATION "{path}"'
    )
