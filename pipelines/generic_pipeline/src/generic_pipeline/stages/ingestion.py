from typing import TYPE_CHECKING, Optional
from collections import defaultdict

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import md5, concat_ws

from generic_pipeline.db import DatabaseConfiguration
from generic_pipeline.config import ROW_HASH_COLUMN_NAME
from generic_pipeline.common_exceptions import SchemaValidationException

if TYPE_CHECKING:
    from generic_pipeline.common_types import Table


def _get_tables_and_columns_from_sql(
    spark: SparkSession,
    config: DatabaseConfiguration,
    _filter_tables: str | None = "TABLE_SCHEMA = 'dbo'",
) -> dict[str, set[str]]:
    """Get the list of tables and columns.

    Args:
        spark (SparkSession): PySpark session.
        config (DatabaseConfiguration): Definition of DB secrets.
        _filter_tables (str): Internal filtration of tables, default is based on dbo schema.

    Returns:
        dict[str, set[str]]: List of tables as key (with full name SCHEMA.TABLE) and column names
            as value.
    """
    reader = (
        spark.read.format("jdbc")
        .option("url", config.url)
        .option("dbtable", "information_schema.columns")
        .option("user", config.user)
        .option("password", config.password)
    )

    if config.driver is not None:
        reader = reader.option("driver", config.driver)

    df_list_tables: DataFrame = (
        reader.load().filter(_filter_tables) if _filter_tables else reader.load()
    )
    sql_tables: dict[str, set[str]] = defaultdict(set)
    col_list_tables = df_list_tables.collect()
    for source_table in col_list_tables:
        if str(source_table["TABLE_NAME"]).startswith("#"):
            # Skips the system tables
            continue
        # Maps tables to "SCHEMA_NAME.TABLE_NAME" and append into the list
        sql_tables[
            (source_table["TABLE_SCHEMA"] + "." + source_table["TABLE_NAME"])
        ].add(source_table["COLUMN_NAME"])

    return sql_tables


def validate_sql_source_against_schema(
    spark: SparkSession, config: DatabaseConfiguration, tables: tuple["Table", ...]
) -> None:
    """Validate schema of tables available in SQL server against catalogue.
    Args:
        spark (SparkSession): PySpark session.
        config (DatabaseConfiguration): Definition of DB secrets.
        tables (tuple[Table, ...]): Tuple of all classified tables.
    Raises:
        LookupError: When schema fails.
    """
    classified_tables: set[str] = set([_table.name for _table in tables])
    sql_tables: dict[str, set[str]] = _get_tables_and_columns_from_sql(spark, config)
    # Get set of tables that are not classified:
    unclassified_tables: set[str] = set(sql_tables) - classified_tables
    # Get set of tables that are classified, but does not exist any more
    classified_but_unexisting_tables: set[str] = classified_tables - set(sql_tables)

    # Auxiliary variable
    _classified_and_existing_tables: set[str] = classified_tables.intersection(
        set(sql_tables)
    )

    # Goes through all classified tables (available in SQL server) and check the column space
    error_on_column_space: bool = False
    analysed_tables: dict[str, dict[str, list[str]]] = {}
    for _table in tables:
        if _table.name not in _classified_and_existing_tables:
            continue
        _source_columns: set[str] = sql_tables[_table.name]
        _target_columns: set[str] = set(_table.analysed_columns)

        _cols_unclassified: list[str] = list(_source_columns - _target_columns)
        _cols_classified_but_unexisting: list[str] = list(
            _target_columns - _source_columns
        )

        if _cols_unclassified or _cols_classified_but_unexisting:
            analysed_tables[_table.name] = {
                "unclassified_columns": _cols_unclassified,
                "classified_but_unexisting_columns": _cols_classified_but_unexisting,
            }
            error_on_column_space = True

    if classified_but_unexisting_tables or unclassified_tables or error_on_column_space:
        raise SchemaValidationException(
            {
                "unclassified_tables": list(unclassified_tables),
                "classified_but_unexisting_tables": list(
                    classified_but_unexisting_tables
                ),
                "analysed_tables": analysed_tables,
            }
        )


def load_sql_table(
    spark: SparkSession, config: DatabaseConfiguration, table_or_query: str
) -> DataFrame:
    """Load a SQL table into the PySpark DataFrame.

    Args:
        spark (SparkSession): PySpark session.
        config (DatabaseConfiguration): Definition of DB secrets.
        table_or_query (str): Name of table that is exported.

    Returns:
        DataFrame: Dataframe with SQL table.
    """
    reader = (
        spark.read.format("jdbc")
        .option("url", config.url)
        .option("dbtable", table_or_query)
        .option("user", config.user)
        .option("password", config.password)
    )

    if config.driver is not None:
        reader = reader.option("driver", config.driver)

    df: DataFrame = reader.load()
    return df


def upsert_append_only_table(
    spark: SparkSession, source: DataFrame, path: str, table: "Table"
) -> None:
    """Upsert to Datalake Delta table within a specified zone

    Only works on append-only tables as it does not perform any updates.

    Args:
        spark (SparkSession): PySpark session.
        source (DataFrame): Source DataFrame - usually loaded from the SQL Database.
        path (str): Path inside Bronze Layer (into equivalent dataset).
        table (Table): Definition of imported table.
    """
    # Drop duplicities on the key column (as incoming data are often defective).
    source = source.dropDuplicates(table.columns_primary_keys)  # type: ignore
    if DeltaTable.isDeltaTable(spark, path):
        target_table = DeltaTable.forPath(sparkSession=spark, path=path)

        condition = " and ".join(
            [
                f"target.{key_column} = source.{key_column}"
                for key_column in table.columns_primary_keys
            ]
        )

        target_table.alias("target").merge(
            source.alias("source"), condition
        ).whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute()
    else:
        source.write.format("delta").save(path)


def upsert_full_update_table(
    spark: SparkSession, source: DataFrame, path: str, table: "Table"
) -> None:
    """Full update of Datalake Delta table within a specified zone.

    Adds a new column that contains a hash of values (to determine whether to
    update or not).

    Args:
        spark (SparkSession): PySpark session.
        source (DataFrame): Source DataFrame - usually loaded from the SQL Database.
        path (str): Path inside Bronze Layer (into equivalent dataset).
        table (Table): Definition of imported table.
    """
    # Compute hash value and add it as a column
    hashed_source = source.withColumn(
        ROW_HASH_COLUMN_NAME, md5(concat_ws("|", *source.columns))
    )
    return upsert_append_only_table(spark, hashed_source, path, table)


def upsert_drop_and_recreate_table(
    spark: SparkSession, source: DataFrame, path: str, table: Optional["Table"]
) -> None:
    """Rewrites existing table with the new data (drop and recreate logic of update)

    Args:
        spark (SparkSession): PySpark session.
        source (DataFrame): Source DataFrame - usually loaded from the SQL Database.
        path (str): Path inside Bronze Layer (into equivalent dataset).
        table (Table): Unused, to unify interface
    """
    if DeltaTable.isDeltaTable(spark, path):
        source.write.mode("overwrite").format("delta").save(path)
    else:
        source.write.format("delta").save(path)
