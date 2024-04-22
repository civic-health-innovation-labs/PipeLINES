import os
import copy
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark_test import assert_pyspark_df_equal  # type: ignore
from generic_pipeline.stages.ingestion import (
    load_sql_table,
    upsert_append_only_table,
    upsert_full_update_table,
    upsert_drop_and_recreate_table,
    validate_sql_source_against_schema,
)
from generic_pipeline.tests.helpers import mssql_configuration
from sqlalchemy import create_engine, text
from testcontainers.mssql import SqlServerContainer  # type: ignore
from generic_pipeline.table_classification import TABLE_CONFIG
from generic_pipeline.config import ROW_HASH_COLUMN_NAME
from generic_pipeline.common_types import Table, IngestionType, ColumnType
from generic_pipeline.common_exceptions import SchemaValidationException
from generic_pipeline.entrypoints.ingestion import run_ingestion


# Common configuration for full update logic test
_TABLE_FU = Table(
    name="test_table",
    column_types={},
    analysed_columns=tuple(),
    update_logic=IngestionType.FULL_UPDATE,
    table_primary_keys=("key",),
)
# Common configuration for append only logic test
_TABLE_AO = Table(
    name="test_table",
    column_types={},
    analysed_columns=tuple(),
    update_logic=IngestionType.APPEND_AND_REMOVE_ONLY,
    table_primary_keys=("key",),
)


# === CONCRETE UNIT-TESTS ===
def test_validate_sql_source_against_schema(
    spark_session: SparkSession, mssql_container: SqlServerContainer
):
    # Prepare concrete table in SQL server for testing purposes
    engine = create_engine(mssql_container.get_connection_url())
    with engine.connect() as conn:
        conn.execute(
            text(
                "CREATE TABLE RandomAppointment ("
                "   [SequenceID] int NOT NULL, "
                "   [Location] varchar(15) NOT NULL, "
                "   [Comment] nvarchar(max), "
                "   [SuperUnclassified] int NOT NULL"
                ");"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE SomeNonsenseToBeUnclassified ("
                "   [SequenceID] int NOT NULL"
                ");"
            )
        )
        conn.commit()

    try:
        validate_sql_source_against_schema(
            spark=spark_session,
            config=mssql_configuration(mssql_container),
            tables=TABLE_CONFIG,
        )
    except SchemaValidationException as raised_exception:
        # Analyses raised exception
        analysed_exception: dict = raised_exception.args[0]
        assert (
            len(analysed_exception["analysed_tables"]) == 1
        )  # only dbo.RandomAppointment exist
        assert (
            len(analysed_exception["classified_but_unexisting_tables"])
            == len(TABLE_CONFIG) - 1
        )
        assert (
            len(
                set(analysed_exception["classified_but_unexisting_tables"])
                - set([_tc.name for _tc in TABLE_CONFIG])
            )
            == 0
        )
        assert (
            "dbo.SomeNonsenseToBeUnclassified"
            in analysed_exception["unclassified_tables"]
        )
        # Analyse column space
        assert (
            "SuperUnclassified"
            in analysed_exception["analysed_tables"]["dbo.RandomAppointment"][
                "unclassified_columns"
            ]
        )
        assert len(
            analysed_exception["analysed_tables"]["dbo.RandomAppointment"][
                "classified_but_unexisting_columns"
            ]
        ) == (
            len(
                # Find analysed columns of dbo.RandomAppointment table
                [
                    tc.analysed_columns
                    for tc in TABLE_CONFIG
                    if tc.name == "dbo.RandomAppointment"
                ][0]
            )
            - 3
        )

    # Cleaning work in the testing SQL instance (for other tests)
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE RandomAppointment;"))
        conn.execute(text("DROP TABLE SomeNonsenseToBeUnclassified;"))
        conn.commit()


def test_md5_hashing(spark_session: SparkSession, delta_dir: str):
    df = spark_session.createDataFrame(
        [(1, 5, "a"), (2, 6, "b"), (3, 7, None)], ["key", "value_int", "value_str"]
    )

    upsert_full_update_table(spark_session, df, delta_dir, _TABLE_FU)

    df_hashed = spark_session.read.format("delta").load(delta_dir)

    df_comparison = spark_session.createDataFrame(
        [
            (1, 5, "a", "9f12338fb5d8f48a5957916e78cb4f3b"),
            (2, 6, "b", "442bd90b2959404f49608b9f1a648bf0"),
            (3, 7, None, "75cf219fc872342266cce9607f6d21ee"),
        ],
        ["key", "value_int", "value_str", ROW_HASH_COLUMN_NAME],
    )
    assert_pyspark_df_equal(
        df_hashed,
        df_comparison,
        order_by="key",
    )


def test_load_sql_table(
    spark_session: SparkSession, mssql_container: SqlServerContainer
) -> None:
    engine = create_engine(mssql_container.get_connection_url())
    with engine.connect() as conn:
        conn.execute(
            text(
                "CREATE TABLE PatientNote ([SequenceID] int NOT NULL, [ClientID]"
                " varchar(15) NOT NULL, [DateAndTime] datetime NOT NULL, [NoteText]"
                " nvarchar(max));"
            )
        )
        conn.execute(
            text(
                "INSERT INTO PatientNote VALUES (1,'123','2016-05-03 15:20:00','The"
                " patient is feeling very well')"
            )
        )
        conn.commit()

    config = mssql_configuration(mssql_container)

    df = load_sql_table(spark_session, config, "PatientNote")

    assert_pyspark_df_equal(
        df,
        spark_session.createDataFrame(
            [
                (
                    1,
                    "123",
                    datetime(2016, 5, 3, 15, 20),
                    "The patient is feeling very well",
                )
            ],
            StructType(
                [
                    StructField("SequenceID", IntegerType()),
                    StructField("ClientID", StringType()),
                    StructField("DateAndTime", TimestampType()),
                    StructField("NoteText", StringType()),
                ]
            ),
        ),
    )

    # Cleaning work in the testing SQL instance (for other tests)
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE PatientNote;"))
        conn.commit()


def test_run_ingestion(
    spark_session: SparkSession, mssql_container: SqlServerContainer, bronze_dir
) -> None:
    # Prepare SQL database to ingestion
    engine = create_engine(mssql_container.get_connection_url())
    with engine.connect() as conn:
        # Create a new table
        conn.execute(
            text(
                "CREATE TABLE TestTable("
                "[SequenceID] int NOT NULL, "
                "[ClientID] varchar(15) NOT NULL, "
                "[DateAndTime] datetime NOT NULL, "
                "[NoteText] nvarchar(max)"
                ");"
            )
        )
        conn.execute(
            text(
                "INSERT INTO TestTable VALUES (1,'123','2016-05-03 15:20:00','The"
                " patient John Lock is feeling very well in London.');"
            )
        )
        conn.commit()

    # Run actual ingestion
    config = mssql_configuration(mssql_container)
    testing_table = Table(
        name="dbo.TestTable",
        column_types={
            ColumnType.FREE_TEXT: ("NoteText",),
            ColumnType.CLIENT_ID: ("ClientID",),
            ColumnType.DATE_TIME: ("DateAndTime",),
        },
        analysed_columns=("SequenceID", "NoteText", "ClientID", "DateAndTime"),
        update_logic=IngestionType.FULL_UPDATE,
        table_primary_keys=("SequenceID",),
    )
    run_ingestion(spark_session, config, [testing_table], lambda *args: bronze_dir)

    # Cleaning work in the testing SQL instance (for other tests)
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE TestTable;"))
        conn.commit()


def test_upsert_append_only_table_creates_table_if_not_exists(
    spark_session: SparkSession, delta_dir: str
):
    df = spark_session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["key", "value"])

    upsert_append_only_table(spark_session, df, delta_dir, _TABLE_AO)

    assert os.listdir(delta_dir)
    result_df = spark_session.read.format("delta").load(delta_dir)

    assert_pyspark_df_equal(result_df, df, order_by="key")


def test_upsert_full_update_table_creates_table_if_not_exists(
    spark_session: SparkSession, delta_dir: str
):
    df = spark_session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["key", "value"])

    upsert_full_update_table(spark_session, df, delta_dir, _TABLE_FU)

    assert os.listdir(delta_dir)
    result_df = spark_session.read.format("delta").load(delta_dir)

    # To ensure that added column is dropped for comparison:
    result_df = result_df.drop(ROW_HASH_COLUMN_NAME)

    assert_pyspark_df_equal(result_df, df, order_by="key")


def test_upsert_drop_and_recreate_table_creates_table_if_not_exists(
    spark_session: SparkSession, delta_dir: str
):
    df = spark_session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["key", "value"])

    upsert_drop_and_recreate_table(spark_session, df, delta_dir, None)

    assert os.listdir(delta_dir)
    result_df = spark_session.read.format("delta").load(delta_dir)

    assert_pyspark_df_equal(result_df, df, order_by="key")


def test_upsert_append_only_table_adds_new_rows_only(
    spark_session: SparkSession, delta_dir: str
):
    df = spark_session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["key", "value"])
    df.write.format("delta").save(delta_dir)

    # Do the upsert
    update_df = spark_session.createDataFrame(
        [(1, "d"), (2, "b"), (3, "c"), (4, "e")], ["key", "value"]
    )
    upsert_append_only_table(spark_session, update_df, delta_dir, _TABLE_AO)

    # Check result
    result_df = spark_session.read.format("delta").load(delta_dir)

    assert_pyspark_df_equal(
        result_df,
        spark_session.createDataFrame(
            [
                (1, "a"),  # row existed previously - does not get updated
                (2, "b"),  # exists in both
                (3, "c"),  # exists in both
                (4, "e")  # new row - gets added
                # keys 2 and 3 not present in update_df - removed
            ],
            ["key", "value"],
        ),
        order_by="key",
    )

    # Check that the first version had 3 rows
    first_version_dt = (
        spark_session.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .option("endingVersion", 0)
        .load(delta_dir)
        .select(["key", "value"])
    )

    assert_pyspark_df_equal(
        first_version_dt,
        spark_session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["key", "value"]),
        order_by="key",
    )

    # Check that the update only has added 1 row - the updated row for key 1 is ignored
    second_version_dt = (
        spark_session.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 1)
        .load(delta_dir)
        .select(["key", "value"])
    )
    assert_pyspark_df_equal(
        second_version_dt,
        spark_session.createDataFrame([(4, "e")], ["key", "value"]),
        order_by="key",
    )


def test_upsert_full_update_table_adds_new_rows_only(
    spark_session: SparkSession, delta_dir: str
):
    # To create a dataframe with all required columns (including hash one)
    # A value must be stored with hash column for this method
    upsert_full_update_table(
        spark_session,
        spark_session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["key", "value"]),
        delta_dir,
        _TABLE_FU,
    )

    # Do the upsert
    update_df = spark_session.createDataFrame(
        [(1, "d"), (2, "b"), (3, "c"), (4, "e")], ["key", "value"]
    )
    upsert_full_update_table(spark_session, update_df, delta_dir, _TABLE_FU)

    # Check result
    result_df = spark_session.read.format("delta").load(delta_dir)

    # To ensure that added column is dropped for comparison:
    result_df = result_df.drop(ROW_HASH_COLUMN_NAME)

    assert_pyspark_df_equal(
        result_df,
        spark_session.createDataFrame(
            [
                (1, "d"),  # row existed previously - but updated
                (2, "b"),  # exists in both
                (3, "c"),  # exists in both
                (4, "e")  # new row - gets added
                # keys 2 and 3 not present in update_df - removed
            ],
            ["key", "value"],
        ),
        order_by="key",
    )

    # Check that the first version had 3 rows
    first_version_dt = (
        spark_session.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .option("endingVersion", 0)
        .load(delta_dir)
        .select(["key", "value"])
    )

    assert_pyspark_df_equal(
        first_version_dt,
        spark_session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["key", "value"]),
        order_by="key",
    )

    # Check that the update only has added 1 row - the updated row for key 1 is ignored
    second_version_dt = (
        spark_session.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 1)
        .load(delta_dir)
        .select(["key", "value"])
    )

    assert_pyspark_df_equal(
        second_version_dt,
        spark_session.createDataFrame([(4, "e"), (1, "a"), (1, "d")], ["key", "value"]),
    )


def test_upsert_drop_and_recreate_table_adds_new_rows_only(
    spark_session: SparkSession, delta_dir: str
):
    df = spark_session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["key", "value"])
    df.write.format("delta").save(delta_dir)

    # Do the upsert
    update_df = spark_session.createDataFrame(
        [(1, "d"), (2, "b"), (3, "c"), (4, "e")], ["key", "value"]
    )
    upsert_drop_and_recreate_table(spark_session, update_df, delta_dir, None)

    # Check result
    result_df = spark_session.read.format("delta").load(delta_dir)

    assert_pyspark_df_equal(
        result_df,
        spark_session.createDataFrame(
            [
                (1, "d"),  # row existed previously - does not get updated
                (2, "b"),  # exists in both
                (3, "c"),  # exists in both
                (4, "e")  # new row - gets added
                # keys 2 and 3 not present in update_df - removed
            ],
            ["key", "value"],
        ),
        order_by="key",
    )


def test_upsert_append_only_table_deletes_rows(
    spark_session: SparkSession, delta_dir: str
):
    df = spark_session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["key", "value"])
    df.write.format("delta").save(delta_dir)

    # Do the upsert
    update_df = spark_session.createDataFrame([(1, "d"), (4, "e")], ["key", "value"])
    upsert_append_only_table(spark_session, update_df, delta_dir, _TABLE_AO)

    # Check the result
    result_df = spark_session.read.format("delta").load(delta_dir)
    assert_pyspark_df_equal(
        result_df,
        spark_session.createDataFrame(
            [
                (1, "a"),  # row existed previously - does not get updated
                (4, "e")  # new row - gets added
                # keys 2 and 3 not present in update_df - removed
            ],
            ["key", "value"],
        ),
        order_by="key",
    )


def test_upsert_full_update_table_deletes_rows(
    spark_session: SparkSession, delta_dir: str
):
    # A value must be stored with hash column for this method
    upsert_full_update_table(
        spark_session,
        spark_session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["key", "value"]),
        delta_dir,
        _TABLE_FU,
    )

    # Do the upsert
    update_df = spark_session.createDataFrame([(1, "d"), (4, "e")], ["key", "value"])
    upsert_full_update_table(spark_session, update_df, delta_dir, _TABLE_FU)

    # Check the result
    result_df = spark_session.read.format("delta").load(delta_dir)

    # To insure that added column is dropped for comparison:
    result_df = result_df.drop(ROW_HASH_COLUMN_NAME)

    assert_pyspark_df_equal(
        result_df,
        spark_session.createDataFrame(
            [
                (1, "d"),  # row existed previously, but update
                (4, "e")  # new row - gets added
                # keys 2 and 3 not present in update_df - removed
            ],
            ["key", "value"],
        ),
        order_by="key",
    )


def test_upsert_drop_and_recreate_table_deletes_rows(
    spark_session: SparkSession, delta_dir: str
):
    df = spark_session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["key", "value"])
    df.write.format("delta").save(delta_dir)

    # Do the upsert
    update_df = spark_session.createDataFrame([(1, "d"), (4, "e")], ["key", "value"])
    upsert_drop_and_recreate_table(spark_session, update_df, delta_dir, None)

    # Check the result
    result_df = spark_session.read.format("delta").load(delta_dir)

    assert_pyspark_df_equal(
        result_df,
        spark_session.createDataFrame(
            [
                (1, "d"),  # row existed previously, but update
                (4, "e")  # new row - gets added
                # keys 2 and 3 not present in update_df - removed
            ],
            ["key", "value"],
        ),
        order_by="key",
    )


def test_upsert_append_only_table_updates_on_composite_key(
    spark_session: SparkSession, delta_dir: str
):
    # Save the initial table
    df = spark_session.createDataFrame(
        [(1, 1, "a"), (1, 2, "b"), (1, 5, "f")], ["key1", "key2", "value"]
    )
    df.write.format("delta").save(delta_dir)

    # Do the upsert
    update_df = spark_session.createDataFrame(
        [(1, 1, "d"), (1, 2, "b"), (1, 3, "c"), (1, 4, "e")], ["key1", "key2", "value"]
    )
    _table_complex_key = copy.deepcopy(_TABLE_AO)
    _table_complex_key.table_primary_keys = ("key1", "key2")
    upsert_append_only_table(spark_session, update_df, delta_dir, _table_complex_key)

    # Check the result
    result_df = spark_session.read.format("delta").load(delta_dir)

    assert_pyspark_df_equal(
        result_df,
        spark_session.createDataFrame(
            [
                (1, 1, "a"),  # row existed previously - does not get updated
                (1, 2, "b"),  #
                (1, 3, "c"),  # new row - gets added
                (1, 4, "e")  # new row - gets added
                # key (1, 5) not present in update_df - removed
            ],
            ["key1", "key2", "value"],
        ),
    )


def test_upsert_full_update_table_updates_on_composite_key(
    spark_session: SparkSession, delta_dir: str
):
    # Save the initial table
    df = spark_session.createDataFrame(
        [(1, 1, "a"), (1, 2, "b"), (1, 5, "f")], ["key1", "key2", "value"]
    )
    _table_complex_key = copy.deepcopy(_TABLE_FU)
    _table_complex_key.table_primary_keys = ("key1", "key2")
    # A value must be stored with hash column for this method
    upsert_full_update_table(spark_session, df, delta_dir, _table_complex_key)

    # Do the upsert
    update_df = spark_session.createDataFrame(
        [(1, 1, "d"), (1, 2, "b"), (1, 3, "c"), (1, 4, "e")], ["key1", "key2", "value"]
    )
    upsert_full_update_table(spark_session, update_df, delta_dir, _table_complex_key)

    # Check the result
    result_df = spark_session.read.format("delta").load(delta_dir)

    # To insure that added column is dropped for comparison:
    result_df = result_df.drop(ROW_HASH_COLUMN_NAME)

    assert_pyspark_df_equal(
        result_df,
        spark_session.createDataFrame(
            [
                (1, 1, "d"),  # row existed previously - updated
                (1, 2, "b"),  #
                (1, 3, "c"),  # new row - gets added
                (1, 4, "e")  # new row - gets added
                # key (1, 5) not present in update_df - removed
            ],
            ["key1", "key2", "value"],
        ),
        order_by="key2",
    )


def test_upsert_drop_and_recreate_table_updates_on_composite_key(
    spark_session: SparkSession, delta_dir: str
):
    # Save the initial table
    df = spark_session.createDataFrame(
        [(1, 1, "a"), (1, 2, "b"), (1, 5, "f")], ["key1", "key2", "value"]
    )
    df.write.format("delta").save(delta_dir)

    # Do the upsert
    update_df = spark_session.createDataFrame(
        [(1, 1, "d"), (1, 2, "b"), (1, 3, "c"), (1, 4, "e")], ["key1", "key2", "value"]
    )
    upsert_drop_and_recreate_table(spark_session, update_df, delta_dir, None)

    # Check the result
    result_df = spark_session.read.format("delta").load(delta_dir)

    assert_pyspark_df_equal(
        result_df,
        spark_session.createDataFrame(
            [
                (1, 1, "d"),  # row existed previously - updated
                (1, 2, "b"),  #
                (1, 3, "c"),  # new row - gets added
                (1, 4, "e")  # new row - gets added
                # key (1, 5) not present in update_df - removed
            ],
            ["key1", "key2", "value"],
        ),
        order_by="key2",
    )


# ===========================
