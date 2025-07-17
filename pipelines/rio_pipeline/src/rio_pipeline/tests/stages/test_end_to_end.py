from pytest import mark
from pyspark_test import assert_pyspark_df_equal  # type: ignore
from pyspark.sql import SparkSession
from rio_pipeline.tests.helpers import mssql_configuration
from sqlalchemy import create_engine, text
from testcontainers.mssql import SqlServerContainer  # type: ignore
from rio_pipeline.common_types import (
    ColumnType,
    IngestionType,
    Table,
    DatalakeZone,
)
from rio_pipeline.entrypoints.ingestion import run_ingestion
from rio_pipeline.entrypoints.pseudonymisation import run_pseudonymisation
from rio_pipeline.entrypoints.feature_extraction import run_feature_extraction
from rio_pipeline.entrypoints.cleaning_step import run_cleaning_step
from rio_pipeline.datalake import read_delta_table
from rio_pipeline.config import RUN_E2E_TEST, RUN_FEATURE_EXTRACTION
import os


@mark.skipif(not RUN_E2E_TEST, reason="E2E test skipped")
def test_end_to_end(
    spark_session: SparkSession,
    mssql_container: SqlServerContainer,
    bronze_dir: str,
    silver_dir: str,
    gold_dir: str,
    feature_extraction_udf,
    presidio_udf,
) -> None:
    if not RUN_E2E_TEST:
        # To be sure test is skipped
        return
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
        conn.execute(
            text(
                "INSERT INTO TestTable VALUES (2,'456','2017-08-07 14:25:00','The"
                " patient Peter Smith is feeling badly in Liverpool.');"
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

    def mocked_uri_constructor(_session, position, table_name):
        if position == DatalakeZone.BRONZE:
            return bronze_dir + table_name
        if position == DatalakeZone.SILVER:
            return silver_dir + table_name
        elif position == DatalakeZone.GOLD:
            return gold_dir + table_name

    def mocked_create_table_in_unity_catalog(*args, **kwargs):
        pass

    # Run the pipelines
    run_ingestion(
        spark_session=spark_session,
        config=config,
        table_config=[testing_table],
        uri_constructor=mocked_uri_constructor,
    )
    run_pseudonymisation(
        spark_session=spark_session,
        anonymise_udf=presidio_udf,
        table_config=[testing_table],
        uri_constructor=mocked_uri_constructor,
    )
    run_feature_extraction(
        spark_session=spark_session,
        feature_extraction_udf=feature_extraction_udf,
        table_config=[testing_table],
        uri_constructor=mocked_uri_constructor,
        unity_catalogue_creator=mocked_create_table_in_unity_catalog,
    )
    run_cleaning_step(
        spark_session=spark_session,
        table_config=[testing_table],
        uri_constructor=mocked_uri_constructor,
    )

    # SECOND ROUND
    with engine.connect() as conn:
        # Insert new row
        conn.execute(
            text(
                "INSERT INTO TestTable VALUES (3,'789','2019-03-03 15:20:00','The"
                " patient Don Perinon is feeling quite alright in Manchester.');"
            )
        )
        # Delete one existing row
        conn.execute(text("DELETE FROM TestTable WHERE SequenceID=2;"))
        conn.commit()

    # Run the pipelines (second round)
    run_ingestion(
        spark_session=spark_session,
        config=config,
        table_config=[testing_table],
        uri_constructor=mocked_uri_constructor,
    )
    run_pseudonymisation(
        spark_session=spark_session,
        anonymise_udf=presidio_udf,
        table_config=[testing_table],
        uri_constructor=mocked_uri_constructor,
    )
    run_feature_extraction(
        spark_session=spark_session,
        feature_extraction_udf=feature_extraction_udf,
        table_config=[testing_table],
        uri_constructor=mocked_uri_constructor,
        unity_catalogue_creator=mocked_create_table_in_unity_catalog,
    )
    run_cleaning_step(
        spark_session=spark_session,
        table_config=[testing_table],
        uri_constructor=mocked_uri_constructor,
    )

    # CHECK THE CONTENT OF BRONZE/SILVER/GOLD
    bronze_content = read_delta_table(
        spark_session,
        mocked_uri_constructor(None, DatalakeZone.BRONZE, testing_table.name),
    )
    assert bronze_content.count() == 2
    assert len(bronze_content.columns) == 5
    silver_content = read_delta_table(
        spark_session,
        mocked_uri_constructor(None, DatalakeZone.SILVER, testing_table.name),
    )
    assert silver_content.count() == 2
    assert len(silver_content.columns) == 5
    gold_content = read_delta_table(
        spark_session,
        mocked_uri_constructor(None, DatalakeZone.GOLD, testing_table.name),
    )
    assert gold_content.count() == 2
    if RUN_FEATURE_EXTRACTION:
        assert len(gold_content.columns) == 6
    else:
        assert len(gold_content.columns) == 5
    # CLEANING work in the testing SQL instance (for other tests)
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE TestTable;"))
        conn.commit()


@mark.test_monitoring  # noqa Marker to be used when running individual testcase
def test_monitoring(
    spark_session: SparkSession,
    mssql_container: SqlServerContainer,
    bronze_dir: str,
    silver_dir: str,
    gold_dir: str,
    monitoring_dir: str,
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
        conn.execute(
            text(
                "INSERT INTO TestTable VALUES (2,'456','2017-08-07 14:25:00','The"
                " patient Peter Smith is feeling badly in Liverpool.');"
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

    config = mssql_configuration(mssql_container)
    testing_table2 = Table(
        name="dbo.TestTable2",
        column_types={
            ColumnType.FREE_TEXT: ("NoteText",),
            ColumnType.CLIENT_ID: ("ClientID",),
            ColumnType.DATE_TIME: ("DateAndTime",),
        },
        analysed_columns=("SequenceID", "NoteText", "ClientID", "DateAndTime"),
        update_logic=IngestionType.FULL_UPDATE,
        table_primary_keys=("SequenceID",),
    )

    def mocked_uri_constructor(_session, position, table_name):
        if position == DatalakeZone.BRONZE:
            return bronze_dir + table_name
        if position == DatalakeZone.SILVER:
            return silver_dir + table_name
        if position == DatalakeZone.MONITORING:
            return monitoring_dir + table_name
        elif position == DatalakeZone.GOLD:
            return gold_dir + table_name

    # Run the pipelines
    run_ingestion(
        spark_session=spark_session,
        config=config,
        table_config=[testing_table],
        uri_constructor=mocked_uri_constructor,
    )

    # Check monitoring logs are correct when TestTable has values
    logs_directory = mocked_uri_constructor(None, DatalakeZone.MONITORING, "UpsertLogs")
    parquet_file = [
        os.path.join(logs_directory, f)
        for f in os.listdir(logs_directory)
        if f.endswith(".parquet")
    ]
    latest_file = max(parquet_file, key=os.path.getctime)
    actual_df = spark_session.read.format("parquet").load(logs_directory)
    actual_df = actual_df.drop("Date_Time")

    expected_df = spark_session.createDataFrame(
        [(testing_table.name, 2)],
        ["Row_Count", "Value"],
    )

    assert_pyspark_df_equal(
        actual_df,
        expected_df,
        order_by="Row_Count",
    )

    # Check monitoring logs when TestTable is empty
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM TestTable"))
        conn.commit()

    # Run the pipelines (second round)
    run_ingestion(
        spark_session=spark_session,
        config=config,
        table_config=[testing_table],
        uri_constructor=mocked_uri_constructor,
    )

    logs_directory = mocked_uri_constructor(None, DatalakeZone.MONITORING, "UpsertLogs")
    parquet_file = [
        os.path.join(logs_directory, f)
        for f in os.listdir(logs_directory)
        if f.endswith(".parquet")
    ]
    latest_file = max(parquet_file, key=os.path.getctime)
    actual_df = spark_session.read.format("parquet").load(latest_file)
    actual_df = actual_df.drop("Date_Time")

    expected_df = spark_session.createDataFrame(
        [(testing_table.name, 0)],
        ["Row_Count", "Value"],
    )
    assert_pyspark_df_equal(
        actual_df,
        expected_df,
        order_by="Row_Count",
    )

    # Check monitoring logs when there are two Test tables
    with engine.connect() as conn:
        # Create a new table
        conn.execute(
            text(
                "CREATE TABLE TestTable2("
                "[SequenceID] int NOT NULL, "
                "[ClientID] varchar(15) NOT NULL, "
                "[DateAndTime] datetime NOT NULL, "
                "[NoteText] nvarchar(max)"
                ");"
            )
        )
        conn.execute(
            text(
                "INSERT INTO TestTable2 VALUES (123,'1000','2019-02-03 15:20:00','The"
                " patient Alex is not feeling very well in Bradford.');"
            )
        )
        conn.commit()

    # Run the pipelines (third round)
    run_ingestion(
        spark_session=spark_session,
        config=config,
        table_config=[testing_table, testing_table2],
        uri_constructor=mocked_uri_constructor,
    )

    logs_directory = mocked_uri_constructor(None, DatalakeZone.MONITORING, "UpsertLogs")
    parquet_file = [
        os.path.join(logs_directory, f)
        for f in os.listdir(logs_directory)
        if f.endswith(".parquet")
    ]
    latest_file = max(parquet_file, key=os.path.getctime)
    actual_df = spark_session.read.format("parquet").load(latest_file)
    actual_df = actual_df.drop("Date_Time")

    expected_df = spark_session.createDataFrame(
        [(testing_table.name, 0), (testing_table2.name, 1)],
        ["Row_Count", "Value"],
    )
    assert_pyspark_df_equal(
        actual_df,
        expected_df,
        order_by="Row_Count",
    )

    # CLEANING work in the testing SQL instance (for other tests)
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE TestTable;"))
        conn.execute(text("DROP TABLE TestTable2;"))
        conn.commit()
