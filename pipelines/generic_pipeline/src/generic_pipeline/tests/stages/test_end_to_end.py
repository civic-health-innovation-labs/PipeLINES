from pytest import mark
from pyspark.sql import SparkSession
from generic_pipeline.tests.helpers import mssql_configuration
from sqlalchemy import create_engine, text
from testcontainers.mssql import SqlServerContainer  # type: ignore
from generic_pipeline.common_types import ColumnType, IngestionType, Table, DatalakeZone
from generic_pipeline.entrypoints.ingestion import run_ingestion
from generic_pipeline.entrypoints.pseudonymisation import run_pseudonymisation
from generic_pipeline.entrypoints.feature_extraction import run_feature_extraction
from generic_pipeline.entrypoints.cleaning_step import run_cleaning_step
from generic_pipeline.datalake import read_delta_table
from generic_pipeline.config import RUN_E2E_TEST, RUN_FEATURE_EXTRACTION


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
