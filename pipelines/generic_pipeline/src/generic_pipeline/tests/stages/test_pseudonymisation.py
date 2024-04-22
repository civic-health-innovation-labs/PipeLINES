import datetime
from pytest import mark
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.udf import UserDefinedFunction
from pyspark_test import assert_pyspark_df_equal  # type: ignore
from generic_pipeline.stages.pseudonymisation.transform import pseudo_transform
from generic_pipeline.common_types import ColumnType, IngestionType, Table, DatalakeZone
from generic_pipeline.entrypoints.pseudonymisation import run_pseudonymisation
from generic_pipeline.config import RUN_FREE_TEXT_ANONYMISATION


# Pseudonymisation - Run the whole logic
def test_run_pseudonymisation(
    spark_session: SparkSession,
    presidio_udf: UserDefinedFunction,
    bronze_dir,
    silver_dir,
) -> None:
    input_df = spark_session.createDataFrame(
        [
            ("This is a note value", 1, "text value"),
            ("This is a note value", 2, "text value"),
        ],
        ["NoteText", "ID", "Text"],
    )
    table: Table = Table(
        name="testing_table",
        column_types={ColumnType.OTHER_IDENTIFIABLE: ("NoteText",)},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )
    # Save DF to bronze
    input_df.write.format("delta").mode("overwrite").save(bronze_dir + table.name)

    def mocked_uri_constructor(_session, position, table_name):
        if position == DatalakeZone.BRONZE:
            return bronze_dir + table_name
        elif position == DatalakeZone.SILVER:
            return silver_dir + table_name

    run_pseudonymisation(
        spark_session=spark_session,
        anonymise_udf=presidio_udf,
        table_config=[table],
        uri_constructor=mocked_uri_constructor,
    )


# Pseudonymisation - Remove column tests
def test_transformer_remove_columns(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame(
        [
            ("This is a note value", 1, "text value"),
            ("This is a note value", 2, "text value"),
        ],
        ["NoteText", "ID", "Text"],
    )
    table: Table = Table(
        name="table1",
        column_types={ColumnType.OTHER_IDENTIFIABLE: ("NoteText",)},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )

    output_df = pseudo_transform(input_df, table, presidio_udf)

    expected_df = spark_session.createDataFrame(
        [(1, "text value"), (2, "text value")], ["ID", "Text"]
    )
    assert set(expected_df.columns) == set(output_df.columns)
    assert expected_df.collect() == output_df.collect()


def test_transformer_remove_columns_multiple_columns(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame(
        [
            ("This is a note value", 1, "text value", "another text"),
            ("This is a note value", 2, "text value", "another text"),
        ],
        ["NoteText", "ID", "Text", "Another"],
    )

    table: Table = Table(
        name="table1",
        column_types={ColumnType.OTHER_IDENTIFIABLE: ("Text", "Another")},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )

    output_df = pseudo_transform(input_df, table, presidio_udf)

    expected_df = spark_session.createDataFrame(
        [("This is a note value", 1), ("This is a note value", 2)], ["NoteText", "ID"]
    )
    assert set(expected_df.columns) == set(output_df.columns)
    assert expected_df.collect() == output_df.collect()


def test_pseudo_transform_raises_exception_when_column_doesnt_exist(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame(
        [
            ("This is a note value", 1, "text value"),
            ("This is a note value", 2, "text value"),
        ],
        ["NoteText", "ID", "Text"],
    )
    table: Table = Table(
        name="table1",
        column_types={ColumnType.OTHER_IDENTIFIABLE: ("ColumnThatDoesntExist",)},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )

    with pytest.raises(KeyError) as excinfo:
        pseudo_transform(input_df, table, presidio_udf)

    assert "ColumnThatDoesntExist" in str(excinfo.value)


def test_pseudo_transform_does_not_change_df(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame(
        [
            ("This is a note value", 1, "text value"),
            ("This is a note value", 2, "text value"),
        ],
        ["NoteText", "ID", "Text"],
    )

    table: Table = Table(
        name="table1",
        column_types={ColumnType.OTHER_IDENTIFIABLE: tuple()},
        analysed_columns=tuple(),
        update_logic=IngestionType.FULL_UPDATE,
    )

    output_df = pseudo_transform(input_df, table, presidio_udf)

    expected_df = spark_session.createDataFrame(
        [
            ("This is a note value", 1, "text value"),
            ("This is a note value", 2, "text value"),
        ],
        ["NoteText", "ID", "Text"],
    )
    assert set(expected_df.columns) == set(output_df.columns)
    assert expected_df.collect() == output_df.collect()


# Pseudonymisation - Free text tests
@mark.skipif(not RUN_FREE_TEXT_ANONYMISATION, reason="FT test skipped")
def test_pseudo_transform_pseudonymises_free_text_name_and_location(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame(
        [("John Smith is in London", 1), ("Adam is in London", 2)], ["NoteText", "ID"]
    )
    table: Table = Table(
        name="table1",
        column_types={ColumnType.FREE_TEXT: ("NoteText",)},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )

    output_df = pseudo_transform(input_df, table, presidio_udf)

    expected_df = spark_session.createDataFrame(
        [("<PERSON> is in <LOCATION>", 1), ("<PERSON> is in <LOCATION>", 2)],
        ["NoteText", "ID"],
    )
    assert set(expected_df.columns) == set(output_df.columns)
    assert expected_df.collect() == output_df.collect()


@mark.skipif(not RUN_FREE_TEXT_ANONYMISATION, reason="FT test skipped")
def test_pseudo_transform_pseudonymises_multiple_free_text_columns(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame(
        [
            ("John Smith is in London", 1, "This is London"),
            ("Adam is in London", 2, "This is London"),
        ],
        ["NoteText", "ID", "OtherText"],
    )
    table: Table = Table(
        name="table1",
        column_types={ColumnType.FREE_TEXT: ("NoteText", "OtherText")},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )

    output_df = pseudo_transform(input_df, table, presidio_udf)

    expected_df = spark_session.createDataFrame(
        [
            ("<PERSON> is in <LOCATION>", 1, "This is <LOCATION>"),
            ("<PERSON> is in <LOCATION>", 2, "This is <LOCATION>"),
        ],
        ["NoteText", "ID", "OtherText"],
    )
    assert set(expected_df.columns) == set(output_df.columns)
    assert expected_df.collect() == output_df.collect()


def test_pseudo_transform_doesnt_throw_error_when_no_columns_are_configured(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame(
        [
            ("John Smith is in London", 1, "This is London"),
            ("Adam is in London", 2, "This is London"),
        ],
        ["NoteText", "ID", "OtherText"],
    )
    table: Table = Table(
        name="table1",
        column_types={ColumnType.FREE_TEXT: tuple()},
        analysed_columns=tuple(),
        update_logic=IngestionType.FULL_UPDATE,
    )

    output_df = pseudo_transform(input_df, table, presidio_udf)

    expected_df = spark_session.createDataFrame(
        [
            ("John Smith is in London", 1, "This is London"),
            ("Adam is in London", 2, "This is London"),
        ],
        ["NoteText", "ID", "OtherText"],
    )
    assert set(expected_df.columns) == set(output_df.columns)
    assert expected_df.collect() == output_df.collect()


def test_pseudo_transform_raises_exception_when_free_text_column_doesnt_exist(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame(
        [("John Smith is in London", 1), ("Adam is in London", 2)], ["NoteText", "ID"]
    )
    table: Table = Table(
        name="table1",
        column_types={ColumnType.FREE_TEXT: ("ColumnThatDoesntExist",)},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )

    with pytest.raises(KeyError) as excinfo:
        pseudo_transform(input_df, table, presidio_udf)

    assert "ColumnThatDoesntExist" in str(excinfo.value)


# Pseudonymisation - Round Dates tests
def test_pseudo_transform_rounds_date_time(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_datetime = datetime.datetime(2023, 8, 16, 3, 4, 5)
    input_df = spark_session.createDataFrame(
        [(1, input_datetime)], ["ID", "SomeDateTimeColumn"]
    )
    table: Table = Table(
        name="table1",
        column_types={ColumnType.DATE_TIME: ("SomeDateTimeColumn",)},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )

    output_df = pseudo_transform(input_df, table, presidio_udf)

    expected_datetime = datetime.datetime(2023, 8, 16, 3, 0, 0)

    output_datetime = output_df.collect()[0]["SomeDateTimeColumn"]
    assert expected_datetime == output_datetime


def test_pseudo_transform_rounds_date(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_datetime = datetime.datetime(2023, 8, 16, 5, 4, 2)
    input_df = spark_session.createDataFrame(
        [(1, input_datetime)], ["ID", "SomeDateColumn"]
    )
    table: Table = Table(
        name="table1",
        column_types={ColumnType.DATE: ("SomeDateColumn",)},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )

    output_df = pseudo_transform(input_df, table, presidio_udf)

    expected_datetime = datetime.datetime(2023, 8, 1, 0, 0)

    output_datetime = output_df.collect()[0]["SomeDateColumn"]
    assert expected_datetime == output_datetime


def test_pseudo_transform_raises_exception_when_date_column_doesnt_exist(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_datetime = datetime.datetime(2023, 8, 16, 5, 4, 2)
    input_df = spark_session.createDataFrame(
        [(1, input_datetime)], ["ID", "SomeDateTimeColumn"]
    )
    table: Table = Table(
        name="table1",
        column_types={ColumnType.DATE_TIME: ("ColumnThatDoesntExist",)},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )

    with pytest.raises(KeyError) as excinfo:
        pseudo_transform(input_df, table, presidio_udf)

    assert "ColumnThatDoesntExist" in str(excinfo.value)


def test_pseudo_transform_returns_none_for_dttm_when_col_has_invalid_value(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_datetime = "invalid value"
    input_df = spark_session.createDataFrame(
        [(1, input_datetime)], ["ID", "SomeDateTimeColumn"]
    )
    table: Table = Table(
        name="table1",
        column_types={ColumnType.DATE_TIME: ("SomeDateTimeColumn",)},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )

    output_df = pseudo_transform(input_df, table, presidio_udf)

    output_datetime = output_df.collect()[0]["SomeDateTimeColumn"]
    assert output_datetime is None


# Pseudonymisation - Hash client id tests
def test_pseudo_transform_hashes_id(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame([(1, "Text1")], ["ID", "Text"])
    table: Table = Table(
        name="table1",
        column_types={ColumnType.CLIENT_ID: ("ID",)},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )

    output_df = pseudo_transform(input_df, table, presidio_udf)

    expected_df = spark_session.createDataFrame(
        [("e59cb3f3ffba6255f0f32b278a76f8a44780fde36bb7a1b3428a394ff4c39596", "Text1")],
        ["ID_hashed", "Text"],
    )
    assert set(expected_df.columns) == set(output_df.columns)
    assert_pyspark_df_equal(output_df, expected_df, order_by="Text")


def test_pseudo_transform_raises_exception_when_client_id_columns_doesnt_exist(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame([(1, "Text1")], ["ID", "Text"])
    table: Table = Table(
        name="table1",
        column_types={ColumnType.CLIENT_ID: ("ThisColumnDoesntExist",)},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )

    with pytest.raises(KeyError) as excinfo:
        pseudo_transform(input_df, table, presidio_udf)

    assert "ThisColumnDoesntExist" in str(excinfo.value)
