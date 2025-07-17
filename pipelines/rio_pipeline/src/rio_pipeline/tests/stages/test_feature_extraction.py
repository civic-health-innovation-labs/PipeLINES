import spacy
from pytest import mark
from pyspark.sql import SparkSession
from pyspark.sql.udf import UserDefinedFunction
from pyspark_test import assert_pyspark_df_equal  # type: ignore
from rio_pipeline.stages.feature_extraction.transform import (
    feature_extraction_transform,
)
from rio_pipeline.common_types import (
    ColumnType,
    IngestionType,
    Table,
    DatalakeZone,
)
from rio_pipeline.config import (
    FEATURE_EXTRACTION_SUFFIX,
    RUN_FEATURE_EXTRACTION,
)
from rio_pipeline.entrypoints.feature_extraction import run_feature_extraction


@mark.skipif(not RUN_FEATURE_EXTRACTION, reason="Feature Extraction test skipped")
def test_run_feature_extraction(
    spark_session: SparkSession, silver_dir, gold_dir, feature_extraction_udf
):
    if not RUN_FEATURE_EXTRACTION:
        # To be sure test is skipped
        return
    input_df = spark_session.createDataFrame(
        [
            (
                "John Example was diagnosed with paranoid schizophrenia"
                " on 26th July 2023 and is treated with aripiprazole.",
                1,
                "text value",
            ),
            ("Nothing to be extracted", 2, "text value"),
        ],
        ["NoteText", "ID", "Text"],
    )
    table: Table = Table(
        name="table1",
        column_types={ColumnType.FREE_TEXT: ("NoteText",)},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )
    # Save DF to silver
    input_df.write.format("delta").mode("overwrite").save(silver_dir + table.name)

    def mocked_uri_constructor(_session, position, table_name):
        if position == DatalakeZone.SILVER:
            return silver_dir + table_name
        elif position == DatalakeZone.GOLD:
            return gold_dir + table_name

    def mocked_create_table_in_unity_catalog(*args, **kwargs):
        pass

    run_feature_extraction(
        spark_session=spark_session,
        feature_extraction_udf=feature_extraction_udf,
        table_config=[table],
        uri_constructor=mocked_uri_constructor,
        unity_catalogue_creator=mocked_create_table_in_unity_catalog,
    )


@mark.skipif(not RUN_FEATURE_EXTRACTION, reason="Feature Extraction test skipped")
def test_scispacy(spark_session: SparkSession):
    """Test if the SciSpacy model is ready"""
    nlp = spacy.load("en_ner_bc5cdr_md")
    text = (
        "John Example was diagnosed with paranoid schizophrenia on 26th July 2023 "
        "and is treated with aripiprazole."
    )
    doc = nlp(text)

    labels: set[str] = set(nlp.get_pipe("ner").labels)  # type: ignore
    dct: dict[str, list[str]] = {_label: [] for _label in labels}
    for ent in doc.ents:
        dct[ent.label_].append(ent.text)
    assert len(dct.keys()) == 2


@mark.skipif(not RUN_FEATURE_EXTRACTION, reason="Feature Extraction test skipped")
def test_feature_extraction(
    spark_session: SparkSession, feature_extraction_udf: UserDefinedFunction
):
    input_df = spark_session.createDataFrame(
        [
            (
                "John Example was diagnosed with paranoid schizophrenia"
                " on 26th July 2023 and is treated with aripiprazole.",
                1,
                "text value",
            ),
            ("Nothing to be extracted", 2, "text value"),
        ],
        ["NoteText", "ID", "Text"],
    )
    table: Table = Table(
        name="table1",
        column_types={ColumnType.FREE_TEXT: ("NoteText",)},
        analysed_columns=("NoteText", "ID", "Text"),
        update_logic=IngestionType.FULL_UPDATE,
    )
    output_df = feature_extraction_transform(input_df, table, feature_extraction_udf)

    assert_pyspark_df_equal(
        output_df,
        spark_session.createDataFrame(
            [
                (
                    "John Example was diagnosed with paranoid schizophrenia"
                    " on 26th July 2023 and is treated with aripiprazole.",
                    1,
                    "text value",
                    [["paranoid schizophrenia"], ["aripiprazole"]],
                ),
                ("Nothing to be extracted", 2, "text value", [[], []]),
            ],
            [
                "NoteText",
                "ID",
                "Text",
                "".join(["NoteText", FEATURE_EXTRACTION_SUFFIX]),
            ],
        ),
        order_by="ID",
    )
