from pytest import mark
from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal  # type: ignore
from delta import DeltaTable
from generic_pipeline.datalake import (
    DatalakeZone,
    construct_uri,
    overwrite_delta_table,
    read_delta_table,
    read_delta_table_change_feed,
    upsert_delta_table,
    vacuum_delta_table,
)
from generic_pipeline.tests.conftest import test_datalake_uri
from generic_pipeline.common_types import Table, IngestionType
from generic_pipeline.config import SKIP_VACUUM_TEST


@mark.skipif(SKIP_VACUUM_TEST, reason="Test containing vacuum is skipped")
def test_read_delta_table_change_feed(spark_session: SparkSession, delta_dir: str):
    _cols_names = ["key", "value"]
    start_df = spark_session.createDataFrame(
        [("1", "a"), ("2", "b"), ("3", "c")], _cols_names
    )
    start_df.write.format("delta").save(delta_dir)
    # Check if the initial reading is correct (without any vacuum)
    start_df_check = read_delta_table_change_feed(spark_session, delta_dir)
    assert_pyspark_df_equal(start_df_check, start_df)
    # Call vacuum to test
    vacuum_delta_table(spark_session, delta_dir)
    # Prepare update table
    modified_df = spark_session.createDataFrame(
        [("1", "a"), ("2", "b"), ("4", "d")], _cols_names
    )
    delta_file = DeltaTable.forPath(sparkSession=spark_session, path=delta_dir)
    # Update delta file
    delta_file.alias("start").merge(
        modified_df.alias("modified"), "start.key=modified.key"
    ).whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute()
    # Check inserts
    inserts_after_vacuum = read_delta_table_change_feed(spark_session, delta_dir)
    inserts_comparison = spark_session.createDataFrame([("4", "d")], _cols_names)
    assert_pyspark_df_equal(inserts_after_vacuum, inserts_comparison)
    # Check deletes
    delete_after_vacuum = read_delta_table_change_feed(
        spark_session, delta_dir, "delete"
    )
    delete_comparison = spark_session.createDataFrame([("3", "c")], _cols_names)
    assert_pyspark_df_equal(delete_after_vacuum, delete_comparison)
    # Check after new vacuum
    vacuum_delta_table(spark_session, delta_dir)
    final_df = read_delta_table_change_feed(spark_session, delta_dir)
    assert final_df.count() == 0


def test_construct_uri(spark_session: SparkSession):
    table = "dbo.TestTable"
    zone = DatalakeZone.BRONZE
    uri = construct_uri(spark_session, zone, table)
    assert uri == f"abfss://{zone.value}@{test_datalake_uri}/TestTable"


def test_read_delta_table(spark_session: SparkSession, delta_dir: str):
    test_df = spark_session.createDataFrame(
        [(1, "a"), (2, "b"), (3, "c")], ["key", "value"]
    )
    test_df.write.format("delta").save(delta_dir)

    read_df = read_delta_table(spark_session, delta_dir)
    assert_pyspark_df_equal(read_df, test_df, order_by="key")


def test_overwrite_delta_table(spark_session: SparkSession, delta_dir: str):
    test_df = spark_session.createDataFrame(
        [(1, "a"), (2, "b"), (3, "c")], ["key", "value"]
    )
    overwrite_delta_table(test_df, delta_dir)

    written_df = spark_session.read.format("delta").load(delta_dir)
    assert_pyspark_df_equal(written_df, test_df, order_by="key")


def test_upsert_delta_table(spark_session: SparkSession, delta_dir: str):
    _cols_names = ["key", "value"]
    start_df = spark_session.createDataFrame(
        [("1", "a"), ("2", "b"), ("3", "c")], _cols_names
    )
    single_table = Table(
        name="test_table",
        column_types={},
        analysed_columns=tuple(_cols_names),
        update_logic=IngestionType.APPEND_AND_REMOVE_ONLY,
        table_primary_keys=("key",),
    )
    upsert_delta_table(spark_session, start_df, start_df, delta_dir, single_table)
    # Compare if the stored version is correct
    first_version_target = read_delta_table_change_feed(spark_session, delta_dir)
    assert_pyspark_df_equal(start_df, first_version_target)
    # Test upsert with a new version
    new_version_source = spark_session.createDataFrame(
        [("1", "a"), ("2", "b"), ("4", "d")], _cols_names
    )
    new_version_insert = spark_session.createDataFrame([("4", "d")], _cols_names)
    new_version_remove = spark_session.createDataFrame([("3", "c")], _cols_names)
    upsert_delta_table(
        spark_session, new_version_insert, new_version_remove, delta_dir, single_table
    )
    new_version_target = read_delta_table(spark_session, delta_dir)
    assert_pyspark_df_equal(new_version_source, new_version_target)
