import os
import random
import shutil
import string
import time
from pathlib import Path
from typing import Generator, Iterator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from rio_pipeline.stages.pseudonymisation.presidio import (
    broadcast_presidio_with_anonymise_udf,
)
from rio_pipeline.stages.feature_extraction.scispacybr import (
    broadcast_scispacy_with_udf,
)
from sqlalchemy import Connection, create_engine
from testcontainers.mssql import SqlServerContainer  # type: ignore

test_datalake_uri = "test.dfs.core.windows.net"


@pytest.fixture(scope="session")
def spark_session() -> Iterator[SparkSession]:
    # Used to ensure the tested python environment is using UTC like the Spark
    # session so that there are no differences between timezones.
    os.environ["TZ"] = "UTC"
    time.tzset()

    builder = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars", Path(__file__).parent / "mssql-jdbc-12.4.0.jre11.jar")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.secret.datalake-uri", test_datalake_uri)
        .config("spark.secret.unity-catalog-catalog-name", "unity-catalogue-name")
        .config("spark.secret.unity-catalog-schema-name", "unity-catalogue-schema")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.databricks.delta.properties.defaults.enableChangeDataFeed",
            "true",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()

    yield session

    session.stop()


@pytest.fixture
def mssql_container() -> Iterator[SqlServerContainer]:
    container = SqlServerContainer(dialect="mssql+pymssql")

    container.start()
    yield container
    container.stop()


@pytest.fixture
def mssql_connection(
    mssql_container: SqlServerContainer,
) -> Iterator[Connection]:
    with create_engine(mssql_container.get_connection_url()).connect() as conn:
        yield conn


@pytest.fixture
def delta_dir() -> Generator:
    path = f"/tmp/delta_{''.join(random.choice(string.ascii_lowercase) for i in range(10))}"

    yield path

    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def bronze_dir() -> Generator:
    path = f"/tmp/bronze_{''.join(random.choice(string.ascii_lowercase) for i in range(10))}"

    yield path

    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def presidio_udf(spark_session: SparkSession) -> Generator:
    udf = broadcast_presidio_with_anonymise_udf(spark_session)
    yield udf


@pytest.fixture(scope="session")
def feature_extraction_udf(spark_session: SparkSession) -> Generator:
    udf = broadcast_scispacy_with_udf(spark_session)
    yield udf


@pytest.fixture
def internal_dir() -> Generator:
    path = f"/tmp/internal_{''.join(random.choice(string.ascii_lowercase) for i in range(10))}"

    yield path

    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def silver_dir() -> Generator:
    path = f"/tmp/silver_{''.join(random.choice(string.ascii_lowercase) for i in range(10))}"

    yield path

    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def monitoring_dir() -> Generator:
    path = f"/tmp/monitoring_{''.join(random.choice(string.ascii_lowercase) for i in range(10))}"

    yield path

    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def gold_dir() -> Generator:
    path = (
        f"/tmp/gold_{''.join(random.choice(string.ascii_lowercase) for i in range(10))}"
    )

    yield path

    shutil.rmtree(path, ignore_errors=True)
