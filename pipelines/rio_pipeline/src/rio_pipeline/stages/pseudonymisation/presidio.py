from presidio_analyzer import AnalyzerEngine  # type: ignore
from presidio_anonymizer import AnonymizerEngine  # type: ignore
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from rio_pipeline.config import (
    PII_ENTITIES,
    PSEUDONYMISATION_LANGUAGE,
    MAXIMAL_STRING_SIZE,
)


def anonymise(text: str, broadcasted_analyser, broadcasted_anonymiser) -> str | None:
    """Anonymise text entry with analyser and anonymiser broadcast by Spark"""
    if text:
        if len(text) > MAXIMAL_STRING_SIZE:
            # Cut the strings for required sizes
            text = text[:MAXIMAL_STRING_SIZE]
        analyser = broadcasted_analyser.value
        anonymiser = broadcasted_anonymiser.value
        results = analyser.analyze(
            text=text, entities=PII_ENTITIES, language=PSEUDONYMISATION_LANGUAGE
        )
        return anonymiser.anonymize(text=text, analyzer_results=results).text
    else:
        return None


def broadcast_presidio_with_anonymise_udf(spark_session: SparkSession):
    """Broadcast Presidio engines across Spark cluster and create anonymise UDF"""
    broadcasted_analyser = spark_session.sparkContext.broadcast(AnalyzerEngine())
    broadcasted_anonymiser = spark_session.sparkContext.broadcast(AnonymizerEngine())

    anonymise_udf = udf(
        lambda text: anonymise(text, broadcasted_analyser, broadcasted_anonymiser),
        StringType(),
    )
    return anonymise_udf
