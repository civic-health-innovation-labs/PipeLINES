import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

from rio_pipeline.config import FEATURE_EXTRACTION_LABELS, MAXIMAL_STRING_SIZE


def extract_features(text: str, broadcasted_nlp) -> list[list[str]]:
    """Extract features using en_ner_bc5cdr_md model.

    Returns:
        list[list[str]]: Values as arrays in order defined in the FEATURE_EXTRACTION_LABELS.
    """
    if text:
        if len(text) > MAXIMAL_STRING_SIZE:
            # Cut the strings for required sizes
            text = text[:MAXIMAL_STRING_SIZE]
        nlp = broadcasted_nlp.value
        doc = nlp(text)

        # Pre-create dictionary with labels matching to expected extracted entities
        classified_entities: dict[str, list[str]] = {
            _label: [] for _label in FEATURE_EXTRACTION_LABELS
        }
        for ent in doc.ents:
            # Add entities from extracted values
            classified_entities[ent.label_].append(ent.text)

        return [_ent for _ent in classified_entities.values()]
    else:
        return [[] for _ in FEATURE_EXTRACTION_LABELS]


def broadcast_scispacy_with_udf(spark_session: SparkSession):
    """Broadcast scispacy NLP across Spark cluster and create UDF"""
    broadcasted_nlp = spark_session.sparkContext.broadcast(
        spacy.load("en_ner_bc5cdr_md")
    )

    extract_features_udf = udf(
        lambda text: extract_features(text, broadcasted_nlp),
        ArrayType(ArrayType(StringType())),
    )
    return extract_features_udf
