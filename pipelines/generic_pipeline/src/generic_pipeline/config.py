# Name of the pipeline
PIPELINE_NAME = "generic_pipeline"
# Name for the column that is appended to tables to analyse whether it was updated:
ROW_HASH_COLUMN_NAME = "MD5_ROW_HASH"
# This string is appended to column name that is hashed
CLIENT_ID_HASH_SUFFIX = "_hashed"
# Suffix that is added after column name for feature extractions
FEATURE_EXTRACTION_SUFFIX = "_feature_extraction"
# Salt for hashing Client Index columns
HASH_SALT = "TODO_FILL_RANDOM_VALUE"
# Language used for Presidio
PSEUDONYMISATION_LANGUAGE = "en"
# Suffix to apply to column names for new columns created by feature extraction
EXTRACTED_PREFIX = "_extracted"
# Number of Spark workers and cores to optimise parallel processing for
WORKER_COUNT = 8
CORE_COUNT = 4
# Maximal size for strings (for anonymisation & FE), strings are cut to this size
MAXIMAL_STRING_SIZE = 20_000
# Determines whether to run end-to-end test or not
RUN_E2E_TEST = False
# Determines whether to run Feature Extraction
RUN_FEATURE_EXTRACTION = False
# Skip tests with vacuum method
SKIP_VACUUM_TEST = True
# Perform data frames partitioning
#   Note: it increases performance, but also instability, default configuration
#   makes algorithm practically useless (as upsert does not work).
DATAFRAME_PARTITIONING = False
# Determines if to run free text anonymisation or just remove columns.
RUN_FREE_TEXT_ANONYMISATION = True
# Determines whether to write into Unity Catalogue or not
WRITE_INTO_UNITY_CATALOGUE = False

# Skip steps (use only if you know what is going on):
SKIP_BRONZE = False
SKIP_SILVER = False
SKIP_GOLD = False

# Entities to anonymise from free text (input for Presidio)
PII_ENTITIES = [
    "PERSON",
    "LOCATION",
    "DATE_TIME",
    "EMAIL_ADDRESS",
    "URL",
    "PHONE_NUMBER",
]

# Labels provided by the NLP model (e. g. en_ner_bc5cdr_md):
FEATURE_EXTRACTION_LABELS = [
    "DISEASE",
    "CHEMICAL",
]
