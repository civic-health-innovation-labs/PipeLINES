import logging
import argparse
from base64 import b64decode as base64_decode_byte_array
import datetime
import uuid
import json

from pyspark.sql.session import SparkSession

LOGGER = logging.getLogger("DatasetProvisioning")


def get_request_details():
    """Get the arguments for running the script from the command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--query_base64",
        type=str,
        help="List of tables and columns to be provisioned. Technically a base64 encoded "
        'mapping with logic table_name -> {"columns" -> [col_1, ...], "where_statement" -> '
        '"condition" | None} encoded as JSON string',
    )
    parser.add_argument(
        "--workspace_uuid",
        type=str,
        help="UUID of the workspace where data are provisioned.",
    )

    return parser.parse_args()


def set_spark_auth_variables_for_storage_account(
    session: SparkSession, storage_account: str
):
    """Set up all authentication variables of the Spark to be capable to communicate with the
        storage account.
    Args:
        session (SparkSession): Spark session where variables are set.
        storage_account (str): Name of the storage account
    """
    tenant_id = session.conf.get("spark.secret.workspace-tenant-id")

    # Link to TRE
    workspace_app_id = session.conf.get("spark.secret.workspace-app-id")
    workspace_app_secret = session.conf.get("spark.secret.workspace-app-secret")

    spark_session_config_dict: dict[str, str] = {
        f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net": "OAuth",  # noqa
        f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",  # noqa
        f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net": workspace_app_id,  # noqa
        f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net": workspace_app_secret,  # noqa
        f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",  # noqa
    }
    for session_key, session_value in spark_session_config_dict.items():
        session.conf.set(session_key, session_value)


def storage_account_name(workspace_uuid: str) -> str:
    """Get the name of the storage account as an Azure resource.
    Returns:
         str: name of the storage account (how is it called as a resource in Azure Portal)
    """
    return f"stgws{workspace_uuid[-4:]}"


if __name__ == "__main__":
    # Start the new spark session
    spark_session = SparkSession.builder.getOrCreate()
    # Get the parameters for the provisioning
    request_details = get_request_details()

    # --- Validate the incoming parameters ---
    if not request_details.query_base64:
        LOGGER.error("Parameter query_base64 needs to be set")
        exit(1)
    if not request_details.workspace_uuid:
        LOGGER.error("Parameter workspace_uuid needs to be set")
        exit(1)
    try:
        uuid.UUID(request_details.workspace_uuid)
    except ValueError:
        LOGGER.error("Parameter workspace_uuid is not a valid UUID")
        exit(1)
    try:
        base64_decode_byte_array(request_details.query_base64).decode("ascii")
    except BaseException:  # noqa
        LOGGER.error("Parameter query_base64 is not a valid base64 value")
        exit(1)
    try:
        json.loads(
            base64_decode_byte_array(request_details.query_base64).decode("ascii")
        )
    except BaseException:  # noqa
        LOGGER.error("Parameter query_base64 is not a valid JSON value")
        exit(1)
    # ========================================

    # Get the name of the resource for the storage account where data are provided
    storage_account_name_in_azure = storage_account_name(request_details.workspace_id)
    set_spark_auth_variables_for_storage_account(
        spark_session, storage_account_name_in_azure
    )

    # Decode the request
    request_json = base64_decode_byte_array(request_details.query_base64).decode(
        "ascii"
    )
    # Mapping with logic:
    #   table_name -> {"columns" -> [col_1, ...], "where_statement" -> "condition" | None}
    request: dict[str, dict[str, list[str] | str]] = json.loads(request_json)
    # Current date and time (acts as a folder name where data are provided).
    datetime_now_iso = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    # Get the Datalake URI
    datalake_uri = spark_session.conf.get("spark.secret.datalake-uri")

    for table_name, table_def in request.items():
        LOGGER.info(f"provisioning table {table_name}")
        # Get the access to GOLD data lake storage
        provisioned_dataframe = (
            spark_session.read.format("delta")
            .load(f"abfss://gold@{datalake_uri}/{table_name}")
            .select(table_def["columns"])
        )
        if table_def[
            "where_statement"
        ]:  # Applies WHERE statement for query if required
            provisioned_dataframe = provisioned_dataframe.where(
                table_def["where_statement"]
            )
        # Provision data (save a Parquet copy):
        provisioned_dataframe.write.format("parquet").mode("overwrite").save(
            f"abfss://datalake@{storage_account_name_in_azure}.dfs.core.windows.net/"
            f"{table_name}/{datetime_now_iso}"
        )
