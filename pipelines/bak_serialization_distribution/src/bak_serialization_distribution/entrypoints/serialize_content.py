from datetime import timezone, datetime
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, BlobClient

from pyspark.sql.session import SparkSession


def serialize_content(
    spark,
    # Database
    server: str,
    database: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    # Storage account
    storage_temp_account_name: str,
    storage_temp_client_tenant_id: str,
    storage_temp_client_client_id: str,
    storage_temp_client_secret: str,
    storage_temp_container: str,
    storage_destination_urls: list[str],
    latest_folder_path: str = "latest",
    archive_folder_path: str = "archive",
    *,
    perform_spark_authentication: bool = False
):
    """Serialize the whole Microsoft SQL Database content into .parquet files.

    Args:
        spark: Spark Session instance.
        server (str): SQL server FQDN.
        database (str): SQL server Database Name.
        tenant_id (str): App Registration Tenant ID for MS-SQL Server access.
        client_id (str): App Registration Client (App) ID for MS-SQL Server access.
        client_secret (str): App Registration secret value for MS-SQL Server access.
        storage_temp_account_name (str): Temporary Storage Account name for Parquet serialization.
        storage_temp_client_tenant_id (str): App Registration Tenant ID for Storage Account access.
        storage_temp_client_client_id (str): App Registration Client (App) ID for Storage Account access.
        storage_temp_client_secret (str): App Registration secret value for Storage Account access.
        storage_temp_container (str): Temporary Storage Account container for Parquet serialization.
        storage_destination_urls (list[str]): List of URLs of destinations.
        latest_folder_path (str): Name of the folder for the latest files (default latest).
        archive_folder_path (str): Name of the folder for the archive of files (default archive).
        perform_spark_authentication (bool): To use default credentials for temp container.
    """
    if perform_spark_authentication:
        # Write to Storage
        spark.conf.set(
            f"fs.azure.account.auth.type.{storage_temp_account_name}.dfs.core.windows.net", "OAuth"
        )
        spark.conf.set(
            f"fs.azure.account.oauth.provider.type.{storage_temp_account_name}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.id.{storage_temp_account_name}.dfs.core.windows.net",
            storage_temp_client_client_id,
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.secret.{storage_temp_account_name}.dfs.core.windows.net",
            storage_temp_client_secret,
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{storage_temp_account_name}.dfs.core.windows.net",
            f"https://login.microsoftonline.com/{storage_temp_client_tenant_id}/oauth2/token",
        )

    # 3) Acquire an access token for Azure SQL
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    access_token = credential.get_token("https://database.windows.net/.default").token

    # 3. JDBC URL
    jdbc_url = (
        f"jdbc:sqlserver://{server}:1433;"
        f"database={database};"
        "encrypt=true;"
        "trustServerCertificate=false;"
        f"hostNameInCertificate={server}"
    )

    # 4. Connection properties
    connection_properties = {
        "accessToken": access_token,
        # "authentication"         : "ActiveDirectoryAccessToken",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }

    # 5. READ ALL TABLES:
    df_all_tables = spark.read.jdbc(
        url=jdbc_url, table="INFORMATION_SCHEMA.TABLES", properties=connection_properties
    )
    tbl_schemas: list[str] = (
        df_all_tables.select("TABLE_SCHEMA").rdd.flatMap(lambda x: x).collect()
    )
    tbl_names: list[str] = df_all_tables.select("TABLE_NAME").rdd.flatMap(lambda x: x).collect()

    iso_now: str = datetime.now(timezone.utc).isoformat()
    full_table_names: list[str] = []
    # Parse every single table
    for tbl_sch, tbl_nm in zip(tbl_schemas, tbl_names):
        df_table = spark.read.jdbc(
            url=jdbc_url, table=f"{tbl_sch}.{tbl_nm}", properties=connection_properties
        )
        table_name_underscore = f"{tbl_sch}_{tbl_nm}"

        df_table.coalesce(1).write.mode("overwrite").parquet(
            f"abfss://{storage_temp_container}@{storage_temp_account_name}"
            f".dfs.core.windows.net/{iso_now}/{table_name_underscore}"
        )
        full_table_names.append(table_name_underscore)

    credential = ClientSecretCredential(
        storage_temp_client_tenant_id, storage_temp_client_client_id, storage_temp_client_secret
    )
    source_account_url = f"https://{storage_temp_account_name}.blob.core.windows.net"
    source_blob_service_client = BlobServiceClient(
        account_url=source_account_url, credential=credential
    )
    source_container_client = source_blob_service_client.get_container_client(
        storage_temp_container
    )
    # List all blobs
    blob_files: list[str] = [
        _blob.name
        for _blob in source_container_client.list_blobs()
        if _blob.name.startswith(iso_now) and _blob.name.endswith(".parquet")
    ]
    for full_table_name in full_table_names:
        # Find the right pass
        blob_file: str = [
            _bf for _bf in blob_files if _bf.startswith(f"{iso_now}/{full_table_name}/")
        ][0]

        # Upload with chunk (default 4MiB size)
        source_blob_url = source_account_url + f"/{storage_temp_container}/{blob_file}"
        for storage_destination_url in storage_destination_urls:
            destination_latest_blob_url = (
                f"{storage_destination_url}/{latest_folder_path}/{full_table_name}.parquet"
            )
            _source_latest = BlobClient.from_blob_url(source_blob_url, credential=credential)
            _destination_latest = BlobClient.from_blob_url(
                destination_latest_blob_url, credential=credential
            )

            _stream_latest = _source_latest.download_blob()
            _destination_latest.upload_blob(data=_stream_latest.chunks(), overwrite=True)

            destination_archive_blob_url = f"{storage_destination_url}/{archive_folder_path}/{iso_now}/{full_table_name}.parquet"
            _source_archive = BlobClient.from_blob_url(source_blob_url, credential=credential)
            _destination_archive = BlobClient.from_blob_url(
                destination_archive_blob_url, credential=credential
            )

            _stream_archive = _source_archive.download_blob()
            _destination_archive.upload_blob(data=_stream_archive.chunks(), overwrite=True)


if __name__ == "__main__":
    _spark = SparkSession.builder.getOrCreate()

    serialize_content(
        spark=_spark,
        # Database
        database=_spark.conf.get("spark.secret.managed-instance-database-name"),
        server=_spark.conf.get("spark.secret.managed-instance-fqdn"),
        tenant_id=_spark.conf.get("spark.secret.managed-instance-app-tenant"),
        client_id=_spark.conf.get("spark.secret.managed-instance-app-client-id"),
        client_secret=_spark.conf.get("spark.secret.managed-instance-app-client-secret"),
        # Storage account
        storage_temp_account_name=_spark.conf.get("spark.secret.serialization-temp-account-name"),
        storage_temp_container=_spark.conf.get(
            "spark.secret.serialization-temp-account-container"
        ),
        
        storage_temp_client_tenant_id=_spark.conf.get("spark.secret.pre-bronze-tenant"),
        storage_temp_client_client_id=_spark.conf.get("spark.secret.pre-bronze-client-id"),
        storage_temp_client_secret=_spark.conf.get("spark.secret.pre-bronze-client-secret"),
        
        storage_destination_urls=_spark.conf.get(
            "spark.secret.serialization-destination-urls"
        ).split("|"),
        
        latest_folder_path="latest",
        archive_folder_path="archive",
    )
