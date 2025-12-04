from datetime import datetime, timezone, timedelta

from azure.identity import ClientSecretCredential
from azure.storage.blob import (
    BlobServiceClient,
    generate_blob_sas,
    BlobSasPermissions,
)

from pyspark.sql.session import SparkSession


def restore_bak_to_sql_managed_instance(
    spark_session,
    mi_tenant_id,
    mi_client_id,
    mi_client_secret,
    mi_server,
    mi_target_database_name,
    bak_storage_account,
    bak_storage_container,
    bak_filename,
    bak_tenant_id,
    bak_client_id,
    bak_client_secret,
):
    """Unloads the BAK file whose access is authenticated by one App Registration
    into the SQL Server Managed Instance which access is authenticated using another App Reg.

    Args:
        spark_session: Spark Session to commit SQL Quires.
        mi_tenant_id: Tenant where App Registration is located (for MI access).
        mi_client_id: Client ID of App Registration (for MI access).
        mi_client_secret: Client Secret of App Registration (for MI access).
        mi_server: FQDN of the SQL Server Managed Instance.
        mi_target_database_name: Target database name for unloading of the BAK file.

        bak_storage_account: Storage Account name where the BAK file is stored.
        bak_storage_container: Storage Account Container name where the BAK file is stored.
        bak_filename: Full path to the file (without URL of the Storage Account).
        bak_tenant_id: Tenant where App Registration is located (for BAK access authentication).
        bak_client_id: Client ID of App Registration (for BAK access authentication).
        bak_client_secret: Client Secret of App Registration (for BAK access authentication).
    """
    # Generate SAS for BAK file
    bak_credential = ClientSecretCredential(
        bak_tenant_id, bak_client_id, bak_client_secret
    )
    bak_account_url = f"https://{bak_storage_account}.blob.core.windows.net"
    bak_blob_service_client = BlobServiceClient(
        account_url=bak_account_url, credential=bak_credential
    )

    bak_sas = generate_blob_sas(
        account_name=bak_storage_account,
        container_name=bak_storage_container,
        blob_name=bak_filename,
        user_delegation_key=bak_blob_service_client.get_user_delegation_key(
            key_start_time=datetime.now(timezone.utc) - timedelta(hours=2),
            key_expiry_time=datetime.now(timezone.utc) + timedelta(hours=8),
        ),
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(hours=8),
    )
    bak_full_blob_uri = f"{bak_account_url}/{bak_storage_container}/{bak_filename}"
    bak_credentials_uri = f"{bak_account_url}/{bak_storage_container}"

    # 3) Acquire an access token for Azure SQL
    mi_credential = ClientSecretCredential(mi_tenant_id, mi_client_id, mi_client_secret)
    mi_access_token = mi_credential.get_token(
        "https://database.windows.net/.default"
    ).token

    # 4) JDBC URL pointed at the 'master' database (required for RESTORE)
    jdbc_url = (
        f"jdbc:sqlserver://{mi_server}:1433;"
        "database=master;"
        "encrypt=true;"
        "trustServerCertificate=false;"
        f"hostNameInCertificate={mi_server}"
    )

    # 5) Build Java Properties for the connection
    #    (we have to go through the JVM via spark._jvm)
    props = spark_session._jvm.java.util.Properties()
    props.setProperty("accessToken", mi_access_token)
    props.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    # 6) Open a JDBC connection
    conn = spark_session._jvm.java.sql.DriverManager.getConnection(jdbc_url, props)

    # 7) Create a statement and run your RESTORE command
    stmt = conn.createStatement()
    restore_sql = (
        r"""
    IF EXISTS (SELECT 1 FROM sys.credentials
            WHERE name = N'"""
        + bak_credentials_uri
        + """')
    DROP CREDENTIAL ["""
        + bak_credentials_uri
        + """];

    IF EXISTS (
        SELECT 1
        FROM   master.sys.databases
        WHERE  name = N'"""
        + mi_target_database_name
        + r"""'
    )
    BEGIN
        DROP DATABASE ["""
        + mi_target_database_name
        + r"""];
    END

    CREATE CREDENTIAL ["""
        + bak_credentials_uri
        + """]
    WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
            SECRET = N'"""
        + bak_sas
        + r"""';

    RESTORE DATABASE ["""
        + mi_target_database_name
        + r"""]
    FROM URL = '"""
        + bak_full_blob_uri
        + "';"
    )
    # restore_sql = "SELECT @@VERSION AS 'SQL Server Version';"

    stmt.execute(restore_sql)

    # 8) Cleanup
    stmt.close()
    conn.close()


def get_latest_bak_file_path(
    bak_storage_account,
    bak_storage_container,
    destination_folder_path,
    bak_tenant_id,
    bak_client_id,
    bak_client_secret,
) -> str:
    """Get the full path to the latest BAK file (path without Storage Account URL).
    Args:
        bak_storage_account: Storage Account name where the BAK file is stored.
        bak_storage_container: Storage Account Container name where the BAK file is stored.
        destination_folder_path: Prefix for the name (folder where BAK is unloaded).
        bak_tenant_id: Tenant where App Registration is located (for BAK access authentication).
        bak_client_id: Client ID of App Registration (for BAK access authentication).
        bak_client_secret: Client Secret of App Registration (for BAK access authentication).
    Returns:
        str: Full path without Storage Account URL (e.g. 'lovely/whatever/something.bak').
    Raises:
        IndexError: If the folder is empty.
    """
    # Authenticate to Blob Storage
    credential = ClientSecretCredential(bak_tenant_id, bak_client_id, bak_client_secret)
    bak_account_url = f"https://{bak_storage_account}.blob.core.windows.net"
    bak_blob_service_client = BlobServiceClient(
        account_url=bak_account_url, credential=credential
    )
    bak_container_client = bak_blob_service_client.get_container_client(
        bak_storage_container
    )
    # List all blobs
    all_files = [
        _blob.name
        for _blob in bak_container_client.list_blobs(
            name_starts_with=destination_folder_path
        )
    ]
    bak_files = [_fname for _fname in all_files if _fname.lower().endswith(".bak")]
    bak_files = sorted(bak_files, reverse=True)

    # Find the latest ZIP file (or exception if it does not exist)
    correct_bak_file: str = bak_files[0]
    return correct_bak_file


if __name__ == "__main__":
    _spark = SparkSession.builder.getOrCreate()

    mi_tenant_id = _spark.conf.get("spark.secret.managed-instance-app-tenant")
    mi_client_id = _spark.conf.get("spark.secret.managed-instance-app-client-id")
    mi_client_secret = _spark.conf.get(
        "spark.secret.managed-instance-app-client-secret"
    )

    mi_server = _spark.conf.get("spark.secret.managed-instance-fqdn")
    mi_target_database_name = _spark.conf.get(
        "spark.secret.managed-instance-database-name"
    )

    bak_storage_account = _spark.conf.get("spark.secret.pre-bronze-storage-account")
    bak_storage_container = _spark.conf.get(
        "spark.secret.pre-bronze-unzipped-bak-dataset-container"
    )
    destination_folder_path = _spark.conf.get(
        "spark.secret.pre-bronze-unzipped-bak-dataset-folder-path"
    )

    bak_client_id = _spark.conf.get("spark.secret.pre-bronze-client-id")
    bak_tenant_id = _spark.conf.get("spark.secret.pre-bronze-tenant")
    bak_client_secret = _spark.conf.get("spark.secret.pre-bronze-client-secret")

    bak_filename = get_latest_bak_file_path(
        bak_storage_account,
        bak_storage_container,
        destination_folder_path,
        bak_tenant_id,
        bak_client_id,
        bak_client_secret,
    )

    restore_bak_to_sql_managed_instance(
        _spark,
        mi_tenant_id,
        mi_client_id,
        mi_client_secret,
        mi_server,
        mi_target_database_name,
        bak_storage_account,
        bak_storage_container,
        bak_filename,
        bak_tenant_id,
        bak_client_id,
        bak_client_secret,
    )
