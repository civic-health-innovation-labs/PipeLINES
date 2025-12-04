from azure.identity import ClientSecretCredential
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
)

from pyspark.sql.session import SparkSession


def copy_latest_zip_file(
    source_storage_account_name: str,
    source_container_name: str,
    destination_storage_account_name: str,
    destination_container_name: str,
    destination_file_name: str,
    container_client_id: str,
    container_tenant_id: str,
    container_client_secret: str,
) -> None:
    """Copy the latest ZIP file from the landing zone to the temporary zone for unzipping."""
    # Authenticate to Blob Storage
    credential = ClientSecretCredential(
        container_tenant_id, container_client_id, container_client_secret
    )
    source_account_url = f"https://{source_storage_account_name}.blob.core.windows.net"
    source_blob_service_client = BlobServiceClient(
        account_url=source_account_url, credential=credential
    )
    source_container_client = source_blob_service_client.get_container_client(
        source_container_name
    )
    # List all blobs
    all_files = [_blob.name for _blob in source_container_client.list_blobs()]
    zip_files = [_fname for _fname in all_files if _fname.lower().endswith(".zip")]
    zip_files = sorted(zip_files, reverse=True)

    # Find the latest ZIP file (or exception if it does not exist)
    latest_zip_name: str = zip_files[0]

    # Upload with chunk (default 4MiB size)
    source_blob_url = source_account_url + f"/{source_container_name}/{latest_zip_name}"
    destination_account_url = (
        f"https://{destination_storage_account_name}.blob.core.windows.net"
    )
    destination_blob_url = (
        f"{destination_account_url}/{destination_container_name}/"
        f"{destination_file_name}"
    )
    _source = BlobClient.from_blob_url(source_blob_url, credential=credential)
    _destination = BlobClient.from_blob_url(destination_blob_url, credential=credential)

    stream = _source.download_blob()
    _destination.upload_blob(data=stream.chunks(), overwrite=True)


if __name__ == "__main__":
    _spark = SparkSession.builder.getOrCreate()
    # Get variables
    source_storage_account_name = _spark.conf.get(
        "spark.secret.landing-zip-storage-account"
    )
    source_container_name = _spark.conf.get(
        "spark.secret.landing-zip-storage-container"
    )
    destination_storage_account_name = _spark.conf.get(
        "spark.secret.pre-bronze-storage-account"
    )
    destination_container_name = _spark.conf.get(
        "spark.secret.pre-bronze-zipped-bak-dataset-container"
    )
    destination_file_name = _spark.conf.get(
        "spark.secret.pre_bronze_zipped_bak_dataset_file_name"
    )
    # Configure client App registration that can access the storage account
    container_client_id = _spark.conf.get("spark.secret.pre-bronze-client-id")
    container_tenant_id = _spark.conf.get("spark.secret.pre-bronze-tenant")
    container_client_secret = _spark.conf.get("spark.secret.pre-bronze-client-secret")
    # Run the logic
    copy_latest_zip_file(
        source_storage_account_name,
        source_container_name,
        destination_storage_account_name,
        destination_container_name,
        destination_file_name,
        container_client_id,
        container_tenant_id,
        container_client_secret,
    )
