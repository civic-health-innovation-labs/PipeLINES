from itertools import islice

from azure.identity import ClientSecretCredential
from azure.storage.blob import ContainerClient

from pyspark.sql.session import SparkSession


def delete_container_content(
    app_tenant_id,
    app_client_id,
    app_client_secret,
    account_name,
    container_name,
    path_prefix,
):
    cred = ClientSecretCredential(app_tenant_id, app_client_id, app_client_secret)
    account_url = f"https://{account_name}.blob.core.windows.net"
    container = ContainerClient(account_url, container_name, credential=cred)
    batch_size = 256

    def grouper(iterable, n):
        """Yield lists of length ≤ n from the iterable."""
        it = iter(iterable)
        while chunk := list(islice(it, n)):
            yield chunk

    for chunk in grouper(
        (blob.name for blob in container.list_blobs(name_starts_with=path_prefix)),
        batch_size,
    ):
        # delete_blobs() transparently issues a single batched REST call
        container.delete_blobs(
            *chunk, delete_snapshots="include"  # important if the blobs have snapshots
        )


if __name__ == "__main__":
    _spark = SparkSession.builder.getOrCreate()

    # Configure client App registration that can access the storage account
    container_client_id = _spark.conf.get("spark.secret.pre-bronze-client-id")
    container_tenant_id = _spark.conf.get("spark.secret.pre-bronze-tenant")
    container_client_secret = _spark.conf.get("spark.secret.pre-bronze-client-secret")
    # Where to delete
    destination_storage_account_name = _spark.conf.get(
        "spark.secret.pre-bronze-storage-account"
    )
    destination_container_name = _spark.conf.get(
        "spark.secret.pre-bronze-unzipped-bak-dataset-container"
    )
    destination_folder_path = _spark.conf.get(
        "spark.secret.pre-bronze-unzipped-bak-dataset-folder-path"
    )
    # Run the delete procedure
    delete_container_content(
        container_tenant_id,
        container_client_id,
        container_client_secret,
        destination_storage_account_name,
        destination_container_name,
        destination_folder_path,
    )
