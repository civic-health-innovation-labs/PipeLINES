import abc
from dataclasses import dataclass

from pyspark.sql import SparkSession


@dataclass
class DatabaseConfiguration(abc.ABC):
    """Contains configuration required to connect into SQL Server.

    Arguments:
        host (str): Host location.
        port (int): Port for connection (typically 1433).
        database (str): Database name.
        user (str): Username for connection.
        password (str): Password for connection.
        driver (str): Driver name (not in use).
        use_aad_service_principal_auth (bool): If true, use AAD service principal auth.
        trust_server_certificate (bool): If true, trust server certificate (SSL).
    """

    host: str
    port: int
    database: str
    user: str
    password: str
    driver: str | None
    use_aad_service_principal_auth: bool = False
    trust_server_certificate: bool = True

    @property
    @abc.abstractmethod
    def url(self) -> str:
        """Generates connection URL for the database server.

        Returns:
            str: URL for database server connection.
        """
        raise NotImplementedError()


@dataclass
class SqlServerConfiguration(DatabaseConfiguration):
    """Extension of DatabaseConfiguration for Azure SQL Server."""

    @property
    def url(self) -> str:
        connection_url: str = (
            f"jdbc:sqlserver://{self.host}:{self.port};"
            f"database={self.database};"
            "encrypt=true;"
            "loginTimeout=30"
        )
        if self.trust_server_certificate:
            connection_url += ";trustServerCertificate=true"
        else:
            connection_url += ";trustServerCertificate=false"

        if self.use_aad_service_principal_auth:
            connection_url += ";Authentication=ActiveDirectoryServicePrincipal"

        return connection_url


def generic_pipeline_config(spark: SparkSession) -> DatabaseConfiguration:
    """Create a database connection based on environmental variables passed.

    Args:
        spark (SparkSession): Session with variables (Spark's secrets):
            1. database-fqdn: Contains hostname.
            2. database-database: Contains database name.
            3. database-username: Username for DB connection.
            4. database-password: Password for DB connection.

    Returns:
        DatabaseConfiguration: Created SQL Server connection.
    """
    host = spark.conf.get("spark.secret.database-fqdn")
    database = spark.conf.get("spark.secret.database-database")
    user = spark.conf.get("spark.secret.database-username")
    password = spark.conf.get("spark.secret.database-password")

    if not host or not database or not user or not password:
        raise TypeError(
            "Configuration for accessing database is not set in the config"
        )
    # Configuration of the connection to SQL Server
    sql_config = SqlServerConfiguration(
        host=host,
        port=1433,
        database=database,
        user=user,
        password=password,
        driver=None,  # Use the default driver.
    )
    sql_config.use_aad_service_principal_auth = False
    sql_config.trust_server_certificate = True
    return sql_config
