import abc
from enum import Enum
from dataclasses import dataclass

from pyspark.sql import SparkSession


class AuthenticationMethod(Enum):
    APP_REGISTRATION = "APP_REGISTRATION"
    SERVER_AUTHENTICATION = "SERVER_AUTHENTICATION"


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
        trust_server_certificate (bool): If true, trust server certificate (SSL).
    """

    host: str
    port: int
    database: str
    # The following is for AuthenticationMethod.SERVER_AUTHENTICATION
    user: str = "EMPTY"
    password: str = "EMPTY"

    driver: str | None = None
    trust_server_certificate: bool = True

    authentication_method: AuthenticationMethod = (
        AuthenticationMethod.SERVER_AUTHENTICATION
    )
    # The following is for AuthenticationMethod.APP_REGISTRATION
    app_registration_client_id: str = "EMPTY"
    app_registration_client_secret: str = "EMPTY"
    app_registration_tenant_id: str = "EMPTY"

    @property
    def smart_username(self):
        if self.authentication_method == AuthenticationMethod.APP_REGISTRATION:
            return self.app_registration_client_id
        return self.user

    @property
    def smart_password(self):
        if self.authentication_method == AuthenticationMethod.APP_REGISTRATION:
            return self.app_registration_client_secret
        return self.password

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
            connection_url += r";trustServerCertificate=true"
        else:
            connection_url += (
                f";trustServerCertificate=false;hostNameInCertificate={self.host}"
            )

        return connection_url


def rio_config(spark: SparkSession) -> DatabaseConfiguration:
    """Create a database connection based on environmental variables passed.

    Args:
        spark (SparkSession): Session with variables (Spark's secrets):
            1. rio-tre-fqdn: Contains hostname.
            2. rio-tre-database: Contains database name.
            3. rio-tre-username: Username for DB connection.
            4. rio-tre-password: Password for DB connection.

    Returns:
        DatabaseConfiguration: Created SQL Server connection.
    """
    authentication_method_str = spark.conf.get("spark.secret.rio-authentication_method")
    host = spark.conf.get("spark.secret.rio-database-fqdn")
    database = spark.conf.get("spark.secret.rio-database-database")
    trust_server_certificate = bool(
        spark.conf.get("spark.secret.rio-database-trust-server-certificate") == "True"
    )
    authentication_method = AuthenticationMethod.SERVER_AUTHENTICATION
    user = "EMPTY"
    password = "EMPTY"
    app_registration_client_id = "EMPTY"
    app_registration_client_secret = "EMPTY"
    app_registration_tenant_id = "EMPTY"
    driver = None
    if authentication_method_str == "SERVER_AUTHENTICATION":
        user = spark.conf.get("spark.secret.rio-database-username")
        password = spark.conf.get("spark.secret.rio-database-password")
        authentication_method = AuthenticationMethod.SERVER_AUTHENTICATION
    elif authentication_method_str == "APP_REGISTRATION":
        driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        app_registration_client_id = spark.conf.get(
            "spark.secret.rio-app-registration-client-id"
        )
        app_registration_client_secret = spark.conf.get(
            "spark.secret.rio-app-registration-client-secret"
        )
        app_registration_tenant_id = spark.conf.get(
            "spark.secret.rio-app-registration-tenant-id"
        )
        authentication_method = AuthenticationMethod.APP_REGISTRATION
    else:
        raise RuntimeError("unsupported SQL Server authentication method")

    # Configuration of the connection to SQL Server
    sql_config = SqlServerConfiguration(
        host=host,
        port=1433,
        database=database,
        driver=driver,  # Use the default driver.
        trust_server_certificate=trust_server_certificate,
        authentication_method=authentication_method,
        # Relevant for Auth Method SQL Credentials
        user=user,
        password=password,
        # Relevant for Auth Method APP REG
        app_registration_client_id=app_registration_client_id,
        app_registration_client_secret=app_registration_client_secret,
        app_registration_tenant_id=app_registration_tenant_id,
    )
    return sql_config
