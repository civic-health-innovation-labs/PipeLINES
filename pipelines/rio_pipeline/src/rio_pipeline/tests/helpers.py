from dataclasses import dataclass

from rio_pipeline.db import DatabaseConfiguration
from testcontainers.mssql import SqlServerContainer  # type: ignore


@dataclass
class TestSqlServerConfiguration(DatabaseConfiguration):
    @property
    def url(self) -> str:
        return (
            f"jdbc:sqlserver://{self.host}:{self.port};"
            f"database={self.database};"
            "encrypt=false;"
            "trustServerCertificate=true;"
        )


def mssql_configuration(
    container: SqlServerContainer,
) -> TestSqlServerConfiguration:
    return TestSqlServerConfiguration(
        host=container.get_container_host_ip(),
        port=container.get_exposed_port(1433),
        database=container.SQLSERVER_DBNAME,
        user=container.SQLSERVER_USER,
        password=container.SQLSERVER_PASSWORD,
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
    )


def print_logs(mssql_container: SqlServerContainer):
    for line in mssql_container.get_logs()[0].splitlines():
        print(line)
