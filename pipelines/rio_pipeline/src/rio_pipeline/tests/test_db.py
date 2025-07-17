from sqlalchemy import CursorResult, create_engine, text
from testcontainers.mssql import SqlServerContainer  # type: ignore


def test_mssql_connection(mssql_container: SqlServerContainer) -> None:
    with create_engine(mssql_container.get_connection_url()).connect() as conn:
        result: CursorResult = conn.execute(text("select @@servicename"))
        row = result.fetchone()

        assert row is not None
        assert row[0] == "MSSQLSERVER"
        assert mssql_container.get_logs() != ""
