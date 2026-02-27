from sqlalchemy import create_engine

from .base import Connection

from abc import abstractmethod
from adbc_driver_postgresql import dbapi
import polars as pl

from trino.dbapi import connect
from trino.auth import OAuth2Authentication


class Database(Connection):

    connection_type = "Database"

    def __init__(self, connection_string: str):
        super().__init__()
        self.connection_string = connection_string

    @abstractmethod
    def _read_query(self, statement: str) -> pl.DataFrame:
        return NotImplemented

    @abstractmethod
    def load_table(self, schema_name: str, table_name: str, dataframe: pl.DataFrame):
        return NotImplemented

    def extract_table(self, schema_name: str, table_name: str) -> pl.DataFrame:
        return self._read_query(f"SELECT * FROM {schema_name}.[{table_name}]")


class Postgres(Database):

    def __init__(self, server: str, database: str, user: str, password: str, port: int = 5432):
        connection_string = f"postgresql://{user}:{password}@{server}:{port}/{database}"
        super().__init__(connection_string)
        self.connection = dbapi.connect(uri=connection_string)

    def _read_query(self, query: str):
        cursor = self.connection.cursor()
        cursor.execute(query)
        arrow = cursor.fetch_arrow_table()
        cursor.close()
        return pl.from_arrow(arrow)

    def load_table(self, schema_name, table_name, dataframe):
        dataframe.write_database(
            table_name=f"{schema_name}.{table_name}",
            connection=self.connection_string,
            engine="adbc"
        )


class Mssql(Database):

    def __init__(self, server: str, database: str = 'master', user: str|None = None, password: str|None = None):

        driver = "ODBC+Driver+18+for+SQL+Server"

        if user and password:
            connection_string = f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}&TrustServerCertificate=yes"
        else:
            connection_string = f"mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver={driver}&TrustServerCertificate=yes"

        super().__init__(connection_string)
        self.connection = create_engine(connection_string)

    def _read_query(self, query):
        return pl.read_database(query=query, connection=self.connection)

    def load_table(self, schema_name, table_name, dataframe):
            dataframe = dataframe.select([pl.col(col).cast(pl.Float64) if dataframe[col].dtype == pl.Decimal else pl.col(col) for col in dataframe.columns])
            table_name = f"{schema_name}.{table_name}"
            dataframe.write_database(table_name, connection=self.connection, if_table_exists="replace")


class Trino(Database):

    def __init__(
        self,
        server: str,
        port: int = 443,
        catalog: str = "iceberg_nessie",
    ):
        connection_string = f"trino://{server}:{port}"
        super().__init__(connection_string)

        self.connection = connect(
            host=server,
            port=port,
            http_scheme="https",
            catalog=catalog,
            auth=OAuth2Authentication(),
        )

    def _read_query(self, query: str) -> pl.DataFrame:
        cur = self.connection.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pl.DataFrame(rows, schema=columns, orient="row", infer_schema_length=10_000)
        return df

    def extract_table(self, schema_name: str, table_name: str) -> pl.DataFrame:
        return self._read_query(f"SELECT * FROM {schema_name}.{table_name}")

    def load_table(self, schema_name: str, table_name: str, dataframe: pl.DataFrame):
        raise NotImplementedError(
            "Writing to Trino depends on the configured catalog (Iceberg/Hive/etc). "
            "Implement via CTAS/INSERT for your specific catalog, or write directly "
            "to the underlying storage."
        )
