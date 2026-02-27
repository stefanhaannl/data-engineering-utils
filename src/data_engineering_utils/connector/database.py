from sqlalchemy import create_engine

from .base import Connection

from abc import abstractmethod
from adbc_driver_postgresql import dbapi
import polars as pl

from trino.dbapi import connect
from trino.auth import OAuth2Authentication
from sqlalchemy import text


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

    def _quote_ident(self, name: str) -> str:
        # SQL Server identifier quoting: [name] with escaping of closing bracket
        return "[" + name.replace("]", "]]") + "]"

    def _mssql_type_for_polars_dtype(self, dtype: pl.DataType) -> str:
        # Map Polars dtypes to SQL Server types (adjust if you prefer different defaults)
        if dtype == pl.Boolean:
            return "BIT"
        if dtype in (pl.Int8, pl.Int16, pl.Int32):
            return "INT"
        if dtype == pl.Int64:
            return "BIGINT"
        if dtype in (pl.UInt8, pl.UInt16, pl.UInt32):
            return "INT"  # or BIGINT if you expect values > 2,147,483,647
        if dtype == pl.UInt64:
            return "BIGINT"  # caveat: SQL Server BIGINT is signed
        if dtype == pl.Float32:
            return "REAL"
        if dtype == pl.Float64:
            return "FLOAT(53)"
        if dtype == pl.Decimal:
            # If you keep decimals, choose precision/scale; otherwise cast to Float64 beforehand.
            return "DECIMAL(38, 18)"
        if dtype == pl.Date:
            return "DATE"
        if isinstance(dtype, pl.Datetime):
            # Critical fix: use DATETIME2, not TIMESTAMP (ROWVERSION)
            return "DATETIME2(6)"
        if isinstance(dtype, pl.Duration):
            return "BIGINT"  # store duration as integer (e.g., microseconds); customize as needed
        if dtype == pl.Time:
            return "TIME(6)"
        if dtype == pl.Utf8:
            return "VARCHAR(MAX)"
        if dtype == pl.Binary:
            return "VARBINARY(MAX)"

        # Complex / unsupported types: store as text (e.g., JSON) by default
        if isinstance(dtype, (pl.List, pl.Struct, pl.Array, pl.Object, pl.Categorical, pl.Enum)):
            return "VARCHAR(MAX)"

        # Fallback
        return "VARCHAR(MAX)"

    def _create_table_ddl(self, schema_name: str, table_name: str, df: pl.DataFrame) -> str:
        schema_q = self._quote_ident(schema_name)
        table_q = self._quote_ident(table_name)
        full_q = f"{schema_q}.{table_q}"

        cols_sql = []
        for col, dtype in df.schema.items():
            col_q = self._quote_ident(col)
            sql_type = self._mssql_type_for_polars_dtype(dtype)
            cols_sql.append(f"    {col_q} {sql_type} NULL")

        cols_block = ",\n".join(cols_sql)

        # Use OBJECT_ID with quoted 2-part name; schema/table are user-provided so keep it consistent
        obj_name = f"{schema_name}.{table_name}".replace("'", "''")

        return f"""
    IF OBJECT_ID(N'{obj_name}', N'U') IS NOT NULL
        DROP TABLE {full_q};

    CREATE TABLE {full_q} (
    {cols_block}
    );
    """.strip()

    def load_table(self, schema_name: str, table_name: str, dataframe: pl.DataFrame):
        # Keep your existing decimal handling (optional)
        dataframe = dataframe.select([
            pl.col(c).cast(pl.Float64) if dataframe[c].dtype == pl.Decimal else pl.col(c)
            for c in dataframe.columns
        ])

        ddl = self._create_table_ddl(schema_name, table_name, dataframe)

        # 1) drop/create with correct types (DATETIME2 for Polars Datetime)
        with self.connection.begin() as conn:
            conn.execute(text(ddl))

        # 2) append data (Polars will not try to CREATE TABLE)
        dataframe.write_database(
            f"{schema_name}.{table_name}",
            connection=self.connection,
            if_table_exists="append",
        )


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
        df = pl.DataFrame(rows, schema=columns, orient="row", infer_schema_length=10_000,)
        return df

    def extract_table(self, schema_name: str, table_name: str) -> pl.DataFrame:
        return self._read_query(f"SELECT * FROM {schema_name}.{table_name}")

    def load_table(self, schema_name: str, table_name: str, dataframe: pl.DataFrame):
        raise NotImplementedError(
            "Writing to Trino depends on the configured catalog (Iceberg/Hive/etc). "
            "Implement via CTAS/INSERT for your specific catalog, or write directly "
            "to the underlying storage."
        )
