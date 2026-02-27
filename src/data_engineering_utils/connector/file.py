from polars import DataFrame

from .base import Connection

import polars as pl

class File(Connection):

    connection_type = "File"

    def __init__(self, path: str):
        super().__init__()
        self.path = path


class CsvFile(File):

    def __init__(self, path: str, separator: str  = "\t"):
        super().__init__(path)
        self.separator = separator

    def load(self, dataframe: pl.DataFrame):
        df = dataframe.with_columns([
            pl.col(pl.Utf8)
              .str.replace_all("\r\n", " ")
              .str.replace_all("\n", " ")
              .str.replace_all("\r", " ")
              .str.replace_all("\t", " ")
        ])

        return df.write_csv(
            self.path,
            separator=self.separator
        )

    def extract(self):
        return pl.read_csv(self.path, separator=self.separator, infer_schema_length=10_000)


class ParquetFile(File):

    def __init__(self, path: str):
        super().__init__(path)

    def load(self, dataframe: DataFrame):
        return dataframe.write_parquet(self.path)

    def extract(self):
        return pl.read_parquet(self.path)
