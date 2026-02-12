from faker import Faker
import polars as pl


class SyntheticDataGenerator(object):

    def __init__(self, schema: dict[str: str]):
        self.schema = schema
        self.fake = Faker()

    def run(self, row_count: int = 10):
        data = {}
        for column_name, function_name in self.schema.items():
            print(f"Generating data for {column_name}.")
            generator = getattr(self.fake, function_name)
            data[column_name] = [generator() for _ in range(row_count)]
        return pl.DataFrame(data)
