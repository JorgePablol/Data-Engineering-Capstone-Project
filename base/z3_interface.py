from pandas import DataFrame
from typing import Tuple


class z3Interface:

    @staticmethod
    def get_date_range() -> Tuple[str, str]:
        pass

    def perform_query(self) -> DataFrame:
        pass

    def extract(self) -> DataFrame:
        pass

    def _transform_quantitative_indicators(self, df: DataFrame, column: str) -> DataFrame:
        pass

    def _transform_latest_date_comparison(self, df: DataFrame) -> DataFrame:
        pass

    def transform(self, z3_df: DataFrame) -> DataFrame:
        pass

    def load(self):
        pass

    def main(self):
        pass
