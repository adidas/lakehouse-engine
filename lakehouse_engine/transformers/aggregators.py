"""Aggregators module."""
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max

from lakehouse_engine.utils.logging_handler import LoggingHandler


class Aggregators(object):
    """Class containing all aggregation functions."""

    _logger = LoggingHandler(__name__).get_logger()

    @staticmethod
    def get_max_value(input_col: str, output_col: str = "latest") -> Callable:
        """Get the maximum value of a given column of a dataframe.

        Args:
            input_col: name of the input column.
            output_col: name of the output column (defaults to "latest").

        Returns:
            A function to be executed in the .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            return df.select(col(input_col)).agg(max(input_col).alias(output_col))

        return inner
