"""Regex transformers module."""
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_extract

from lakehouse_engine.utils.logging_handler import LoggingHandler


class RegexTransformers(object):
    """Class containing all regex functions."""

    _logger = LoggingHandler(__name__).get_logger()

    @staticmethod
    def with_regex_value(
        input_col: str,
        output_col: str,
        regex: str,
        drop_input_col: bool = False,
        idx: int = 1,
    ) -> Callable:
        """Get the result of applying a regex to an input column (via regexp_extract).

        Args:
            input_col: name of the input column.
            output_col: name of the output column.
            regex: regular expression.
            drop_input_col: whether to drop input_col or not.
            idx: index to return.

        Returns:
             A function to be executed in the .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            df = df.withColumn(output_col, regexp_extract(col(input_col), regex, idx))

            if drop_input_col:
                df = df.drop(input_col)

            return df

        return inner
