"""Module with null handlers transformers."""
from typing import Callable, List

from pyspark.sql import DataFrame

from lakehouse_engine.utils.logging_handler import LoggingHandler


class NullHandlers(object):
    """Class containing null handler transformers."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def replace_nulls(
        cls,
        replace_on_nums: bool = True,
        default_num_value: int = -999,
        replace_on_strings: bool = True,
        default_string_value: str = "UNKNOWN",
        subset_cols: List[str] = None,
    ) -> Callable:
        """Replace nulls in a dataframe.

        Args:
            replace_on_nums: if it is to replace nulls on numeric columns.
                Applies to ints, longs and floats.
            default_num_value: default integer value to use as replacement.
            replace_on_strings: if it is to replace nulls on string columns.
            default_string_value: default string value to use as replacement.
            subset_cols: list of columns in which to replace nulls. If not
                provided, all nulls in all columns will be replaced as specified.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            if replace_on_nums:
                df = df.na.fill(default_num_value, subset_cols)
            if replace_on_strings:
                df = df.na.fill(default_string_value, subset_cols)

            return df

        return inner
