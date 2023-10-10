"""Module containing date transformers."""
from datetime import datetime
from typing import Callable, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, lit, to_date, to_timestamp

from lakehouse_engine.utils.logging_handler import LoggingHandler


class DateTransformers(object):
    """Class with set of transformers to transform dates in several forms."""

    _logger = LoggingHandler(__name__).get_logger()

    @staticmethod
    def add_current_date(output_col: str) -> Callable:
        """Add column with current date.

        The current date comes from the driver as a constant, not from every executor.

        Args:
            output_col: name of the output column.

        Returns:
            A function to be executed in the .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            return df.withColumn(output_col, lit(datetime.now()))

        return inner

    @staticmethod
    def convert_to_date(
        cols: List[str], source_format: Optional[str] = None
    ) -> Callable:
        """Convert multiple string columns with a source format into dates.

        Args:
            cols: list of names of the string columns to convert.
            source_format: dates source format (e.g., YYYY-MM-dd). Check here:
                https://docs.oracle.com/javase/10/docs/api/java/time/format/
                DateTimeFormatter.html

        Returns:
            A function to be executed in the .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            converted_df = df
            for c in cols:
                converted_df = converted_df.withColumn(
                    c, to_date(col(c), source_format)
                )

            return converted_df

        return inner

    @staticmethod
    def convert_to_timestamp(
        cols: List[str], source_format: Optional[str] = None
    ) -> Callable:
        """Convert multiple string columns with a source format into timestamps.

        Args:
            cols: list of names of the string columns to convert.
            source_format: dates source format (e.g., MM-dd-yyyy HH:mm:ss.SSS). Check
                here: https://docs.oracle.com/javase/10/docs/api/java/time/format/
                DateTimeFormatter.html

        Returns:
            A function to be executed in the .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            converted_df = df
            for c in cols:
                converted_df = converted_df.withColumn(
                    c, to_timestamp(col(c), source_format)
                )

            return converted_df

        return inner

    @staticmethod
    def format_date(cols: List[str], target_format: Optional[str] = None) -> Callable:
        """Convert multiple date/timestamp columns into strings with the target format.

        Args:
            cols: list of names of the string columns to convert.
            target_format: strings target format (e.g., YYYY-MM-dd). Check here:
                https://docs.oracle.com/javase/10/docs/api/java/time/format/
                DateTimeFormatter.html

        Returns:
            A function to be executed in the .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            converted_df = df
            for c in cols:
                converted_df = converted_df.withColumn(
                    c, date_format(col(c), target_format)
                )

            return converted_df

        return inner

    @staticmethod
    def get_date_hierarchy(cols: List[str], formats: Optional[dict] = None) -> Callable:
        """Create day/month/week/quarter/year hierarchy for the provided date columns.

        Uses Spark's extract function.

        Args:
            cols: list of names of the date columns to create the hierarchy.
            formats: dict with the correspondence between the hierarchy and the format
                to apply.
                Example: {
                    "year": "year",
                    "month": "month",
                    "day": "day",
                    "week": "week",
                    "quarter": "quarter"
                }
                Check here: https://docs.oracle.com/javase/10/docs/api/java/time/format/
                DateTimeFormatter.html

        Returns:
            A function to be executed in the .transform() spark function.
        """
        if not formats:
            formats = {
                "year": "year",
                "month": "month",
                "day": "day",
                "week": "week",
                "quarter": "quarter",
            }

        def inner(df: DataFrame) -> DataFrame:
            transformer_df = df
            for c in cols:
                transformer_df = transformer_df.selectExpr(
                    "*",
                    f"extract({formats['day']} from {c}) as {c}_day",
                    f"extract({formats['month']} from {c}) as {c}_month",
                    f"extract({formats['week']} from {c}) as {c}_week",
                    f"extract({formats['quarter']} from {c}) as {c}_quarter",
                    f"extract({formats['year']} from {c}) as {c}_year",
                )

            return transformer_df

        return inner
