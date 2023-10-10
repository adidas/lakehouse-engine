"""Watermarker module."""
from typing import Callable

from pyspark.sql import DataFrame

from lakehouse_engine.utils.logging_handler import LoggingHandler


class Watermarker(object):
    """Class containing all watermarker transformers."""

    _logger = LoggingHandler(__name__).get_logger()

    @staticmethod
    def with_watermark(watermarker_column: str, watermarker_time: str) -> Callable:
        """Get the dataframe with watermarker defined.

        Args:
            watermarker_column: name of the input column to be considered for
             the watermarking. Note: it must be a timestamp.
            watermarker_time: time window to define the watermark value.

        Returns:
            A function to be executed on other transformers.
        """

        def inner(df: DataFrame) -> DataFrame:
            return df.withWatermark(watermarker_column, watermarker_time)

        return inner
