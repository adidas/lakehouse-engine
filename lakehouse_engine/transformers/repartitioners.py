"""Module with repartitioners transformers."""
from typing import Callable, List, Optional

from pyspark.sql import DataFrame

from lakehouse_engine.transformers.exceptions import WrongArgumentsException
from lakehouse_engine.utils.logging_handler import LoggingHandler


class Repartitioners(object):
    """Class containing repartitioners transformers."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def coalesce(cls, num_partitions: int) -> Callable:
        """Coalesce a dataframe into n partitions.

        Args:
            num_partitions: num of partitions to coalesce.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            return df.coalesce(num_partitions)

        return inner

    @classmethod
    def repartition(
        cls, num_partitions: Optional[int] = None, cols: Optional[List[str]] = None
    ) -> Callable:
        """Repartition a dataframe into n partitions.

        If num_partitions is provided repartitioning happens based on the provided
        number, otherwise it happens based on the values of the provided cols (columns).

        Args:
            num_partitions: num of partitions to repartition.
            cols: list of columns to use for repartitioning.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            if cols:
                return df.repartition(num_partitions, *cols)
            elif num_partitions:
                return df.repartition(num_partitions)
            else:
                raise WrongArgumentsException(
                    "num_partitions or cols should be specified"
                )

        return inner
