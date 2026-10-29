"""Module with union transformers."""

from collections.abc import Callable
from functools import reduce

from pyspark.sql import DataFrame

from lakehouse_engine.utils.logging_handler import LoggingHandler


class Unions:
    """Class containing union transformers."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def union(
        cls,
        union_with: list[DataFrame],
        deduplication: bool = True,
    ) -> Callable:
        """Union dataframes, resolving columns by position (not by name).

        Args:
            union_with: list of dataframes to union.
            deduplication: whether to perform deduplication of elements or not.

        Returns:
            A function to be called in .transform() spark function.

        {{get_example(method_name='union')}}
        """

        def inner(df: DataFrame) -> DataFrame:
            union_df = reduce(lambda x, y: x.union(y), [df] + union_with)
            if deduplication and not union_df.isStreaming:
                return union_df.distinct()

            return union_df

        return inner

    @classmethod
    def union_by_name(
        cls,
        union_with: list[DataFrame],
        deduplication: bool = True,
        allow_missing_columns: bool = True,
    ) -> Callable:
        """Union dataframes, resolving columns by name (not by position).

        Args:
            union_with: list of dataframes to union.
            deduplication: whether to perform deduplication of elements or not.
            allow_missing_columns: allow the union of DataFrames with different
                schemas.

        Returns:
            A function to be called in .transform() spark function.

        {{get_example(method_name='union_by_name')}}
        """

        def inner(df: DataFrame) -> DataFrame:
            union_df = reduce(
                lambda x, y: x.unionByName(
                    y, allowMissingColumns=allow_missing_columns
                ),
                [df] + union_with,
            )

            if deduplication and not union_df.isStreaming:
                return union_df.distinct()
            return union_df

        return inner
