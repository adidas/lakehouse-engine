"""Column creators transformers module."""
from typing import Any, Callable, Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, monotonically_increasing_id
from pyspark.sql.types import IntegerType

from lakehouse_engine.transformers.exceptions import (
    UnsupportedStreamingTransformerException,
)
from lakehouse_engine.utils.logging_handler import LoggingHandler


class ColumnCreators(object):
    """Class containing all functions that can create columns to add value."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def with_row_id(
        cls,
        output_col: str = "lhe_row_id",
    ) -> Callable:
        """Create a sequential but not consecutive id.

        Args:
            output_col: optional name of the output column.

        Returns:
            A function to be executed in the .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            if not df.isStreaming:
                return df.withColumn(output_col, monotonically_increasing_id())
            else:
                raise UnsupportedStreamingTransformerException(
                    "Transformer with_row_id is not supported in streaming mode."
                )

        return inner

    @classmethod
    def with_auto_increment_id(
        cls,
        output_col: str = "lhe_row_id",
    ) -> Callable:
        """Create a sequential and consecutive id.

        Args:
            output_col: optional name of the output column.

        Returns:
            A function to be executed in the .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            if not df.isStreaming:
                if len(df.take(1)) == 0:
                    # if df is empty we have to prevent the algorithm from failing
                    return df.withColumn(output_col, lit(None).cast(IntegerType()))
                else:
                    return (
                        df.rdd.zipWithIndex()
                        .toDF()
                        .select(col("_1.*"), col("_2").alias(output_col))
                    )
            else:
                raise UnsupportedStreamingTransformerException(
                    "Transformer with_auto_increment_id is not supported in "
                    "streaming mode."
                )

        return inner

    @classmethod
    def with_literals(
        cls,
        literals: Dict[str, Any],
    ) -> Callable:
        """Create columns given a map of column names and literal values (constants).

        Args:
            Dict[str, Any] literals: map of column names and literal values (constants).

        Returns:
            Callable: A function to be executed in the .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            df_with_literals = df
            for name, value in literals.items():
                df_with_literals = df_with_literals.withColumn(name, lit(value))
            return df_with_literals

        return inner
