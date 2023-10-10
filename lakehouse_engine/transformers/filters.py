"""Module containing the filters transformers."""
from typing import Any, Callable, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from lakehouse_engine.transformers.watermarker import Watermarker
from lakehouse_engine.utils.logging_handler import LoggingHandler


class Filters(object):
    """Class containing the filters transformers."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def incremental_filter(
        cls,
        input_col: str,
        increment_value: Optional[Any] = None,
        increment_df: Optional[DataFrame] = None,
        increment_col: str = "latest",
        greater_or_equal: bool = False,
    ) -> Callable:
        """Incrementally Filter a certain dataframe given an increment logic.

        This logic can either be an increment value or an increment dataframe from which
        the get the latest value from. By default the operator for the filtering process
        is greater or equal to cover cases where we receive late arriving data not cover
        in a previous load. You can change greater_or_equal to false to use greater,
        when you trust the source will never output more data with the increment after
        you have load the data (e.g., you will never load data until the source is still
        dumping data, which may cause you to get an incomplete picture of the last
        arrived data).

        Args:
            input_col: input column name
            increment_value: value to which to filter the data, considering the
                provided input_Col.
            increment_df: a dataframe to get the increment value from.
                you either specify this or the increment_value (this takes precedence).
                This is a good approach to get the latest value from a given dataframe
                that was read and apply that value as filter here. In this way you can
                perform incremental loads based on the last value of a given dataframe
                (e.g., table or file based). Can be used together with the
                get_max_value transformer to accomplish these incremental based loads.
                See our append load feature tests  to see how to provide an acon for
                incremental loads, taking advantage of the scenario explained here.
            increment_col: name of the column from which to get the increment
                value from from (when using increment_df approach). This assumes there's
                only one row in the increment_df, reason why is a good idea to use
                together with the get_max_value transformer. Defaults to "latest"
                because that's the default output column name provided by the
                get_max_value transformer.
            greater_or_equal: if filtering should be done by also including the
                increment value or not (useful for scenarios where you are performing
                increment loads but still want to include data considering the increment
                value, and not only values greater than that increment... examples may
                include scenarios where you already loaded data including those values,
                but the source produced more data containing those values).
                Defaults to false.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            if increment_df:
                if greater_or_equal:
                    return df.filter(  # type: ignore
                        col(input_col) >= increment_df.collect()[0][increment_col]
                    )
                else:
                    return df.filter(  # type: ignore
                        col(input_col) > increment_df.collect()[0][increment_col]
                    )
            else:
                if greater_or_equal:
                    return df.filter(col(input_col) >= increment_value)  # type: ignore
                else:
                    return df.filter(col(input_col) > increment_value)  # type: ignore

        return inner

    @staticmethod
    def expression_filter(exp: str) -> Callable:
        """Filter a dataframe based on an expression.

        Args:
            exp: filter expression.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            return df.filter(exp)  # type: ignore

        return inner

    @staticmethod
    def column_filter_exp(exp: List[str]) -> Callable:
        """Filter a dataframe's columns based on a list of SQL expressions.

        Args:
            exp: column filter expressions.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            return df.selectExpr(*exp)  # type: ignore

        return inner

    @staticmethod
    def drop_duplicate_rows(
        cols: List[str] = None, watermarker: dict = None
    ) -> Callable:
        """Drop duplicate rows using spark function dropDuplicates().

        This transformer can be used with or without arguments.
        The provided argument needs to be a list of columns.
        For example: [“Name”,”VAT”] will drop duplicate records within
        "Name" and "VAT" columns.
        If the transformer is used without providing any columns list or providing
        an empty list, such as [] the result will be the same as using
        the distinct() pyspark function. If the watermark dict is present it will
        ensure that the drop operation will apply to rows within the watermark timeline
        window.


        Args:
            cols: column names.
            watermarker: properties to apply watermarker to the transformer.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            if watermarker:
                df = Watermarker.with_watermark(
                    watermarker["col"], watermarker["watermarking_time"]
                )(df)
            if not cols:
                return df.dropDuplicates()
            else:
                return df.dropDuplicates(cols)

        return inner
