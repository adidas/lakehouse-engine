"""Condensers module."""
from typing import Callable, List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number

from lakehouse_engine.transformers.exceptions import (
    UnsupportedStreamingTransformerException,
    WrongArgumentsException,
)
from lakehouse_engine.utils.logging_handler import LoggingHandler


class Condensers(object):
    """Class containing all the functions to condensate data for later merges."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def condense_record_mode_cdc(
        cls,
        business_key: List[str],
        record_mode_col: str,
        valid_record_modes: List[str],
        ranking_key_desc: Optional[List[str]] = None,
        ranking_key_asc: Optional[List[str]] = None,
    ) -> Callable:
        """Condense Change Data Capture (CDC) based on record_mode strategy.

        This CDC data is particularly seen in some CDC enabled systems. Other systems
        may have different CDC strategies.

        Args:
            business_key: The business key (logical primary key) of the data.
            ranking_key_desc: In this type of CDC condensation the data needs to be
                ordered descendingly in a certain way, using columns specified in this
                parameter.
            ranking_key_asc: In this type of CDC condensation the data needs to be
                ordered ascendingly in a certain way, using columns specified in
                this parameter.
            record_mode_col: Name of the record mode input_col.
            valid_record_modes: Depending on the context, not all record modes may be
                considered for condensation. Use this parameter to skip those.

        Returns:
            A function to be executed in the .transform() spark function.
        """
        if not ranking_key_desc and not ranking_key_asc:
            raise WrongArgumentsException(
                "The condense_record_mode_cdc transfomer requires data to be ordered"
                "either descendingly or ascendingly, but no arguments for ordering"
                "were provided."
            )

        def inner(df: DataFrame) -> DataFrame:
            if not df.isStreaming:
                partition_window = Window.partitionBy(
                    [col(c) for c in business_key]
                ).orderBy(
                    [
                        col(c).desc()
                        for c in (ranking_key_desc if ranking_key_desc else [])
                    ]  # type: ignore
                    + [
                        col(c).asc()
                        for c in (ranking_key_asc if ranking_key_asc else [])
                    ]  # type: ignore
                )

                return (
                    df.withColumn("ranking", row_number().over(partition_window))
                    .filter(
                        col(record_mode_col).isNull()
                        | col(record_mode_col).isin(valid_record_modes)
                    )
                    .filter(col("ranking") == 1)
                    .drop("ranking")
                )
            else:
                raise UnsupportedStreamingTransformerException(
                    "Transformer condense_record_mode_cdc is not supported in "
                    "streaming mode."
                )

        return inner

    @classmethod
    def group_and_rank(
        cls, group_key: List[str], ranking_key: List[str], descending: bool = True
    ) -> Callable:
        """Condense data based on a simple group by + take latest mechanism.

        Args:
            group_key: list of column names to use in the group by.
            ranking_key: the data needs to be ordered descendingly using columns
                specified in this parameter.
            descending: if the ranking considers descending order or not. Defaults to
                True.

        Returns:
            A function to be executed in the .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            if not df.isStreaming:
                partition_window = Window.partitionBy(
                    [col(c) for c in group_key]
                ).orderBy(
                    [
                        col(c).desc() if descending else col(c).asc()
                        for c in (ranking_key if ranking_key else [])
                    ]  # type: ignore
                )

                return (
                    df.withColumn("ranking", row_number().over(partition_window))
                    .filter(col("ranking") == 1)
                    .drop("ranking")
                )
            else:
                raise UnsupportedStreamingTransformerException(
                    "Transformer group_and_rank is not supported in streaming mode."
                )

        return inner
