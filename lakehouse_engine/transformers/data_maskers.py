"""Module with data masking transformers."""
from typing import Callable, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import hash, sha2

from lakehouse_engine.transformers.exceptions import WrongArgumentsException
from lakehouse_engine.utils.logging_handler import LoggingHandler


class DataMaskers(object):
    """Class containing data masking transformers."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def hash_masker(
        cls,
        cols: List[str],
        approach: str = "SHA",
        num_bits: int = 256,
        suffix: str = "_hash",
    ) -> Callable:
        """Mask specific columns using an hashing approach.

        Args:
            cols: list of column names to mask.
            approach: hashing approach. Defaults to 'SHA'. There's "MURMUR3" as well.
            num_bits: number of bits of the SHA approach. Only applies to SHA approach.
            suffix: suffix to apply to new column name. Defaults to "_hash".
                Note: you can pass an empty suffix to have the original column replaced.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            masked_df = df
            for col in cols:
                if approach == "MURMUR3":
                    masked_df = masked_df.withColumn(col + suffix, hash(col))
                elif approach == "SHA":
                    masked_df = masked_df.withColumn(col + suffix, sha2(col, num_bits))
                else:
                    raise WrongArgumentsException("Hashing approach is not supported.")

                if suffix and suffix != "":
                    masked_df = masked_df.drop(col)

            return masked_df

        return inner

    @classmethod
    def column_dropper(cls, cols: List[str]) -> Callable:
        """Drop specific columns.

        Args:
            cols: list of column names to drop.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            drop_df = df
            for col in cols:
                drop_df = drop_df.drop(col)

            return drop_df

        return inner
