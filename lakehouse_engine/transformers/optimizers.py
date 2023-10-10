"""Optimizers module."""
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel

from lakehouse_engine.utils.logging_handler import LoggingHandler


class Optimizers(object):
    """Class containing all the functions that can provide optimizations."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def cache(cls) -> Callable:
        """Caches the current dataframe.

        The default storage level used is MEMORY_AND_DISK.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            return df.cache()

        return inner

    @classmethod
    def persist(cls, storage_level: str = None) -> Callable:
        """Caches the current dataframe with a specific StorageLevel.

        Args:
            storage_level: the type of StorageLevel, as default MEMORY_AND_DISK_DESER.
                More options here: https://spark.apache.org/docs/latest/api/python/
                reference/api/pyspark.StorageLevel.html

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            level = getattr(
                StorageLevel, storage_level, StorageLevel.MEMORY_AND_DISK_DESER
            )

            return df.persist(level)

        return inner

    @classmethod
    def unpersist(cls, blocking: bool = False) -> Callable:
        """Removes the dataframe from the disk and memory.

        Args:
            blocking: whether to block until all the data blocks are
                removed from disk/memory or run asynchronously.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            return df.unpersist(blocking)

        return inner
