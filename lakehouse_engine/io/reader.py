"""Defines abstract reader behaviour."""
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import InputSpec
from lakehouse_engine.utils.logging_handler import LoggingHandler


class Reader(ABC):
    """Abstract Reader class."""

    def __init__(self, input_spec: InputSpec):
        """Construct Reader instances.

        Args:
            input_spec: input specification for reading data.
        """
        self._logger = LoggingHandler(self.__class__.__name__).get_logger()
        self._input_spec = input_spec

    @abstractmethod
    def read(self) -> DataFrame:
        """Abstract read method.

        Returns:
            A dataframe read according to the input specification.
        """
        raise NotImplementedError
