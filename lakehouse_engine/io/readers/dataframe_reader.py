"""Module to define behaviour to read from dataframes."""

from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import InputSpec
from lakehouse_engine.io.reader import Reader


class DataFrameReader(Reader):
    """Class to read data from a dataframe."""

    def __init__(self, input_spec: InputSpec) -> None:
        """Construct DataFrameReader instances.

        Args:
            input_spec: input specification.
        """
        super().__init__(input_spec)

    def read(self) -> DataFrame:
        """Read data from a dataframe.

        Returns:
            A dataframe containing the data from a dataframe previously
            computed.
        """
        if not self._input_spec.df_name:
            raise ValueError("df_name must be provided for dataframe reader.")
        return self._input_spec.df_name
