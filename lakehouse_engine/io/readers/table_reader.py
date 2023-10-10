"""Module to define behaviour to read from tables."""
from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import InputSpec, ReadType
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.reader import Reader


class TableReader(Reader):
    """Class to read data from a table."""

    def __init__(self, input_spec: InputSpec):
        """Construct TableReader instances.

        Args:
            input_spec: input specification.
        """
        super().__init__(input_spec)

    def read(self) -> DataFrame:
        """Read data from a table.

        Returns:
            A dataframe containing the data from the table.
        """
        if self._input_spec.read_type == ReadType.BATCH.value:
            return ExecEnv.SESSION.read.options(
                **self._input_spec.options if self._input_spec.options else {}
            ).table(self._input_spec.db_table)
        elif self._input_spec.read_type == ReadType.STREAMING.value:
            return ExecEnv.SESSION.readStream.options(
                **self._input_spec.options if self._input_spec.options else {}
            ).table(self._input_spec.db_table)
        else:
            self._logger.error("The requested read type is not supported.")
            raise NotImplementedError
