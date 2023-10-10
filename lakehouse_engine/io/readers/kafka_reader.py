"""Module to define behaviour to read from Kafka."""
from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import InputFormat, InputSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.reader import Reader


class KafkaReader(Reader):
    """Class to read from Kafka."""

    def __init__(self, input_spec: InputSpec):
        """Construct KafkaReader instances.

        Args:
            input_spec: input specification.
        """
        super().__init__(input_spec)

    def read(self) -> DataFrame:
        """Read Kafka data.

        Returns:
            A dataframe containing the data from Kafka.
        """
        df = ExecEnv.SESSION.readStream.load(
            format=InputFormat.KAFKA.value,
            **self._input_spec.options if self._input_spec.options else {}
        )

        return df
