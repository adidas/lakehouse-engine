"""Module to define behaviour to read from files."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name

from lakehouse_engine.core.definitions import FILE_INPUT_FORMATS, InputSpec, ReadType
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.reader import Reader
from lakehouse_engine.utils.schema_utils import SchemaUtils


class FileReader(Reader):
    """Class to read from files."""

    def __init__(self, input_spec: InputSpec):
        """Construct FileReader instances.

        Args:
            input_spec: input specification.
        """
        super().__init__(input_spec)

    def read(self) -> DataFrame:
        """Read file data.

        Returns:
            A dataframe containing the data from the files.
        """
        if (
            self._input_spec.read_type == ReadType.BATCH.value
            and self._input_spec.data_format in FILE_INPUT_FORMATS
        ):
            df = ExecEnv.SESSION.read.load(
                path=self._input_spec.location,
                format=self._input_spec.data_format,
                schema=SchemaUtils.from_input_spec(self._input_spec),
                **self._input_spec.options if self._input_spec.options else {}
            )

            if self._input_spec.with_filepath:
                df = df.withColumn("lhe_extraction_filepath", input_file_name())

            return df
        elif (
            self._input_spec.read_type == ReadType.STREAMING.value
            and self._input_spec.data_format in FILE_INPUT_FORMATS
        ):
            df = ExecEnv.SESSION.readStream.load(
                path=self._input_spec.location,
                format=self._input_spec.data_format,
                schema=SchemaUtils.from_input_spec(self._input_spec),
                **self._input_spec.options if self._input_spec.options else {}
            )

            if self._input_spec.with_filepath:
                df = df.withColumn("lhe_extraction_filepath", input_file_name())

            return df
        else:
            raise NotImplementedError(
                "The requested read type and format combination is not supported."
            )
