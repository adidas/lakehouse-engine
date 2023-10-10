"""Module for writer factory."""
from abc import ABC
from typing import OrderedDict

from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import (
    FILE_OUTPUT_FORMATS,
    OutputFormat,
    OutputSpec,
    WriteType,
)
from lakehouse_engine.io.writer import Writer
from lakehouse_engine.io.writers.console_writer import ConsoleWriter
from lakehouse_engine.io.writers.dataframe_writer import DataFrameWriter
from lakehouse_engine.io.writers.delta_merge_writer import DeltaMergeWriter
from lakehouse_engine.io.writers.file_writer import FileWriter
from lakehouse_engine.io.writers.jdbc_writer import JDBCWriter
from lakehouse_engine.io.writers.kafka_writer import KafkaWriter
from lakehouse_engine.io.writers.table_writer import TableWriter


class WriterFactory(ABC):  # noqa: B024
    """Class for writer factory."""

    AVAILABLE_WRITERS = {
        OutputFormat.TABLE.value: TableWriter,
        OutputFormat.DELTAFILES.value: DeltaMergeWriter,
        OutputFormat.JDBC.value: JDBCWriter,
        OutputFormat.FILE.value: FileWriter,
        OutputFormat.KAFKA.value: KafkaWriter,
        OutputFormat.CONSOLE.value: ConsoleWriter,
        OutputFormat.DATAFRAME.value: DataFrameWriter,
    }

    @classmethod
    def _get_writer_name(cls, spec: OutputSpec) -> str:
        """Get the writer name according to the output specification.

        Args:
            OutputSpec spec: output specification to write data.

        Returns:
            Writer: writer name that will be created to write the data.
        """
        if spec.db_table and spec.write_type != WriteType.MERGE.value:
            writer_name = OutputFormat.TABLE.value
        elif (
            spec.data_format == OutputFormat.DELTAFILES.value or spec.db_table
        ) and spec.write_type == WriteType.MERGE.value:
            writer_name = OutputFormat.DELTAFILES.value
        elif spec.data_format in FILE_OUTPUT_FORMATS:
            writer_name = OutputFormat.FILE.value
        else:
            writer_name = spec.data_format
        return writer_name

    @classmethod
    def get_writer(cls, spec: OutputSpec, df: DataFrame, data: OrderedDict) -> Writer:
        """Get a writer according to the output specification using a factory pattern.

        Args:
            OutputSpec spec: output specification to write data.
            DataFrame df: dataframe to be written.
            OrderedDict data: list of all dfs generated on previous steps before writer.

        Returns:
            Writer: writer that will write the data.
        """
        writer_name = cls._get_writer_name(spec)
        writer = cls.AVAILABLE_WRITERS.get(writer_name)

        if writer:
            return writer(output_spec=spec, df=df, data=data)  # type: ignore
        else:
            raise NotImplementedError(
                f"The requested output spec format {spec.data_format} is not supported."
            )
