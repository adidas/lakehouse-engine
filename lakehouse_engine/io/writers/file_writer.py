"""Module to define behaviour to write to files."""
from typing import Callable, OrderedDict

from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import OutputSpec
from lakehouse_engine.io.writer import Writer


class FileWriter(Writer):
    """Class to write data to files."""

    def __init__(self, output_spec: OutputSpec, df: DataFrame, data: OrderedDict):
        """Construct FileWriter instances.

        Args:
            output_spec: output specification
            df: dataframe to be written.
            data: list of all dfs generated on previous steps before writer.
        """
        super().__init__(output_spec, df, data)

    def write(self) -> None:
        """Write data to files."""
        if not self._df.isStreaming:
            self._write_to_files_in_batch_mode(self._df, self._output_spec)
        else:
            self._write_to_files_in_streaming_mode(
                self._df, self._output_spec, self._data
            )

    @staticmethod
    def _write_to_files_in_batch_mode(df: DataFrame, output_spec: OutputSpec) -> None:
        """Write to files in batch mode.

        Args:
            df: dataframe to write.
            output_spec: output specification.
        """
        df.write.format(output_spec.data_format).partitionBy(
            output_spec.partitions
        ).options(**output_spec.options if output_spec.options else {}).mode(
            output_spec.write_type
        ).save(
            output_spec.location
        )

    @staticmethod
    def _write_to_files_in_streaming_mode(
        df: DataFrame, output_spec: OutputSpec, data: OrderedDict
    ) -> None:
        """Write to files in streaming mode.

        Args:
            df: dataframe to write.
            output_spec: output specification.
            data: list of all dfs generated on previous steps before writer.
        """
        df_writer = df.writeStream.trigger(**Writer.get_streaming_trigger(output_spec))

        if (
            output_spec.streaming_micro_batch_transformers
            or output_spec.streaming_micro_batch_dq_processors
        ):
            stream_df = (
                df_writer.options(**output_spec.options if output_spec.options else {})
                .foreachBatch(
                    FileWriter._write_transformed_micro_batch(output_spec, data)
                )
                .start()
            )
        else:
            stream_df = (
                df_writer.format(output_spec.data_format)
                .partitionBy(output_spec.partitions)
                .options(**output_spec.options if output_spec.options else {})
                .outputMode(output_spec.write_type)
                .start(output_spec.location)
            )

        if output_spec.streaming_await_termination:
            stream_df.awaitTermination(output_spec.streaming_await_termination_timeout)

    @staticmethod
    def _write_transformed_micro_batch(  # type: ignore
        output_spec: OutputSpec, data: OrderedDict
    ) -> Callable:
        """Define how to write a streaming micro batch after transforming it.

        Args:
            output_spec: output specification.
            data: list of all dfs generated on previous steps before writer.

        Returns:
            A function to be executed in the foreachBatch spark write method.
        """

        def inner(batch_df: DataFrame, batch_id: int) -> None:
            transformed_df = Writer.get_transformed_micro_batch(
                output_spec, batch_df, batch_id, data
            )

            if output_spec.streaming_micro_batch_dq_processors:
                transformed_df = Writer.run_micro_batch_dq_process(
                    transformed_df, output_spec.streaming_micro_batch_dq_processors
                )

            FileWriter._write_to_files_in_batch_mode(transformed_df, output_spec)

        return inner
