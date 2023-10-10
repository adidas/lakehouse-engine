"""Module to define behaviour to write to console."""
from typing import Callable, OrderedDict

from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import OutputSpec
from lakehouse_engine.io.writer import Writer
from lakehouse_engine.utils.logging_handler import LoggingHandler


class ConsoleWriter(Writer):
    """Class to write data to console."""

    _logger = LoggingHandler(__name__).get_logger()

    def __init__(self, output_spec: OutputSpec, df: DataFrame, data: OrderedDict):
        """Construct ConsoleWriter instances.

        Args:
            output_spec: output specification
            df: dataframe to be written.
            data: list of all dfs generated on previous steps before writer.
        """
        super().__init__(output_spec, df, data)

    def write(self) -> None:
        """Write data to console."""
        self._output_spec.options = (
            self._output_spec.options if self._output_spec.options else {}
        )
        if not self._df.isStreaming:
            self._logger.info("Dataframe preview:")
            self._show_df(self._df, self._output_spec)
        else:
            self._logger.info("Stream Dataframe preview:")
            self._write_to_console_in_streaming_mode(
                self._df, self._output_spec, self._data
            )

    @staticmethod
    def _show_df(df: DataFrame, output_spec: OutputSpec) -> None:
        """Given a dataframe it applies Spark's show function to show it.

        Args:
            df: dataframe to be shown.
            output_spec: output specification.
        """
        df.show(
            n=output_spec.options.get("limit", 20),
            truncate=output_spec.options.get("truncate", True),
            vertical=output_spec.options.get("vertical", False),
        )

    @staticmethod
    def _show_streaming_df(output_spec: OutputSpec) -> Callable:
        """Define how to show a streaming df.

        Args:
            output_spec: output specification.

        Returns:
            A function to show df in the foreachBatch spark write method.
        """

        def inner(batch_df: DataFrame, batch_id: int) -> None:
            ConsoleWriter._logger.info(f"Showing DF for batch {batch_id}")
            ConsoleWriter._show_df(batch_df, output_spec)

        return inner

    @staticmethod
    def _write_to_console_in_streaming_mode(
        df: DataFrame, output_spec: OutputSpec, data: OrderedDict
    ) -> None:
        """Write to console in streaming mode.

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
            stream_df = df_writer.foreachBatch(
                ConsoleWriter._write_transformed_micro_batch(output_spec, data)
            ).start()
        else:
            stream_df = df_writer.foreachBatch(
                ConsoleWriter._show_streaming_df(output_spec)
            ).start()

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

            ConsoleWriter._show_df(transformed_df, output_spec)

        return inner
