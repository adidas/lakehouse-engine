"""Module that defines the behaviour to write to JDBC targets."""
from typing import Callable, OrderedDict

from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import OutputSpec
from lakehouse_engine.io.writer import Writer


class JDBCWriter(Writer):
    """Class to write to JDBC targets."""

    def __init__(self, output_spec: OutputSpec, df: DataFrame, data: OrderedDict):
        """Construct JDBCWriter instances.

        Args:
            output_spec: output specification.
            df: dataframe to be writen.
            data: list of all dfs generated on previous steps before writer.
        """
        super().__init__(output_spec, df, data)

    def write(self) -> None:
        """Write data into JDBC target."""
        if not self._df.isStreaming:
            self._write_to_jdbc_in_batch_mode(self._df, self._output_spec)
        else:
            stream_df = (
                self._df.writeStream.trigger(
                    **Writer.get_streaming_trigger(self._output_spec)
                )
                .options(
                    **self._output_spec.options if self._output_spec.options else {}
                )
                .foreachBatch(
                    self._write_transformed_micro_batch(self._output_spec, self._data)
                )
                .start()
            )

            if self._output_spec.streaming_await_termination:
                stream_df.awaitTermination(
                    self._output_spec.streaming_await_termination_timeout
                )

    @staticmethod
    def _write_to_jdbc_in_batch_mode(df: DataFrame, output_spec: OutputSpec) -> None:
        """Write to jdbc in batch mode.

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

            JDBCWriter._write_to_jdbc_in_batch_mode(transformed_df, output_spec)

        return inner
