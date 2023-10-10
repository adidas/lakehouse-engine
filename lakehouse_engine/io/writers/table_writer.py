"""Module that defines the behaviour to write to tables."""
from typing import Any, Callable, OrderedDict

from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import OutputFormat, OutputSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.writer import Writer


class TableWriter(Writer):
    """Class to write to a table."""

    def __init__(self, output_spec: OutputSpec, df: DataFrame, data: OrderedDict):
        """Construct TableWriter instances.

        Args:
            output_spec: output specification.
            df: dataframe to be written.
            data: list of all dfs generated on previous steps before writer.
        """
        super().__init__(output_spec, df, data)

    def write(self) -> None:
        """Write data to a table.

        After the write operation we repair the table (e.g., update partitions).
        However, there's a caveat to this, which is the fact that this repair
        operation is not reachable if we are running long-running streaming mode.
        Therefore, we recommend not using the TableWriter with formats other than
        delta lake for those scenarios (as delta lake does not need msck repair).
        So, you can: 1) use delta lake format for the table; 2) use the FileWriter
        and run the repair with a certain frequency in a separate task of your
        pipeline.
        """
        if not self._df.isStreaming:
            self._write_to_table_in_batch_mode(self._df, self._output_spec)
        else:
            df_writer = self._df.writeStream.trigger(
                **Writer.get_streaming_trigger(self._output_spec)
            )

            if (
                self._output_spec.streaming_micro_batch_transformers
                or self._output_spec.streaming_micro_batch_dq_processors
            ):
                stream_df = (
                    df_writer.options(
                        **self._output_spec.options if self._output_spec.options else {}
                    )
                    .foreachBatch(
                        self._write_transformed_micro_batch(
                            self._output_spec, self._data
                        )
                    )
                    .start()
                )

                if self._output_spec.streaming_await_termination:
                    stream_df.awaitTermination(
                        self._output_spec.streaming_await_termination_timeout
                    )
            else:
                self._write_to_table_in_streaming_mode(df_writer, self._output_spec)

        if (
            self._output_spec.data_format != OutputFormat.DELTAFILES.value
            and self._output_spec.partitions
        ):
            ExecEnv.SESSION.sql(f"MSCK REPAIR TABLE {self._output_spec.db_table}")

    @staticmethod
    def _write_to_table_in_batch_mode(df: DataFrame, output_spec: OutputSpec) -> None:
        """Write to a metastore table in batch mode.

        Args:
            df: dataframe to write.
            output_spec: output specification.
        """
        df_writer = df.write.format(output_spec.data_format)

        if output_spec.partitions:
            df_writer = df_writer.partitionBy(output_spec.partitions)

        if output_spec.location:
            df_writer = df_writer.options(
                path=output_spec.location,
                **output_spec.options if output_spec.options else {},
            )
        else:
            df_writer = df_writer.options(
                **output_spec.options if output_spec.options else {}
            )

        df_writer.mode(output_spec.write_type).saveAsTable(output_spec.db_table)

    @staticmethod
    def _write_to_table_in_streaming_mode(
        df_writer: Any, output_spec: OutputSpec
    ) -> None:
        """Write to a metastore table in streaming mode.

        Args:
            df_writer: dataframe writer.
            output_spec: output specification.
        """
        df_writer = df_writer.outputMode(output_spec.write_type).format(
            output_spec.data_format
        )

        if output_spec.partitions:
            df_writer = df_writer.partitionBy(output_spec.partitions)

        if output_spec.location:
            df_writer = df_writer.options(
                path=output_spec.location,
                **output_spec.options if output_spec.options else {},
            )
        else:
            df_writer = df_writer.options(
                **output_spec.options if output_spec.options else {}
            )

        if output_spec.streaming_await_termination:
            df_writer.toTable(output_spec.db_table).awaitTermination(
                output_spec.streaming_await_termination_timeout
            )
        else:
            df_writer.toTable(output_spec.db_table)

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

            TableWriter._write_to_table_in_batch_mode(transformed_df, output_spec)

        return inner
