"""Module to define behaviour to write to dataframe."""
from typing import Callable, Optional, OrderedDict

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from lakehouse_engine.core.definitions import OutputFormat, OutputSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.exceptions import NotSupportedException
from lakehouse_engine.io.writer import Writer
from lakehouse_engine.utils.logging_handler import LoggingHandler


class DataFrameWriter(Writer):
    """Class to write data to dataframe."""

    _logger = LoggingHandler(__name__).get_logger()

    def __init__(self, output_spec: OutputSpec, df: DataFrame, data: OrderedDict):
        """Construct DataFrameWriter instances.

        Args:
            output_spec: output specification.
            df: dataframe to be written.
            data: list of all dfs generated on previous steps before writer.
        """
        super().__init__(output_spec, df, data)

    def write(self) -> Optional[OrderedDict]:
        """Write data to dataframe."""
        self._output_spec.options = (
            self._output_spec.options if self._output_spec.options else {}
        )
        written_dfs: OrderedDict = OrderedDict({})

        if (
            self._output_spec.streaming_processing_time
            or self._output_spec.streaming_continuous
        ):
            raise NotSupportedException(
                f"DataFrame writer doesn't support "
                f"processing time or continuous streaming "
                f"for step ${self._output_spec.spec_id}."
            )

        if self._df.isStreaming:
            output_df = self._write_to_dataframe_in_streaming_mode(
                self._df, self._output_spec, self._data
            )
        else:
            output_df = self._df

        written_dfs[self._output_spec.spec_id] = output_df

        return written_dfs

    @staticmethod
    def _create_global_view(df: DataFrame, stream_df_view_name: str) -> None:
        """Given a dataframe create a global temp view to be available for consumption.

        Args:
            df: dataframe to be shown.
            stream_df_view_name: stream df view name.
        """
        if DataFrameWriter._table_exists(stream_df_view_name):
            DataFrameWriter._logger.info("Global temp view exists")
            existing_data = ExecEnv.SESSION.table(f"global_temp.{stream_df_view_name}")
            df = existing_data.union(df)

        df.createOrReplaceGlobalTempView(f"{stream_df_view_name}")

    @staticmethod
    def _write_streaming_df(stream_df_view_name: str) -> Callable:
        """Define how to create a df from streaming df.

        Args:
            stream_df_view_name: stream df view name.

        Returns:
            A function to show df in the foreachBatch spark write method.
        """

        def inner(batch_df: DataFrame, batch_id: int) -> None:
            DataFrameWriter._create_global_view(batch_df, stream_df_view_name)

        return inner

    @staticmethod
    def _write_to_dataframe_in_streaming_mode(
        df: DataFrame, output_spec: OutputSpec, data: OrderedDict
    ) -> DataFrame:
        """Write to DataFrame in streaming mode.

        Args:
            df: dataframe to write.
            output_spec: output specification.
            data: list of all dfs generated on previous steps before writer.
        """
        app_id = ExecEnv.SESSION.sparkContext.applicationId
        stream_df_view_name = f"`{app_id}_{output_spec.spec_id}`"
        DataFrameWriter._logger.info("Drop temp view if exists")

        if DataFrameWriter._table_exists(stream_df_view_name):
            # Cleaning global temp view to not maintain state and impact other acon runs
            ExecEnv.SESSION.catalog.dropGlobalTempView(stream_df_view_name.strip("`"))

        df_writer = df.writeStream.trigger(**Writer.get_streaming_trigger(output_spec))

        if (
            output_spec.streaming_micro_batch_transformers
            or output_spec.streaming_micro_batch_dq_processors
        ):
            stream_df = (
                df_writer.options(**output_spec.options if output_spec.options else {})
                .format(OutputFormat.NOOP.value)
                .foreachBatch(
                    DataFrameWriter._write_transformed_micro_batch(
                        output_spec, data, stream_df_view_name
                    )
                )
                .start()
            )
        else:
            stream_df = (
                df_writer.options(**output_spec.options if output_spec.options else {})
                .format(OutputFormat.NOOP.value)
                .foreachBatch(DataFrameWriter._write_streaming_df(stream_df_view_name))
                .start()
            )

        if output_spec.streaming_await_termination:
            stream_df.awaitTermination(output_spec.streaming_await_termination_timeout)

        DataFrameWriter._logger.info("Reading stream data as df if exists")
        if DataFrameWriter._table_exists(stream_df_view_name):
            stream_data_as_df = ExecEnv.SESSION.table(
                f"global_temp.{stream_df_view_name}"
            )
        else:
            DataFrameWriter._logger.info(
                f"DataFrame writer couldn't find any data to return "
                f"for streaming, check if you are using checkpoint "
                f"for step {output_spec.spec_id}."
            )
            stream_data_as_df = ExecEnv.SESSION.createDataFrame(
                data=[], schema=StructType([])
            )

        return stream_data_as_df

    @staticmethod
    def _table_exists(  # type: ignore
        table_name: str, db_name: str = "global_temp"
    ) -> bool:
        """Check if the table exists in the session catalog.

        Args:
            table_name: table/view name to check if exists in the session.
            db_name: database name that you want to check if the table/view exists,
            default value is the global_temp.

        Returns:
            A bool representing if the table/view exists.
        """
        return ExecEnv.SESSION.catalog.tableExists(table_name.strip("`"), db_name)

    @staticmethod
    def _write_transformed_micro_batch(  # type: ignore
        output_spec: OutputSpec, data: OrderedDict, stream_as_df_view
    ) -> Callable:
        """Define how to write a streaming micro batch after transforming it.

        Args:
            output_spec: output specification.
            data: list of all dfs generated on previous steps before writer.
            stream_as_df_view: stream df view name.

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

            DataFrameWriter._create_global_view(transformed_df, stream_as_df_view)

        return inner
