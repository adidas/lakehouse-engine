"""Module to define behaviour to write to dataframe."""

import uuid
from typing import Callable, Optional, OrderedDict

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from lakehouse_engine.core.definitions import OutputFormat, OutputSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.exceptions import NotSupportedException
from lakehouse_engine.io.writer import Writer
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.spark_utils import SparkUtils


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
        self.view_prefix = "global_temp" if not ExecEnv.IS_SERVERLESS else ""

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

    def _get_prefixed_view_name(self, stream_df_view_name: str) -> str:
        """Return the fully qualified view name with prefix if needed."""
        return ".".join(filter(None, [self.view_prefix, stream_df_view_name]))

    def _create_temp_view(self, df: DataFrame, stream_df_view_name: str) -> None:
        """Given a dataframe create a temp view to be available for consumption.

        Args:
            df: dataframe to be shown.
            stream_df_view_name: stream df view name.
        """
        prefixed_view_name = self._get_prefixed_view_name(stream_df_view_name)
        if self._table_exists(stream_df_view_name):
            self._logger.info("Temp view already exists")
            existing_data = ExecEnv.SESSION.table(f"{prefixed_view_name}")
            df = existing_data.union(df)

        SparkUtils.create_temp_view(df, stream_df_view_name)

    def _write_streaming_df(self, stream_df_view_name: str) -> Callable:
        """Define how to create a df from streaming df.

        Args:
            stream_df_view_name: stream df view name.

        Returns:
            A function to show df in the foreachBatch spark write method.
        """

        def inner(batch_df: DataFrame, batch_id: int) -> None:
            ExecEnv.get_for_each_batch_session(batch_df)
            self._create_temp_view(batch_df, stream_df_view_name)

        return inner

    def _write_to_dataframe_in_streaming_mode(
        self, df: DataFrame, output_spec: OutputSpec, data: OrderedDict
    ) -> DataFrame:
        """Write to DataFrame in streaming mode.

        Args:
            df: dataframe to write.
            output_spec: output specification.
            data: list of all dfs generated on previous steps before writer.
        """
        app_id = str(uuid.uuid4())
        stream_df_view_name = f"`{app_id}_{output_spec.spec_id}`"
        self._logger.info("Drop temp view if exists")
        prefixed_view_name = self._get_prefixed_view_name(stream_df_view_name)

        if self._table_exists(stream_df_view_name):
            # Cleaning Temp view to not maintain state and impact
            # consecutive acon runs
            ExecEnv.SESSION.sql(f"DROP VIEW {prefixed_view_name}")

        df_writer = df.writeStream.trigger(**Writer.get_streaming_trigger(output_spec))

        if (
            output_spec.streaming_micro_batch_transformers
            or output_spec.streaming_micro_batch_dq_processors
        ):
            stream_df = (
                df_writer.options(**output_spec.options if output_spec.options else {})
                .format(OutputFormat.NOOP.value)
                .foreachBatch(
                    self._write_transformed_micro_batch(
                        output_spec, data, stream_df_view_name
                    )
                )
                .start()
            )
        else:
            stream_df = (
                df_writer.options(**output_spec.options if output_spec.options else {})
                .format(OutputFormat.NOOP.value)
                .foreachBatch(self._write_streaming_df(stream_df_view_name))
                .start()
            )

        if output_spec.streaming_await_termination:
            stream_df.awaitTermination(output_spec.streaming_await_termination_timeout)

        self._logger.info("Reading stream data as df if exists")
        if self._table_exists(stream_df_view_name):
            stream_data_as_df = ExecEnv.SESSION.table(f"{prefixed_view_name}")
        else:
            self._logger.info(
                f"DataFrame writer couldn't find any data to return "
                f"for streaming, check if you are using checkpoint "
                f"for step {output_spec.spec_id}."
            )
            stream_data_as_df = ExecEnv.SESSION.createDataFrame(
                data=[], schema=StructType([])
            )

        return stream_data_as_df

    def _table_exists(self, table_name: str) -> bool:
        """Check if the table or view exists in the session catalog.

        Args:
            table_name: table/view name to check if exists in the session.
        """
        if not ExecEnv.IS_SERVERLESS:
            tables = ExecEnv.SESSION.sql(f"SHOW TABLES IN {self.view_prefix}")
        else:
            tables = ExecEnv.SESSION.sql("SHOW TABLES")
        return (
            len(tables.filter(f"tableName = '{table_name.strip('`')}'").collect()) > 0
        )

    def _write_transformed_micro_batch(
        self, output_spec: OutputSpec, data: OrderedDict, stream_as_df_view: str
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
            ExecEnv.get_for_each_batch_session(batch_df)
            transformed_df = Writer.get_transformed_micro_batch(
                output_spec, batch_df, batch_id, data
            )

            if output_spec.streaming_micro_batch_dq_processors:
                transformed_df = Writer.run_micro_batch_dq_process(
                    transformed_df, output_spec.streaming_micro_batch_dq_processors
                )

            self._create_temp_view(transformed_df, stream_as_df_view)

        return inner
