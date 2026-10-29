"""Module to define the behaviour of delta merges."""

from collections import OrderedDict
from collections.abc import Callable

from delta.tables import DeltaMergeBuilder, DeltaTable
from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import OutputFormat, OutputSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.exceptions import WrongIOFormatException
from lakehouse_engine.io.writer import Writer


class DeltaMergeWriter(Writer):
    """Class to merge data using delta lake."""

    def __init__(
        self, output_spec: OutputSpec, df: DataFrame, data: OrderedDict | None
    ) -> None:
        """Construct DeltaMergeWriter instances.

        Args:
            output_spec: output specification containing merge options and
                relevant information.
            df: the dataframe containing the new data to be merged.
            data: list of all dfs generated on previous steps before writer.
        """
        super().__init__(output_spec, df, data)

    def write(self) -> None:
        """Merge new data with current data."""
        delta_table = self._get_delta_table(self._output_spec)
        if self._df.isStreaming:
            stream_df = (
                self._df.writeStream.options(
                    **self._output_spec.options if self._output_spec.options else {}
                )
                .foreachBatch(
                    self._write_transformed_micro_batch(
                        self._output_spec,
                        self._data,
                        delta_table,
                    )
                )
                .trigger(**Writer.get_streaming_trigger(self._output_spec))
                .start()
            )

            if self._output_spec.streaming_await_termination:
                stream_df.awaitTermination(
                    self._output_spec.streaming_await_termination_timeout
                )
        else:
            DeltaMergeWriter._merge(delta_table, self._output_spec, self._df)

    @staticmethod
    def _get_delta_table(output_spec: OutputSpec) -> DeltaTable:
        """Get the delta table given an output specification w/ table name or location.

        Args:
            output_spec: output specification.

        Returns:
            DeltaTable: the delta table instance.
        """
        if output_spec.db_table:
            delta_table = DeltaTable.forName(ExecEnv.SESSION, output_spec.db_table)
        elif output_spec.data_format == OutputFormat.DELTAFILES.value:
            if not output_spec.location:
                raise ValueError("location must be provided for delta file merges.")
            delta_table = DeltaTable.forPath(ExecEnv.SESSION, output_spec.location)
        else:
            raise WrongIOFormatException(
                f"{output_spec.data_format} is not compatible with Delta Merge Writer."
            )

        return delta_table

    @staticmethod
    def _insert(
        delta_merge: DeltaMergeBuilder,
        insert_predicate: str | None,
        insert_column_set: dict | None,
    ) -> DeltaMergeBuilder:
        """Get the builder of merge data with insert predicate and column set.

        Args:
            delta_merge: builder of the merge data.
            insert_predicate: condition of the insert.
            insert_column_set: rules for setting the values of
                columns that need to be inserted.

        Returns:
            DeltaMergeBuilder: builder of the merge data with insert.
        """
        if insert_predicate:
            if insert_column_set:
                delta_merge = delta_merge.whenNotMatchedInsert(
                    condition=insert_predicate,
                    values=insert_column_set,
                )
            else:
                delta_merge = delta_merge.whenNotMatchedInsertAll(
                    condition=insert_predicate
                )
        else:
            if insert_column_set:
                delta_merge = delta_merge.whenNotMatchedInsert(values=insert_column_set)
            else:
                delta_merge = delta_merge.whenNotMatchedInsertAll()

        return delta_merge

    @staticmethod
    def _merge(delta_table: DeltaTable, output_spec: OutputSpec, df: DataFrame) -> None:
        """Perform a delta lake merge according to several merge options.

        Args:
            delta_table: delta table to which to merge data.
            output_spec: output specification containing the merge options.
            df: dataframe with the new data to be merged into the delta table.
        """
        if not output_spec.merge_opts:
            raise ValueError("merge_opts must be provided for delta merge writer.")

        merge_opts = output_spec.merge_opts
        delta_merge = delta_table.alias("current").merge(
            df.alias("new"), merge_opts.merge_predicate
        )

        if merge_opts.merge_schema:
            delta_merge = delta_merge.withSchemaEvolution()

        if not merge_opts.insert_only:
            if merge_opts.delete_predicate:
                delta_merge = delta_merge.whenMatchedDelete(merge_opts.delete_predicate)
            delta_merge = DeltaMergeWriter._update(
                delta_merge,
                merge_opts.update_predicate,
                merge_opts.update_column_set,
            )

        delta_merge = DeltaMergeWriter._insert(
            delta_merge,
            merge_opts.insert_predicate,
            merge_opts.insert_column_set,
        )

        delta_merge.execute()

    @staticmethod
    def _update(
        delta_merge: DeltaMergeBuilder,
        update_predicate: str | None,
        update_column_set: dict | None,
    ) -> DeltaMergeBuilder:
        """Get the builder of merge data with update predicate and column set.

        Args:
            delta_merge: builder of the merge data.
            update_predicate: condition of the update.
            update_column_set: rules for setting the values of
                columns that need to be updated.

        Returns:
            DeltaMergeBuilder: builder of the merge data with update.
        """
        if update_predicate:
            if update_column_set:
                delta_merge = delta_merge.whenMatchedUpdate(
                    condition=update_predicate,
                    set=update_column_set,
                )
            else:
                delta_merge = delta_merge.whenMatchedUpdateAll(
                    condition=update_predicate
                )
        else:
            if update_column_set:
                delta_merge = delta_merge.whenMatchedUpdate(set=update_column_set)
            else:
                delta_merge = delta_merge.whenMatchedUpdateAll()

        return delta_merge

    @staticmethod
    def _write_transformed_micro_batch(
        output_spec: OutputSpec,
        data: OrderedDict | None,
        delta_table: DeltaTable | None = None,
    ) -> Callable:
        """Perform the merge in streaming mode by specifying a transform function.

        This function returns a function that will be invoked in the foreachBatch in
        streaming mode, performing a delta lake merge while streaming the micro batches.

        Args:
            output_spec: output specification.
            data: list of all dfs generated on previous steps before writer.
            delta_table: delta table for which to merge the streaming data
                with.

        Returns:
            Function to call in .foreachBatch streaming function.
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

            if not delta_table:
                raise ValueError(
                    "delta_table must be provided for streaming delta merge."
                )
            DeltaMergeWriter._merge(delta_table, output_spec, transformed_df)

        return inner
