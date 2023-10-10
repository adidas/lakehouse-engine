"""Defines change data feed processor behaviour."""
from datetime import datetime, timedelta
from typing import OrderedDict

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format

from lakehouse_engine.core.definitions import (
    InputSpec,
    OutputFormat,
    OutputSpec,
    ReadType,
    TerminatorSpec,
    WriteType,
)
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.reader_factory import ReaderFactory
from lakehouse_engine.io.writer_factory import WriterFactory
from lakehouse_engine.utils.logging_handler import LoggingHandler


class CDFProcessor(object):
    """Change data feed processor class."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def expose_cdf(cls, spec: TerminatorSpec) -> None:
        """Expose CDF to external location.

        Args:
            spec: terminator specification.
        """
        cls._logger.info("Reading CDF from input table...")

        df_cdf = ReaderFactory.get_data(cls._get_table_cdf_input_specs(spec))
        new_df_cdf = df_cdf.withColumn(
            "_commit_timestamp",
            date_format(col("_commit_timestamp"), "yyyyMMddHHmmss"),
        )

        cls._logger.info("Writing CDF to external table...")
        cls._write_cdf_to_external(
            spec,
            new_df_cdf.repartition(
                spec.args.get(
                    "materialized_cdf_num_partitions", col("_commit_timestamp")
                )
            ),
        )

        # used to delete old data on CDF table (don't remove parquet).
        if spec.args.get("clean_cdf", True):
            cls._logger.info("Cleaning CDF table...")
            cls.delete_old_data(spec)

        # used to delete old parquet files.
        if spec.args.get("vacuum_cdf", False):
            cls._logger.info("Vacuuming CDF table...")
            cls.vacuum_cdf_data(spec)

    @staticmethod
    def _write_cdf_to_external(
        spec: TerminatorSpec, df: DataFrame, data: OrderedDict = None
    ) -> None:
        """Write cdf results dataframe.

        Args:
            spec: terminator specification.
            df: dataframe with cdf results to write.
            data: list of all dfs generated on previous steps before writer.
        """
        WriterFactory.get_writer(
            spec=OutputSpec(
                spec_id="materialized_cdf",
                input_id="input_table",
                location=spec.args["materialized_cdf_location"],
                write_type=WriteType.APPEND.value,
                data_format=spec.args.get("data_format", OutputFormat.DELTAFILES.value),
                options=spec.args["materialized_cdf_options"],
                partitions=["_commit_timestamp"],
            ),
            df=df,
            data=data,
        ).write()

    @staticmethod
    def _get_table_cdf_input_specs(spec: TerminatorSpec) -> InputSpec:
        """Get the input specifications from a terminator spec.

        Args:
            spec: terminator specifications.

        Returns:
            List of input specifications.
        """
        options = {
            "readChangeFeed": "true",
            **spec.args.get("db_table_options", {}),
        }

        input_specs = InputSpec(
            spec_id="input_table",
            db_table=spec.args["db_table"],
            read_type=ReadType.STREAMING.value,
            data_format=OutputFormat.DELTAFILES.value,
            options=options,
        )

        return input_specs

    @classmethod
    def delete_old_data(cls, spec: TerminatorSpec) -> None:
        """Delete old data from cdf delta table.

        Args:
            spec: terminator specifications.
        """
        today_datetime = datetime.today()
        limit_date = today_datetime + timedelta(
            days=spec.args.get("days_to_keep", 30) * -1
        )
        limit_timestamp = limit_date.strftime("%Y%m%d%H%M%S")

        cdf_delta_table = DeltaTable.forPath(
            ExecEnv.SESSION, spec.args["materialized_cdf_location"]
        )

        cdf_delta_table.delete(col("_commit_timestamp") < limit_timestamp)

    @classmethod
    def vacuum_cdf_data(cls, spec: TerminatorSpec) -> None:
        """Vacuum old data from cdf delta table.

        Args:
            spec: terminator specifications.
        """
        cdf_delta_table = DeltaTable.forPath(
            ExecEnv.SESSION, spec.args["materialized_cdf_location"]
        )

        cdf_delta_table.vacuum(spec.args.get("vacuum_hours", 168))
