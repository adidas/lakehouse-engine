"""Module with helper functions to interact with test dataframes."""
import random
import string
from typing import Optional, OrderedDict

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from lakehouse_engine.core.definitions import (
    InputFormat,
    InputSpec,
    OutputFormat,
    OutputSpec,
    ReadType,
    WriteType,
)
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.readers.file_reader import FileReader
from lakehouse_engine.io.readers.jdbc_reader import JDBCReader
from lakehouse_engine.io.readers.table_reader import TableReader
from lakehouse_engine.io.writers.jdbc_writer import JDBCWriter
from lakehouse_engine.utils.logging_handler import LoggingHandler


class DataframeHelpers(object):
    """Class with helper functions to interact with test dataframes."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def has_diff(
        cls, df: DataFrame, another_df: DataFrame, group_and_order: bool = True
    ) -> bool:
        """Check if a dataframe has differences comparing to another dataframe.

        Note: the order of the columns and rows are not considered as differences
        by default.

        Args:
            df: one dataframe.
            another_df: another dataframe.
            group_and_order: whether to group and order the DFs or not.

        Returns:
            True if it has a difference, false otherwise.
        """

        def print_diff(desc: str, diff_df: DataFrame) -> None:
            cls._logger.debug(desc)
            for row in diff_df.collect():
                cls._logger.debug(row)

        cls._logger.debug("Checking if Dataframes have diff...")
        cols_to_group = df.columns
        if group_and_order:
            df = df.select(*cols_to_group).orderBy(*cols_to_group)
            another_df = another_df.select(*cols_to_group).orderBy(*cols_to_group)

        diff_1 = df.exceptAll(another_df)
        diff_2 = another_df.exceptAll(df)
        if diff_1.rdd.isEmpty() is False or diff_2.rdd.isEmpty() is False:
            df.show(100, False)
            another_df.show(100, False)
            cls._logger.debug("Dataframes have diff...")
            print_diff("Diff 1:", diff_1)
            print_diff("Diff 2:", diff_2)
            return True
        else:
            return False

    @staticmethod
    def read_from_file(
        location: str,
        file_format: str = InputFormat.CSV.value,
        schema: Optional[dict] = None,
        options: Optional[dict] = None,
    ) -> DataFrame:
        """Read data from a file into a dataframe.

        Args:
            location: location of the file(s).
            file_format: file(s) format.
            schema: schema of the files (only works with spark schema
                StructType for now).
            options: options (e.g., spark options) to read data.

        Returns:
            The dataframe that was read.
        """
        if options is None and file_format == InputFormat.CSV.value:
            options = {"header": True, "delimiter": "|", "inferSchema": True}
        spec = InputSpec(
            spec_id=random.choice(string.ascii_letters),  # nosec
            read_type=ReadType.BATCH.value,
            data_format=file_format,
            location=location,
            schema=schema,
            options=options,
        )
        return FileReader(input_spec=spec).read()

    @staticmethod
    def read_from_table(db_table: str, options: Optional[dict] = None) -> DataFrame:
        """Read data from a table into a dataframe.

        Args:
            db_table: database.table_name.
            options: options (e.g., spark options) to read data.

        Returns:
            DataFrame: the dataframe that was read.
        """
        spec = InputSpec(
            spec_id=random.choice(string.ascii_letters),  # nosec
            read_type=ReadType.BATCH.value,
            db_table=db_table,
            options=options,
        )
        return TableReader(input_spec=spec).read()

    @staticmethod
    def read_from_jdbc(
        uri: str, db_table: str, driver: str = "org.sqlite.JDBC"
    ) -> DataFrame:
        """Read data from jdbc into a dataframe.

        Args:
            uri: uri for the jdbc connection.
            db_table: database.table_name.
            driver: driver class.

        Returns:
            DataFrame: the dataframe that was read.
        """
        spec = InputSpec(
            spec_id=random.choice(string.ascii_letters),  # nosec
            db_table=db_table,
            read_type=ReadType.BATCH.value,
            options={"url": uri, "dbtable": db_table, "driver": driver},
        )
        return JDBCReader(input_spec=spec).read()

    @staticmethod
    def write_into_jdbc_table(
        df: DataFrame,
        uri: str,
        db_table: str,
        write_type: str = WriteType.APPEND.value,
        driver: str = "org.sqlite.JDBC",
        data: OrderedDict = None,
    ) -> None:
        """Write data into a jdbc table.

        Args:
            df: dataframe containing the data to append.
            uri: uri for the jdbc connection.
            db_table: database.table_name.
            write_type: type of writer to use for writing into the destination
            driver: driver class.
            data: list of all dfs generated on previous steps before writer.
        """
        spec = OutputSpec(
            spec_id=random.choice(string.ascii_letters),  # nosec
            input_id=random.choice(string.ascii_letters),  # nosec
            write_type=write_type,
            data_format=OutputFormat.JDBC.value,
            options={"url": uri, "dbtable": db_table, "driver": driver},
        )

        JDBCWriter(output_spec=spec, df=df.coalesce(1), data=data).write()

    @staticmethod
    def create_empty_dataframe(struct_type: StructType) -> DataFrame:
        """Create an empty DataFrame.

        Args:
            struct_type: dict containing a spark schema structure. Check here:
                https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/
                StructType.html.
        """
        return ExecEnv.SESSION.createDataFrame(data=[], schema=struct_type)

    @staticmethod
    def create_delta_table(
        cols: dict, table: str, db: str = "test_db", enable_cdf: bool = False
    ) -> None:
        """Create a delta table for test purposes.

        Args:
            cols: dict of columns to create table and their types.
            table: table name.
            db: database name.
            enable_cdf: whether to enable change data feed, or not.
        """
        ExecEnv.SESSION.sql(
            f"""
            CREATE EXTERNAL TABLE {db}.{table} (
                {','.join([f'{cname} {ctype}' for cname, ctype in cols.items()])}
            )
            USING delta
            TBLPROPERTIES (delta.enableChangeDataFeed = {str(enable_cdf).lower()})
            """
        )
