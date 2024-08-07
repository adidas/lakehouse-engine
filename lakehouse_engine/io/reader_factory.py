"""Module for reader factory."""

from abc import ABC

from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import FILE_INPUT_FORMATS, InputFormat, InputSpec
from lakehouse_engine.io.readers.dataframe_reader import DataFrameReader
from lakehouse_engine.io.readers.file_reader import FileReader
from lakehouse_engine.io.readers.jdbc_reader import JDBCReader
from lakehouse_engine.io.readers.kafka_reader import KafkaReader
from lakehouse_engine.io.readers.query_reader import QueryReader
from lakehouse_engine.io.readers.sap_b4_reader import SAPB4Reader
from lakehouse_engine.io.readers.sap_bw_reader import SAPBWReader
from lakehouse_engine.io.readers.table_reader import TableReader


class ReaderFactory(ABC):  # noqa: B024
    """Class for reader factory."""

    @classmethod
    def get_data(cls, spec: InputSpec) -> DataFrame:
        """Get data according to the input specification following a factory pattern.

        Args:
            spec: input specification to get the data.

        Returns:
            A dataframe containing the data.
        """
        if spec.db_table:
            read_df = TableReader(input_spec=spec).read()
        elif spec.data_format == InputFormat.JDBC.value:
            read_df = JDBCReader(input_spec=spec).read()
        elif spec.data_format in FILE_INPUT_FORMATS:
            read_df = FileReader(input_spec=spec).read()
        elif spec.data_format == InputFormat.KAFKA.value:
            read_df = KafkaReader(input_spec=spec).read()
        elif spec.data_format == InputFormat.SQL.value:
            read_df = QueryReader(input_spec=spec).read()
        elif spec.data_format == InputFormat.SAP_BW.value:
            read_df = SAPBWReader(input_spec=spec).read()
        elif spec.data_format == InputFormat.SAP_B4.value:
            read_df = SAPB4Reader(input_spec=spec).read()
        elif spec.data_format == InputFormat.DATAFRAME.value:
            read_df = DataFrameReader(input_spec=spec).read()
        elif spec.data_format == InputFormat.SFTP.value:
            from lakehouse_engine.io.readers.sftp_reader import SFTPReader

            read_df = SFTPReader(input_spec=spec).read()
        else:
            raise NotImplementedError(
                f"The requested input spec format {spec.data_format} is not supported."
            )

        if spec.temp_view:
            read_df.createOrReplaceTempView(spec.temp_view)

        return read_df
