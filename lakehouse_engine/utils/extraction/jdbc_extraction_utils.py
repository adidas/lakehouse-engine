"""Utilities module for JDBC extraction processes."""
from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from logging import Logger
from typing import Any, Dict, List, Optional, Tuple, Union

from lakehouse_engine.core.definitions import InputFormat, InputSpec, ReadType
from lakehouse_engine.utils.logging_handler import LoggingHandler


class JDBCExtractionType(Enum):
    """Standardize the types of extractions we can have from a JDBC source."""

    INIT = "init"
    DELTA = "delta"


@dataclass
class JDBCExtraction(object):
    """Configurations available for an Extraction from a JDBC source.

    These configurations cover:
        user: username to connect to JDBC source.
        password: password to connect to JDBC source (always use secrets,
            don't use text passwords in your code).
        url: url to connect to JDBC source.
        dbtable: database.table to extract data from.
        calc_upper_bound_schema: custom schema used for the upper bound calculation.
        changelog_table: table of type changelog from which to extract data,
            when the extraction type is delta.
        partition_column: column used to split the extraction.
        latest_timestamp_data_location: data location (e.g., s3) containing the data
            to get the latest timestamp already loaded into bronze.
        latest_timestamp_data_format: the format of the dataset in
            latest_timestamp_data_location. Default: delta.
        extraction_type: type of extraction (delta or init). Default: "delta".
        driver: JDBC driver name. Default: "com.sap.db.jdbc.Driver".
        num_partitions: number of Spark partitions to split the extraction.
        lower_bound: lower bound to decide the partition stride.
        upper_bound: upper bound to decide the partition stride. If
            calculate_upper_bound is True, then upperBound will be
            derived by our upper bound optimizer, using the partition column.
        default_upper_bound: the value to use as default upper bound in case
            the result of the upper bound calculation is None. Default: "1".
        fetch_size: how many rows to fetch per round trip. Default: "100000".
        compress: enable network compression. Default: True.
        custom_schema: specify custom_schema for particular columns of the
            returned dataframe in the init/delta extraction of the source table.
        min_timestamp: min timestamp to consider to filter the changelog data.
            Default: None and automatically derived from the location provided.
            In case this one is provided it has precedence and the calculation
            is not done.
        max_timestamp: max timestamp to consider to filter the changelog data.
            Default: None and automatically derived from the table having information
            about the extraction requests, their timestamps and their status.
            In case this one is provided it has precedence and the calculation
            is not done.
        generate_predicates: whether to generate predicates automatically or not.
            Default: False.
        predicates: list containing all values to partition (if generate_predicates
            is used, the manual values provided are ignored). Default: None.
        predicates_add_null: whether to consider null on predicates list.
            Default: True.
        extraction_timestamp: the timestamp of the extraction. Default: current time
            following the format "%Y%m%d%H%M%S".
        max_timestamp_custom_schema: custom schema used on the max_timestamp derivation
            from the table holding the extraction requests information.
    """

    user: str
    password: str
    url: str
    dbtable: str
    calc_upper_bound_schema: Optional[str] = None
    changelog_table: Optional[str] = None
    partition_column: Optional[str] = None
    latest_timestamp_data_location: Optional[str] = None
    latest_timestamp_data_format: str = InputFormat.DELTAFILES.value
    extraction_type: str = JDBCExtractionType.DELTA.value
    driver: str = "com.sap.db.jdbc.Driver"
    num_partitions: Optional[int] = None
    lower_bound: Optional[Union[int, float, str]] = None
    upper_bound: Optional[Union[int, float, str]] = None
    default_upper_bound: str = "1"
    fetch_size: str = "100000"
    compress: bool = True
    custom_schema: Optional[str] = None
    min_timestamp: Optional[str] = None
    max_timestamp: Optional[str] = None
    generate_predicates: bool = False
    predicates: Optional[List] = None
    predicates_add_null: bool = True
    extraction_timestamp: str = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    max_timestamp_custom_schema: Optional[str] = None


class JDBCExtractionUtils(object):
    """Utils for managing data extraction from particularly relevant JDBC sources."""

    def __init__(self, jdbc_extraction: Any):
        """Construct JDBCExtractionUtils.

        Args:
            jdbc_extraction: JDBC Extraction configurations. Can be of type:
                JDBCExtraction, SAPB4Extraction or SAPBWExtraction.
        """
        self._LOGGER: Logger = LoggingHandler(__name__).get_logger()
        self._JDBC_EXTRACTION = jdbc_extraction

    @staticmethod
    def get_additional_spark_options(
        input_spec: InputSpec, options: dict, ignore_options: List = None
    ) -> dict:
        """Helper to get additional Spark Options initially passed.

        If people provide additional Spark options, not covered by the util function
        arguments (get_spark_jdbc_options), we need to consider them.
        Thus, we update the options retrieved by the utils, by checking if there is
        any Spark option initially provided that is not yet considered in the retrieved
        options or function arguments and if the value for the key is not None.
        If these conditions are filled, we add the options and return the complete dict.

        Args:
            input_spec: the input specification.
            options: dict with Spark options.
            ignore_options: list of options to be ignored by the process.
                Spark read has two different approaches to parallelize
                reading process, one of them is using upper/lower bound,
                another one is using predicates, those process can't be
                executed at the same time, you must choose one of them.
                By choosing predicates you can't pass lower and upper bound,
                also can't pass number of partitions and partition column
                otherwise spark will interpret the execution partitioned by
                upper and lower bound and will expect to fill all variables.
                To avoid fill all predicates hardcoded at the acon, there is
                a feature that automatically generates all predicates for init
                or delta load based on input partition column, but at the end
                of the process, partition column can't be passed to the options,
                because we are choosing predicates execution, that is why to
                generate predicates we need to pass some options to ignore.

        Returns:
             a dict with all the options passed as argument, plus the options that
             were initially provided, but were not used in the util
             (get_spark_jdbc_options).
        """
        func_args = JDBCExtractionUtils.get_spark_jdbc_options.__code__.co_varnames

        if ignore_options is None:
            ignore_options = []
        ignore_options = ignore_options + list(options.keys()) + list(func_args)

        return {
            key: value
            for key, value in input_spec.options.items()
            if key not in ignore_options and value is not None
        }

    def get_predicates(self, predicates_query: str) -> List:
        """Get the predicates list, based on a predicates query.

        Args:
            predicates_query: query to use as the basis to get the distinct values for
                a specified column, based on which predicates are generated.

        Returns:
            List containing the predicates to use to split the extraction from
            JDBC sources.
        """
        jdbc_args = {
            "url": self._JDBC_EXTRACTION.url,
            "table": predicates_query,
            "properties": {
                "user": self._JDBC_EXTRACTION.user,
                "password": self._JDBC_EXTRACTION.password,
                "driver": self._JDBC_EXTRACTION.driver,
            },
        }
        from lakehouse_engine.io.reader_factory import ReaderFactory

        predicates_df = ReaderFactory.get_data(
            InputSpec(
                spec_id="get_predicates",
                data_format=InputFormat.JDBC.value,
                read_type=ReadType.BATCH.value,
                jdbc_args=jdbc_args,
            )
        )

        predicates_list = predicates_df.rdd.map(
            lambda row: f"{self._JDBC_EXTRACTION.partition_column}='{row[0]}'"
        ).collect()
        if self._JDBC_EXTRACTION.predicates_add_null:
            predicates_list.append(f"{self._JDBC_EXTRACTION.partition_column} IS NULL")
        self._LOGGER.info(
            f"The following predicate list was generated: {predicates_list}"
        )

        return predicates_list

    def get_spark_jdbc_options(self) -> Tuple[dict, dict]:
        """Get the Spark options to extract data from a JDBC source.

        Returns:
            The Spark jdbc args dictionary, including the query to submit
            and also options args dictionary.
        """
        options_args: Dict[str, Any] = {
            "fetchSize": self._JDBC_EXTRACTION.fetch_size,
            "compress": self._JDBC_EXTRACTION.compress,
        }

        jdbc_args = {
            "url": self._JDBC_EXTRACTION.url,
            "properties": {
                "user": self._JDBC_EXTRACTION.user,
                "password": self._JDBC_EXTRACTION.password,
                "driver": self._JDBC_EXTRACTION.driver,
            },
        }

        if self._JDBC_EXTRACTION.extraction_type == JDBCExtractionType.DELTA.value:
            jdbc_args["table"], predicates_query = self._get_delta_query()
        else:
            jdbc_args["table"], predicates_query = self._get_init_query()

        if self._JDBC_EXTRACTION.custom_schema:
            options_args["customSchema"] = self._JDBC_EXTRACTION.custom_schema

        if self._JDBC_EXTRACTION.generate_predicates:
            jdbc_args["predicates"] = self.get_predicates(predicates_query)
        else:
            if self._JDBC_EXTRACTION.predicates:
                jdbc_args["predicates"] = self._JDBC_EXTRACTION.predicates
            else:
                options_args = self._get_extraction_partition_opts(
                    options_args,
                )

        return options_args, jdbc_args

    def get_spark_jdbc_optimal_upper_bound(self) -> Any:
        """Get an optimal upperBound to properly split a Spark JDBC extraction.

        Returns:
             Either an int, date or timestamp to serve as upperBound Spark JDBC option.
        """
        options = {}
        if self._JDBC_EXTRACTION.calc_upper_bound_schema:
            options["customSchema"] = self._JDBC_EXTRACTION.calc_upper_bound_schema

        table = (
            self._JDBC_EXTRACTION.dbtable
            if self._JDBC_EXTRACTION.extraction_type == JDBCExtractionType.INIT.value
            else self._JDBC_EXTRACTION.changelog_table
        )
        jdbc_args = {
            "url": self._JDBC_EXTRACTION.url,
            "table": f"(SELECT COALESCE(MAX({self._JDBC_EXTRACTION.partition_column}), "
            f"{self._JDBC_EXTRACTION.default_upper_bound}) "
            f"upper_bound FROM {table})",  # nosec: B608
            "properties": {
                "user": self._JDBC_EXTRACTION.user,
                "password": self._JDBC_EXTRACTION.password,
                "driver": self._JDBC_EXTRACTION.driver,
            },
        }

        from lakehouse_engine.io.reader_factory import ReaderFactory

        upper_bound_df = ReaderFactory.get_data(
            InputSpec(
                spec_id="get_optimal_upper_bound",
                data_format=InputFormat.JDBC.value,
                read_type=ReadType.BATCH.value,
                jdbc_args=jdbc_args,
                options=options,
            )
        )
        upper_bound = upper_bound_df.first()[0]

        if upper_bound is not None:
            self._LOGGER.info(
                f"Upper Bound '{upper_bound}' derived from "
                f"'{self._JDBC_EXTRACTION.dbtable}' using the column "
                f"'{self._JDBC_EXTRACTION.partition_column}'"
            )
            return upper_bound
        else:
            raise AttributeError(
                f"Not able to calculate upper bound from "
                f"'{self._JDBC_EXTRACTION.dbtable}' using "
                f"the column '{self._JDBC_EXTRACTION.partition_column}'"
            )

    def _get_extraction_partition_opts(
        self,
        options_args: dict,
    ) -> dict:
        """Get an options dict with custom extraction partition options.

        Args:
            options_args: spark jdbc reader options.
        """
        if self._JDBC_EXTRACTION.num_partitions:
            options_args["numPartitions"] = self._JDBC_EXTRACTION.num_partitions
        if self._JDBC_EXTRACTION.upper_bound:
            options_args["upperBound"] = self._JDBC_EXTRACTION.upper_bound
        if self._JDBC_EXTRACTION.lower_bound:
            options_args["lowerBound"] = self._JDBC_EXTRACTION.lower_bound
        if self._JDBC_EXTRACTION.partition_column:
            options_args["partitionColumn"] = self._JDBC_EXTRACTION.partition_column

        return options_args

    def _get_max_timestamp(self, max_timestamp_query: str) -> str:
        """Get the max timestamp, based on the provided query.

        Args:
            max_timestamp_query: the query used to derive the max timestamp.

        Returns:
            A string having the max timestamp.
        """
        jdbc_args = {
            "url": self._JDBC_EXTRACTION.url,
            "table": max_timestamp_query,
            "properties": {
                "user": self._JDBC_EXTRACTION.user,
                "password": self._JDBC_EXTRACTION.password,
                "driver": self._JDBC_EXTRACTION.driver,
            },
        }
        from lakehouse_engine.io.reader_factory import ReaderFactory

        max_timestamp_df = ReaderFactory.get_data(
            InputSpec(
                spec_id="get_max_timestamp",
                data_format=InputFormat.JDBC.value,
                read_type=ReadType.BATCH.value,
                jdbc_args=jdbc_args,
                options={
                    "customSchema": self._JDBC_EXTRACTION.max_timestamp_custom_schema
                },
            )
        )
        max_timestamp = max_timestamp_df.first()[0]
        self._LOGGER.info(
            f"Max timestamp {max_timestamp} derived from query: {max_timestamp_query}"
        )

        return str(max_timestamp)

    @abstractmethod
    def _get_delta_query(self) -> Tuple[str, str]:
        """Get a query to extract delta (partially) from a source."""
        pass

    @abstractmethod
    def _get_init_query(self) -> Tuple[str, str]:
        """Get a query to extract init (fully) from a source."""
        pass
