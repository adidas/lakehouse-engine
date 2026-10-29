"""Module to define behaviour to read from SAP B4 sources."""

from logging import Logger

from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import InputSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.reader import Reader
from lakehouse_engine.utils.extraction.sap_b4_extraction_utils import (
    ADSOTypes,
    SAPB4Extraction,
    SAPB4ExtractionUtils,
)
from lakehouse_engine.utils.logging_handler import LoggingHandler


class SAPB4Reader(Reader):
    """Class to read from SAP B4 source."""

    _LOGGER: Logger = LoggingHandler(__name__).get_logger()

    def _get_input_options(self) -> dict:
        """Get reader options ensuring they are available."""
        if not self._input_spec.options:
            raise ValueError("options must be provided for SAP B4 reader.")
        return self._input_spec.options

    def __init__(self, input_spec: InputSpec) -> None:
        """Construct SAPB4Reader instances.

        Args:
            input_spec: input specification.
        """
        super().__init__(input_spec)
        self.jdbc_utils = self._get_jdbc_utils()

    def read(self) -> DataFrame:
        """Read data from SAP B4 source.

        Returns:
            A dataframe containing the data from the SAP B4 source.
        """
        options_args, jdbc_args = self._get_options()
        return ExecEnv.SESSION.read.options(**options_args).jdbc(**jdbc_args)

    def _get_jdbc_utils(self) -> SAPB4ExtractionUtils:
        options = self._get_input_options()
        jdbc_extraction = SAPB4Extraction(
            user=options["user"],
            password=options["password"],
            url=options["url"],
            dbtable=options["dbtable"],
            adso_type=options["adso_type"],
            request_status_tbl=options.get(
                "request_status_tbl", SAPB4Extraction.request_status_tbl
            ),
            changelog_table=options.get(
                "changelog_table",
                (
                    options["dbtable"]
                    if options["adso_type"] == ADSOTypes.AQ.value
                    else options["changelog_table"]
                ),
            ),
            data_target=SAPB4ExtractionUtils.get_data_target(options),
            act_req_join_condition=options.get(
                "act_req_join_condition", SAPB4Extraction.act_req_join_condition
            ),
            latest_timestamp_data_location=options.get(
                "latest_timestamp_data_location",
                SAPB4Extraction.latest_timestamp_data_location,
            ),
            latest_timestamp_input_col=options.get(
                "latest_timestamp_input_col",
                SAPB4Extraction.latest_timestamp_input_col,
            ),
            latest_timestamp_data_format=options.get(
                "latest_timestamp_data_format",
                SAPB4Extraction.latest_timestamp_data_format,
            ),
            extraction_type=options.get(
                "extraction_type", SAPB4Extraction.extraction_type
            ),
            driver=options.get("driver", SAPB4Extraction.driver),
            num_partitions=options.get("numPartitions", SAPB4Extraction.num_partitions),
            partition_column=options.get(
                "partitionColumn", SAPB4Extraction.partition_column
            ),
            lower_bound=options.get("lowerBound", SAPB4Extraction.lower_bound),
            upper_bound=options.get("upperBound", SAPB4Extraction.upper_bound),
            default_upper_bound=options.get(
                "default_upper_bound", SAPB4Extraction.default_upper_bound
            ),
            fetch_size=options.get("fetchSize", SAPB4Extraction.fetch_size),
            compress=options.get("compress", SAPB4Extraction.compress),
            custom_schema=options.get("customSchema", SAPB4Extraction.custom_schema),
            extraction_timestamp=options.get(
                "extraction_timestamp",
                SAPB4Extraction.extraction_timestamp,
            ),
            min_timestamp=options.get("min_timestamp", SAPB4Extraction.min_timestamp),
            max_timestamp=options.get("max_timestamp", SAPB4Extraction.max_timestamp),
            default_max_timestamp=options.get(
                "default_max_timestamp", SAPB4Extraction.default_max_timestamp
            ),
            default_min_timestamp=options.get(
                "default_min_timestamp", SAPB4Extraction.default_min_timestamp
            ),
            max_timestamp_custom_schema=options.get(
                "max_timestamp_custom_schema",
                SAPB4Extraction.max_timestamp_custom_schema,
            ),
            generate_predicates=self._input_spec.generate_predicates,
            predicates=options.get("predicates", SAPB4Extraction.predicates),
            predicates_add_null=self._input_spec.predicates_add_null,
            extra_cols_req_status_tbl=options.get(
                "extra_cols_req_status_tbl", SAPB4Extraction.extra_cols_req_status_tbl
            ),
            calc_upper_bound_schema=self._input_spec.calc_upper_bound_schema,
            include_changelog_tech_cols=options.get(
                "include_changelog_tech_cols",
                (options["adso_type"] != ADSOTypes.AQ.value),
            ),
        )
        return SAPB4ExtractionUtils(jdbc_extraction)

    def _get_options(self) -> tuple[dict, dict]:
        """Get Spark Options using JDBC utilities.

        Returns:
            A tuple dict containing the options args and
            jdbc args to be passed to Spark.
        """
        self._LOGGER.info(
            f"Initial options passed to the SAP B4 Reader: {self._input_spec.options}"
        )

        options = self._get_input_options()

        options_args, jdbc_args = self.jdbc_utils.get_spark_jdbc_options()

        if self._input_spec.generate_predicates or options.get("predicates", None):
            options_args.update(
                self.jdbc_utils.get_additional_spark_options(
                    self._input_spec,
                    options_args,
                    ["partitionColumn", "numPartitions", "lowerBound", "upperBound"],
                )
            )
        else:
            if self._input_spec.calculate_upper_bound:
                options_args["upperBound"] = (
                    self.jdbc_utils.get_spark_jdbc_optimal_upper_bound()
                )

            options_args.update(
                self.jdbc_utils.get_additional_spark_options(
                    self._input_spec, options_args
                )
            )

        self._LOGGER.info(
            f"Final options to fill SAP B4 Reader Options: {options_args}"
        )
        self._LOGGER.info(f"Final jdbc args to fill SAP B4 Reader JDBC: {jdbc_args}")
        return options_args, jdbc_args
