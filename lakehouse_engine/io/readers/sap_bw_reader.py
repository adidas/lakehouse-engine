"""Module to define behaviour to read from SAP BW sources."""

from logging import Logger

from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import InputSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.reader import Reader
from lakehouse_engine.utils.extraction.sap_bw_extraction_utils import (
    SAPBWExtraction,
    SAPBWExtractionUtils,
)
from lakehouse_engine.utils.logging_handler import LoggingHandler


class SAPBWReader(Reader):
    """Class to read from SAP BW source."""

    _LOGGER: Logger = LoggingHandler(__name__).get_logger()

    def _get_input_options(self) -> dict:
        """Get reader options ensuring they are available."""
        if not self._input_spec.options:
            raise ValueError("options must be provided for SAP BW reader.")
        return self._input_spec.options

    def __init__(self, input_spec: InputSpec) -> None:
        """Construct SAPBWReader instances.

        Args:
            input_spec: input specification.
        """
        super().__init__(input_spec)
        self.jdbc_utils = self._get_jdbc_utils()

    def read(self) -> DataFrame:
        """Read data from SAP BW source.

        Returns:
            A dataframe containing the data from the SAP BW source.
        """
        options_args, jdbc_args = self._get_options()
        return ExecEnv.SESSION.read.options(**options_args).jdbc(**jdbc_args)

    def _get_jdbc_utils(self) -> SAPBWExtractionUtils:
        options = self._get_input_options()
        jdbc_extraction = SAPBWExtraction(
            user=options["user"],
            password=options["password"],
            url=options["url"],
            dbtable=options["dbtable"],
            latest_timestamp_data_location=options.get(
                "latest_timestamp_data_location",
                SAPBWExtraction.latest_timestamp_data_location,
            ),
            latest_timestamp_input_col=options.get(
                "latest_timestamp_input_col", SAPBWExtraction.latest_timestamp_input_col
            ),
            latest_timestamp_data_format=options.get(
                "latest_timestamp_data_format",
                SAPBWExtraction.latest_timestamp_data_format,
            ),
            extraction_type=options.get(
                "extraction_type", SAPBWExtraction.extraction_type
            ),
            act_request_table=options.get(
                "act_request_table", SAPBWExtraction.act_request_table
            ),
            request_col_name=options.get(
                "request_col_name", SAPBWExtraction.request_col_name
            ),
            act_req_join_condition=options.get(
                "act_req_join_condition", SAPBWExtraction.act_req_join_condition
            ),
            driver=options.get("driver", SAPBWExtraction.driver),
            changelog_table=options.get(
                "changelog_table", SAPBWExtraction.changelog_table
            ),
            num_partitions=options.get("numPartitions", SAPBWExtraction.num_partitions),
            partition_column=options.get(
                "partitionColumn", SAPBWExtraction.partition_column
            ),
            lower_bound=options.get("lowerBound", SAPBWExtraction.lower_bound),
            upper_bound=options.get("upperBound", SAPBWExtraction.upper_bound),
            default_upper_bound=options.get(
                "default_upper_bound", SAPBWExtraction.default_upper_bound
            ),
            fetch_size=options.get("fetchSize", SAPBWExtraction.fetch_size),
            compress=options.get("compress", SAPBWExtraction.compress),
            custom_schema=options.get("customSchema", SAPBWExtraction.custom_schema),
            extraction_timestamp=options.get(
                "extraction_timestamp",
                SAPBWExtraction.extraction_timestamp,
            ),
            odsobject=options.get(
                "odsobject",
                SAPBWExtractionUtils.get_odsobject(options),
            ),
            min_timestamp=options.get("min_timestamp", SAPBWExtraction.min_timestamp),
            max_timestamp=options.get("max_timestamp", SAPBWExtraction.max_timestamp),
            default_max_timestamp=options.get(
                "default_max_timestamp", SAPBWExtraction.default_max_timestamp
            ),
            default_min_timestamp=options.get(
                "default_min_timestamp", SAPBWExtraction.default_min_timestamp
            ),
            max_timestamp_custom_schema=options.get(
                "max_timestamp_custom_schema",
                SAPBWExtraction.max_timestamp_custom_schema,
            ),
            include_changelog_tech_cols=options.get(
                "include_changelog_tech_cols",
                SAPBWExtraction.include_changelog_tech_cols,
            ),
            generate_predicates=self._input_spec.generate_predicates,
            predicates=options.get("predicates", SAPBWExtraction.predicates),
            predicates_add_null=self._input_spec.predicates_add_null,
            extra_cols_act_request=options.get(
                "extra_cols_act_request", SAPBWExtraction.extra_cols_act_request
            ),
            get_timestamp_from_act_request=options.get(
                "get_timestamp_from_act_request",
                SAPBWExtraction.get_timestamp_from_act_request,
            ),
            calc_upper_bound_schema=self._input_spec.calc_upper_bound_schema,
            sap_bw_schema=options.get("sap_bw_schema", SAPBWExtraction.sap_bw_schema),
            ods_prefix=options.get("ods_prefix", SAPBWExtraction.ods_prefix),
            logsys=options.get("logsys", SAPBWExtraction.logsys),
        )
        return SAPBWExtractionUtils(jdbc_extraction)

    def _get_options(self) -> tuple[dict, dict]:
        """Get Spark Options using JDBC utilities.

        Returns:
            A tuple dict containing the options args and
            jdbc args to be passed to Spark.
        """
        self._LOGGER.info(
            f"Initial options passed to the SAP BW Reader: {self._input_spec.options}"
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
            f"Final options to fill SAP BW Reader Options: {options_args}"
        )
        self._LOGGER.info(f"Final jdbc args to fill SAP BW Reader JDBC: {jdbc_args}")
        return options_args, jdbc_args
