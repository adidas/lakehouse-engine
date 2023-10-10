"""Module containing the definition of a data assistant."""
from json import dumps
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import ydata_profiling as pp
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.rule_based_profiler.data_assistant_result import (
    OnboardingDataAssistantResult,
)
from IPython.display import HTML, display
from pyspark.sql import DataFrame

from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.storage.local_fs_storage import LocalFSStorage
from lakehouse_engine.utils.storage.s3_storage import S3Storage


class Assistant(object):
    """Class containing the data assistant."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    @classmethod
    def run_data_assistant(
        cls,
        context: BaseDataContext,
        batch_request: RuntimeBatchRequest,
        expectation_suite_name: str,
        assistant_options: dict,
        data: DataFrame,
        profile_file_name: str,
    ) -> Any:
        """Entrypoint to run the data assistant.

        Based on the data, it uses GE Onboarding Data Assistant to generate expectations
        that can be applied to the data. Then, it returns the generated expectations
        and, depending on your configuration, it can display plots of the metrics,
        expectations and also display or store the profiling of the data, for you to get
        a better sense of it.

        Args:
            context: the BaseDataContext containing the configurations for the data
            source and store backend.
            batch_request: batch request to be able to query underlying data.
            expectation_suite_name: name of the expectation suite.
            assistant_options: additional options to pass to the DQ assistant processor.
            data: the input dataframe for which the DQ is running.
            profile_file_name: file name for storing the profiling html file.

        Returns:
            The context with the expectation suite stored.
        """
        data_assistant_result = context.assistants.onboarding.run(
            batch_request=batch_request,
            estimation=assistant_options.get("estimation", "exact"),
            include_column_names=assistant_options.get("include_column_names", None),
            exclude_column_names=assistant_options.get("exclude_column_names", None),
            include_column_name_suffixes=assistant_options.get(
                "include_column_name_suffixes", None
            ),
            exclude_column_name_suffixes=assistant_options.get(
                "exclude_column_name_suffixes", None
            ),
            cardinality_limit_mode=assistant_options.get(
                "cardinality_limit_mode", None
            ),
        )

        if assistant_options.get("plot_expectations_and_metrics", False):
            data_assistant_result.plot_expectations_and_metrics()

        if assistant_options.get("plot_metrics", False):
            data_assistant_result.plot_metrics()

        expectation_suite = data_assistant_result.get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )

        cls._return_expectations(
            data_assistant_result, assistant_options.get("log_expectations", False)
        )

        if assistant_options.get("display_profiling", False) or assistant_options.get(
            "profiling_path", None
        ):
            Assistant._return_profiling(
                data,
                assistant_options.get("profiling_limit", 1000000),
                assistant_options,
                profile_file_name,
            )

        return context.add_or_update_expectation_suite(
            expectation_suite=expectation_suite
        )

    @classmethod
    def _return_expectations(
        cls,
        data_assistant_result: OnboardingDataAssistantResult,
        log_expectations: bool,
    ) -> None:
        """Log and/or display a Pandas DF with the generated expectations.

        Args:
            data_assistant_result: the result of the onboarding data assistant.
            log_expectations: whether to log expectations to the console or not.
            Can be useful for scenarios in which display won't work (e.g. local tests).
        """
        expectations = []
        for expectation_config in data_assistant_result.expectation_configurations:
            expectation = {
                "expectation_type": expectation_config["expectation_type"],
                "args": dumps(expectation_config["kwargs"]),
                "dq_function": dumps(
                    {
                        "function": expectation_config["expectation_type"],
                        "args": expectation_config["kwargs"],
                    }
                ),
            }
            expectations.append(expectation)

        rdd = ExecEnv.SESSION.sparkContext.parallelize([dumps(expectations)])
        expectations_df = ExecEnv.SESSION.read.json(rdd)

        cls._LOGGER.info(
            f"{len(expectations)} expectations were generated by the Onboarding Data "
            f"Assistant. "
            "Based on these suggestions you can pick the expectations and arguments "
            "that you think would be important for your production pipelines. You just "
            "need to grab them from the dq_function column and put it as part of your "
            "dq_functions in your dq_specs. Note: keep in mind that you should be "
            "selective with the amount of expectations to run, as this impacts "
            "performance. You can also complement this analysis by checking Great "
            "Expectations documentation: https://greatexpectations.io/expectations."
        )
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_rows", None)
        pd.set_option("max_colwidth", None)
        if log_expectations:
            cls._LOGGER.info(expectations)
        display(
            expectations_df.select("expectation_type", "args", "dq_function").toPandas()
        )

    @classmethod
    def _return_profiling(
        cls, df: DataFrame, limit: int, assistant_options: dict, profile_file_name: str
    ) -> None:
        """Generate and returns (display and/or store) a data profiling report.

        Args:
            df: dataframe to profile.
            limit: limit to filter the dataframe.
            assistant_options: additional options to pass to the DQ assistant processor.
            profile_file_name: file name for storing the profiling html file.
        """
        cls._LOGGER.info("Start Profiling with Pandas")
        profile = pp.ProfileReport(df.limit(limit).toPandas()).html

        if assistant_options.get("display_profiling", False):
            display(HTML(profile))

        profiling_path = assistant_options.get("profiling_path", None)
        if profiling_path:
            url = urlparse(
                f"{profiling_path}/{profile_file_name}.html", allow_fragments=False
            )
            if profiling_path.startswith("s3://"):
                S3Storage.write_payload_to_file(url, profile)
            else:
                LocalFSStorage.write_payload_to_file(url, profile)
        cls._LOGGER.info("Finished Profiling with Pandas")
