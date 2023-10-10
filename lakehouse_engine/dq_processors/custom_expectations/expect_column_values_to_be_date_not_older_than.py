"""Expectation to check if column value is a date within a timeframe."""
import datetime
from datetime import timedelta
from typing import Any, Dict, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import ColumnMapMetricProvider
from great_expectations.expectations.metrics.map_metric_provider import (
    column_condition_partial,
)

from lakehouse_engine.utils.expectations_utils import validate_result


class ColumnValuesDateNotOlderThan(ColumnMapMetricProvider):
    """Asserts that column values are a date that isn't older than a given date."""

    condition_metric_name = "column_values.date_is_not_older_than"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column",
        "ignore_row_if",
    )
    condition_value_keys = ("timeframe",)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(
        self: ColumnMapMetricProvider,
        column: Any,
        timeframe: Any,
        **kwargs: dict,
    ) -> Any:
        """Implementation of the expectation's logic.

        Since timedelta can only define an interval up to weeks, a month is defined
        as 4 weeks and a year is defined as 52 weeks.

        Args:
            column: Name of column to validate.
            timeframe: dict with the definition of the timeframe.
            kwargs: dict with additional parameters.

        Returns:
            If the condition is met.
        """
        weeks = (
            timeframe.get("weeks", 0)
            + (timeframe.get("months", 0) * 4)
            + (timeframe.get("years", 0) * 52)
        )

        delta = timedelta(
            days=timeframe.get("days", 0),
            seconds=timeframe.get("seconds", 0),
            microseconds=timeframe.get("microseconds", 0),
            milliseconds=timeframe.get("milliseconds", 0),
            minutes=timeframe.get("minutes", 0),
            hours=timeframe.get("hours", 0),
            weeks=weeks,
        )

        return delta > (datetime.datetime.now() - column)


class ExpectColumnValuesToBeDateNotOlderThan(ColumnMapExpectation):
    """Expect value in column to be date that is not older than a given time.

    Since timedelta can only define an interval up to weeks, a month is defined
    as 4 weeks and a year is defined as 52 weeks.

    Args:
        column: Name of column to validate
        Note: Column must be of type Date, Timestamp or String (with Timestamp format).
        Format: yyyy-MM-ddTHH:mm:ss
        timeframe: dict with the definition of the timeframe.
        kwargs: dict with additional parameters.

    Keyword Args:
        allow_cross_type_comparisons: If True, allow
            comparisons between types (e.g. integer and string).
            Otherwise, attempting such comparisons will raise an exception.
        ignore_row_if: "both_values_are_missing",
            "either_value_is_missing", "neither" (default).
        result_format: Which output mode to use:
            `BOOLEAN_ONLY`, `BASIC` (default), `COMPLETE`, or `SUMMARY`.
        include_config: If True (default), then include the expectation config
            as part of the result object.
        catch_exceptions: If True, then catch exceptions and
            include them as part of the result object. Default: False.
        meta: A JSON-serializable dictionary (nesting allowed)
            that will be included in the output without modification.

    Returns:
        An ExpectationSuiteValidationResult.
    """

    examples = [
        {
            "dataset_name": "Test Dataset",
            "data": [
                {
                    "data": {
                        "a": [
                            datetime.datetime(2023, 6, 1, 12, 0, 0),
                            datetime.datetime(2023, 6, 2, 12, 0, 0),
                            datetime.datetime(2023, 6, 3, 12, 0, 0),
                        ],
                        "b": [
                            datetime.datetime(1800, 6, 1, 12, 0, 0),
                            datetime.datetime(2023, 6, 2, 12, 0, 0),
                            datetime.datetime(1800, 6, 3, 12, 0, 0),
                        ],
                    }
                }
            ],
            "schemas": {"spark": {"a": "TimestampType", "b": "TimestampType"}},
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "a",
                        "timeframe": {"years": 100},
                        "result_format": {
                            "result_format": "BASIC",
                            "unexpected_index_column_names": ["b"],
                        },
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                    },
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "b",
                        "timeframe": {"years": 100},
                        "result_format": {
                            "result_format": "COMPLETE",
                            "unexpected_index_column_names": ["a"],
                        },
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [
                            {
                                "a": datetime.datetime(2023, 6, 1, 12, 0),
                                "b": datetime.datetime(1800, 6, 1, 12, 0),
                            },
                            {
                                "a": datetime.datetime(2023, 6, 3, 12, 0),
                                "b": datetime.datetime(1800, 6, 3, 12, 0),
                            },
                        ],
                    },
                },
            ],
        },
    ]

    map_metric = "column_values.date_is_not_older_than"
    success_keys = (
        "column",
        "ignore_row_if",
        "timeframe",
    )
    default_kwarg_values = {
        "mostly": 1.0,
        "ignore_row_if": "neither",
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ) -> dict:
        """Custom implementation of the GE _validate method.

        This method is used on the tests to validate both the result
        of the tests themselves and if the unexpected index list
        is correctly generated.
        The GE test logic does not do this validation, and thus
        we need to make it manually.

        Args:
            configuration: Configuration used in the test.
            metrics: Test result metrics.
            runtime_configuration: Configuration used when running the expectation.
            execution_engine: Execution Engine where the expectation was run.

        Returns:
            Dictionary with the result of the validation.
        """
        return validate_result(self, configuration, metrics)


"""Mandatory block of code. If it is removed the expectation will not be available."""
if __name__ == "__main__":
    # test the custom expectation with the function `print_diagnostic_checklist()`
    ExpectColumnValuesToBeDateNotOlderThan().print_diagnostic_checklist()
