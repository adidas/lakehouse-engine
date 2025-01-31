"""Expectation to check if column value is not null or empty string."""

from typing import Any, Dict, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import ColumnMapMetricProvider
from great_expectations.expectations.metrics.map_metric_provider import (
    column_condition_partial,
)

from lakehouse_engine.utils.expectations_utils import validate_result


class ColumnValuesNotNullOrEpmtyString(ColumnMapMetricProvider):
    """Asserts that column values are not null or empty string."""

    condition_metric_name = "column_values.not_null_or_empty_string"
    filter_column_isnull = False
    condition_domain_keys = (
        "batch_id",
        "table",
        "column",
        "ignore_row_if",
    )
    condition_value_keys = ()

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(
        self: ColumnMapMetricProvider,
        column: Any,
        **kwargs: dict,
    ) -> Any:
        """Implementation of the expectation's logic.

        Args:
            column: Name of column to validate.
            kwargs: dict with additional parameters.

        Returns:
            If the condition is met.
        """
        return (column.isNotNull()) & (column != "")


class ExpectColumnValuesToNotBeNullOrEmptyString(ColumnMapExpectation):
    """Expect value in column to be not null or empty string.

    Args:
        column: Name of column to validate.
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
                            "4061622965678",
                            "4061622965679",
                            "4061622965680",
                        ],
                        "b": [
                            "4061622965678",
                            "",
                            "4061622965680",
                        ],
                    }
                }
            ],
            "schemas": {"spark": {"a": "StringType", "b": "StringType"}},
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "a",
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
                        "result_format": {
                            "result_format": "COMPLETE",
                            "unexpected_index_column_names": ["a"],
                        },
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [
                            {
                                "a": "4061622965679",
                                "b": "",
                            }
                        ],
                    },
                },
            ],
        },
    ]

    map_metric = "column_values.not_null_or_empty_string"
    success_keys = (
        "column",
        "ignore_row_if",
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
    ) -> Any:
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
        return validate_result(
            self,
            configuration,
            metrics,
            runtime_configuration,
            execution_engine,
            ColumnMapExpectation,
        )


"""Mandatory block of code. If it is removed the expectation will not be available."""
if __name__ == "__main__":
    # test the custom expectation with the function `print_diagnostic_checklist()`
    ExpectColumnValuesToNotBeNullOrEmptyString().print_diagnostic_checklist()
