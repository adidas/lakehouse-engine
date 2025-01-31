"""Expectation to check if column 'a' is not equal to column 'b'."""

from typing import Any, Dict, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.expectation import ColumnPairMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)

from lakehouse_engine.utils.expectations_utils import validate_result


class ColumnPairCustom(ColumnPairMapMetricProvider):
    """Asserts that column 'A' is not equal to column 'B'.

    Additionally, It compares Null as well.
    """

    condition_metric_name = "column_pair_values.a_not_equal_to_b"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_A",
        "column_B",
        "ignore_row_if",
    )
    condition_value_keys = ()

    @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(
        self: ColumnPairMapMetricProvider,
        column_A: Any,
        column_B: Any,
        **kwargs: dict,
    ) -> Any:
        """Implementation of the expectation's logic.

        Args:
            column_A: Value of the row of column_A.
            column_B: Value of the row of column_B.
            kwargs: dict with additional parameters.

        Returns:
            If the condition is met.
        """
        return ((column_A.isNotNull()) | (column_B.isNotNull())) & (
            column_A != column_B
        )  # noqa: E501


class ExpectColumnPairAToBeNotEqualToB(ColumnPairMapExpectation):
    """Expect values in column A to be not equal to column B.

    Args:
        column_A: The first column name.
        column_B: The second column name.

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
                        "a": ["IE4019", "IM6092", "IE1405"],
                        "b": ["IE4019", "IM6092", "IE1405"],
                        "c": ["IE1404", "IN6192", "842075"],
                    },
                    "schemas": {
                        "spark": {
                            "a": "StringType",
                            "b": "StringType",
                            "c": "StringType",
                        }
                    },
                }
            ],
            "tests": [
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "a",
                        "column_B": "b",
                        "result_format": {
                            "result_format": "COMPLETE",
                            "unexpected_index_column_names": ["b"],
                        },
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [
                            {"b": "IE4019", "a": "IE4019"},
                            {"b": "IM6092", "a": "IM6092"},
                            {"b": "IE1405", "a": "IE1405"},
                        ],
                    },
                },
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "a",
                        "column_B": "c",
                        "result_format": {
                            "result_format": "COMPLETE",
                            "unexpected_index_column_names": ["a"],
                        },
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                    },
                },
            ],
        },
    ]

    map_metric = "column_pair_values.a_not_equal_to_b"
    success_keys = (
        "column_A",
        "column_B",
        "ignore_row_if",
        "mostly",
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
            ColumnPairMapExpectation,
        )


"""Mandatory block of code. If it is removed the expectation will not be available."""
if __name__ == "__main__":
    # test the custom expectation with the function `print_diagnostic_checklist()`
    ExpectColumnPairAToBeNotEqualToB().print_diagnostic_checklist()
