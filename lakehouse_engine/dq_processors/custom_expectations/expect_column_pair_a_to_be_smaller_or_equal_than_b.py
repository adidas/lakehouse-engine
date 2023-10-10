"""Expectation to check if column 'a' is lower or equal than column 'b'."""
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
    """Asserts that column 'A' is lower or equal than column 'B'.

    Additionally, the 'margin' parameter can be used to add a margin to the
    check between column 'A' and 'B': 'A' <= 'B' + 'margin'.
    """

    condition_metric_name = "column_pair_values.a_smaller_or_equal_than_b"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_A",
        "column_B",
        "ignore_row_if",
    )
    condition_value_keys = ("margin",)

    @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(
        self: ColumnPairMapMetricProvider,
        column_A: Any,
        column_B: Any,
        margin: Any,
        **kwargs: dict,
    ) -> Any:
        """Implementation of the expectation's logic.

        Args:
            column_A: Value of the row of column_A.
            column_B: Value of the row of column_B.
            margin: margin value to be added to column_b.
            kwargs: dict with additional parameters.

        Returns:
            If the condition is met.
        """
        if margin is None:
            approx = 0
        elif not isinstance(margin, (int, float, complex)):
            raise TypeError(
                f"margin must be one of int, float, complex."
                f" Found: {margin} as {type(margin)}"
            )
        else:
            approx = margin  # type: ignore

        return column_A <= column_B + approx  # type: ignore


class ExpectColumnPairAToBeSmallerOrEqualThanB(ColumnPairMapExpectation):
    """Expect values in column A to be lower or equal than column B.

    Args:
        column_A: The first column name.
        column_B: The second column name.
        margin: additional approximation to column B value.

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
                        "a": [11, 22, 50],
                        "b": [10, 21, 100],
                        "c": [9, 21, 30],
                    },
                    "schemas": {
                        "spark": {
                            "a": "IntegerType",
                            "b": "IntegerType",
                            "c": "IntegerType",
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
                        "column_B": "c",
                        "result_format": {
                            "result_format": "COMPLETE",
                            "unexpected_index_column_names": ["c"],
                        },
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [
                            {"c": 9, "a": 11},
                            {"c": 21, "a": 22},
                            {"c": 30, "a": 50},
                        ],
                    },
                },
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "a",
                        "column_B": "b",
                        "margin": 1,
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

    map_metric = "column_pair_values.a_smaller_or_equal_than_b"
    success_keys = (
        "column_A",
        "column_B",
        "ignore_row_if",
        "margin",
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
    ExpectColumnPairAToBeSmallerOrEqualThanB().print_diagnostic_checklist()
