"""Expectation to check if column 'a' equals 'b', or 'c'."""
from typing import Any, Dict, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)

from lakehouse_engine.utils.expectations_utils import validate_result


class MulticolumnCustomMetric(MulticolumnMapMetricProvider):
    """Expectation metric definition.

    This expectation asserts that column 'a' must equal to column 'b' or column 'c'.
    In addition to this it is possible to validate that column 'b' or 'c' match a regex.
    """

    condition_metric_name = "multicolumn_values.column_a_must_equal_b_or_c"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "ignore_row_if",
    )

    condition_value_keys = ("validation_regex_b", "validation_regex_c")

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(
        self: MulticolumnMapMetricProvider, column_list: list, **kwargs: dict
    ) -> Any:
        validation_regex_b = (
            kwargs.get("validation_regex_b") if "validation_regex_b" in kwargs else ".*"
        )
        validation_regex_c = (
            kwargs.get("validation_regex_c") if "validation_regex_c" in kwargs else ".*"
        )

        return (column_list[0].isNotNull()) & (
            (
                column_list[1].isNotNull()
                & (column_list[1].rlike(validation_regex_b))
                & (column_list[0] == column_list[1])
            )
            | (
                (column_list[1].isNull())
                & (column_list[2].rlike(validation_regex_c))
                & (column_list[0] == column_list[2])
            )
        )


class ExpectMulticolumnColumnAMustEqualBOrC(MulticolumnMapExpectation):
    """MultiColumn Expectation.

    Expect that the column 'a' is equal to 'b' when this is
    not empty; otherwise 'a' must be equal to 'c'.

    Args:
        column_list: The column names to evaluate.

    Keyword Args:
        ignore_row_if: default to "never".
        result_format:  Which output mode to use:
           `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
           Default set to `BASIC`.
        include_config: If True, then include the expectation
           config as part of the result object.
           Default set to True.
        catch_exceptions: If True, then catch exceptions
           and include them as part of the result object.
           Default set to False.

    Returns:
        An ExpectationSuiteValidationResult.
    """

    examples = [
        {
            "dataset_name": "Test Dataset",
            "data": [
                {
                    "data": {
                        "a": ["d001", "1000", "1001"],
                        "b": [None, "1000", "1001"],
                        "c": ["d001", "d002", "d002"],
                        "d": ["d001", "d002", "1001"],
                    },
                    "schemas": {
                        "spark": {
                            "a": "StringType",
                            "b": "StringType",
                            "c": "StringType",
                            "d": "StringType",
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
                        "column_list": ["d", "b", "c"],
                        "validation_regex_c": "d[0-9]{3}$",
                        "result_format": {
                            "result_format": "COMPLETE",
                            "unexpected_index_column_names": ["d", "b", "c"],
                        },
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [
                            {
                                "d": "d002",
                                "b": "1000",
                                "c": "d002",
                            }
                        ],
                    },
                },
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_list": ["a", "b", "c"],
                        "validation_regex_c": "d[0-9]{3}$",
                        "result_format": {
                            "result_format": "COMPLETE",
                            "unexpected_index_column_names": ["a", "b", "c"],
                        },
                    },
                    "out": {"success": True},
                },
            ],
        },
    ]

    map_metric = "multicolumn_values.column_a_must_equal_b_or_c"
    success_keys = (
        "validation_regex_b",
        "validation_regex_c",
        "mostly",
    )
    default_kwarg_values = {
        "ignore_row_if": "never",
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "mostly": 1,
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


if __name__ == "__main__":
    # test the custom expectation with the function `print_diagnostic_checklist()`
    ExpectMulticolumnColumnAMustEqualBOrC().print_diagnostic_checklist()
