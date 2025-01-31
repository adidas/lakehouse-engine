"""Expectation to check if date column 'a' is greater or equal to date column 'b'."""

import datetime
from typing import Any, Dict, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.expectation import ColumnPairMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)

from lakehouse_engine.utils.expectations_utils import validate_result


# This class defines a Metric to support your Expectation
class ColumnPairDateAToBeGreaterOrEqualToDateB(ColumnPairMapMetricProvider):
    """Asserts that date column 'A' is greater or equal to date column 'B'."""

    # This is the id string that will be used to refer your metric.
    condition_metric_name = "column_pair_values.date_a_greater_or_equal_to_date_b"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_A",
        "column_B",
        "ignore_row_if",
    )

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
            Boolean on the basis of condition.
        """
        return (
            (column_A.isNotNull()) & (column_B.isNotNull()) & (column_A >= column_B)
        )  # type: ignore


class ExpectColumnPairDateAToBeGreaterThanOrEqualToDateB(ColumnPairMapExpectation):
    """Expect values in date column A to be greater than or equal to date column B.

    Args:
        column_A: The first date column name.
        column_B: The second date column name.

    Keyword Args:
        ignore_row_if: "both_values_are_missing",
            "either_value_is_missing", "neither" (default).
        result_format: Which output mode to use:
            `BOOLEAN_ONLY`, `BASIC` (default), `COMPLETE`, or `SUMMARY`.
        include_config: If True (default), then include the
            expectation config as part of the result object.
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
                            "2029-01-12",
                            "2024-11-21",
                            "2022-01-01",
                        ],
                        "b": [
                            "2019-02-11",
                            "2014-12-22",
                            "2012-09-09",
                        ],
                        "c": [
                            "2010-02-11",
                            "2015-12-22",
                            "2022-09-09",
                        ],
                    },
                    "schemas": {
                        "spark": {
                            "a": "DateType",
                            "b": "DateType",
                            "c": "DateType",
                        }
                    },
                }
            ],
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "a",
                        "column_B": "b",
                        "result_format": {
                            "result_format": "COMPLETE",
                            "unexpected_index_column_names": ["a", "b"],
                        },
                    },
                    "out": {"success": True, "unexpected_index_list": []},
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "b",
                        "column_B": "c",
                        "result_format": {
                            "result_format": "COMPLETE",
                            "unexpected_index_column_names": ["a"],
                        },
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [
                            {
                                "a": datetime.date(2024, 11, 21),
                                "b": datetime.date(2014, 12, 22),
                                "c": datetime.date(2015, 12, 22),
                            },
                            {
                                "a": datetime.date(2022, 1, 1),
                                "b": datetime.date(2012, 9, 9),
                                "c": datetime.date(2022, 9, 9),
                            },
                        ],
                    },
                },
            ],
        }
    ]

    map_metric = "column_pair_values.date_a_greater_or_equal_to_date_b"
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
        "catch_exceptions": True,
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
    ExpectColumnPairDateAToBeGreaterThanOrEqualToDateB().print_diagnostic_checklist()
