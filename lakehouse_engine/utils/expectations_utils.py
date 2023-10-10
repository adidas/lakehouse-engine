"""Utilities to be used by custom expectations."""
from typing import Dict

from great_expectations.core import ExpectationConfiguration
from great_expectations.expectations.expectation import Expectation


def validate_result(
    expectation: Expectation,
    configuration: ExpectationConfiguration,
    metrics: Dict,
    partial_success: bool = True,
    partial_result: dict = None,
) -> dict:
    """Validates the test results of the custom expectations.

    If you need to make additional validations on your custom expectation
    and/or require additional fields to be returned you can add them before
    calling this function. The partial_success and partial_result
    optional parameters can be used to pass the result of additional
    validations and add more information to the result key of the
    returned dict respectively.

    Args:
        expectation: Expectation to validate.
        configuration: Configuration used in the test.
        metrics: Test result metrics.
        partial_success: Result of validations done before calling this method.
        partial_result: Extra fields to be returned to the user.

    Returns:
         The result of the validation.
    """
    if partial_result is None:
        partial_result = {}

    example_unexpected_index_list = _get_example_unexpected_index_list(
        expectation, configuration
    )

    test_unexpected_index_list = _get_test_unexpected_index_list(
        expectation.map_metric, metrics
    )

    if example_unexpected_index_list != test_unexpected_index_list:
        raise AssertionError(
            f"Example unexpected_index_list: {example_unexpected_index_list}\n"
            f"Test unexpected_index_list: {test_unexpected_index_list}"
        )

    unexpected_row_count = metrics[f"{expectation.map_metric}.unexpected_count"]
    total_rows = metrics["table.row_count"]

    mostly = expectation.get_success_kwargs(configuration).get("mostly", 1)

    success = (unexpected_row_count <= total_rows * (1 - mostly)) and partial_success

    metric_value = success
    if expectation.map_metric in metrics:
        metric_value = metrics[expectation.map_metric]

    result = {
        "observed_value": metric_value,
        "unexpected_row_count": unexpected_row_count,
        "unexpected_index_list": test_unexpected_index_list,
    }

    if "unexpected_values" in metrics:
        result["unexpected_values"] = metrics[
            f"{expectation.map_metric}.unexpected_values"
        ]

    return {"success": success, "result": {**result, **partial_result}}


def _get_example_unexpected_index_list(
    expectation: Expectation, configuration: ExpectationConfiguration
) -> list:
    """Retrieves the unexpected index list defined from the example used on the test.

    This needs to be done manually because GE allows us to get either the complete
    output of the test or the complete configuration used on the test.
    To get around this limitation this function is used to fetch the example used
    in the test directly from the expectation itself.

    Args:
        expectation: Expectation to fetch the examples.
        configuration: Configuration used in the test.

    Returns:
         List of unexpected indexes defined in the example used.
    """
    filtered_example: dict = {"out": {"unexpected_index_list": []}}

    for example in expectation.examples:
        for test in example["tests"]:  # type: ignore
            example_result_format = []
            if "result_format" in configuration["kwargs"]:
                example_result_format = configuration["kwargs"]["result_format"]

            if test["in"]["result_format"] == example_result_format:
                filtered_example = test

    example_unexpected_index_list = []
    if "unexpected_index_list" in filtered_example["out"]:
        example_unexpected_index_list = filtered_example["out"]["unexpected_index_list"]

    return example_unexpected_index_list


def _get_test_unexpected_index_list(metric_name: str, metrics: Dict) -> list:
    """Retrieves the unexpected index list from the test case that has been run.

    Args:
        metric_name: Name of the metric to retrieve the unexpected index list.
        metrics: Metric values resulting from the test.

    Returns:
         List of unexpected indexes retrieved form the test.
    """
    test_unexpected_index_list = []
    if f"{metric_name}.unexpected_index_list" in metrics:
        if metrics[f"{metric_name}.unexpected_index_list"]:
            test_unexpected_index_list = metrics[f"{metric_name}.unexpected_index_list"]
        else:
            test_unexpected_index_list = []

    return test_unexpected_index_list
