"""Utilities to be used by custom expectations."""

from typing import Any, Dict


def validate_result(
    expectation_configuration: Any,
    metrics: dict,
) -> None:
    """Validates that the unexpected_index_list in the tests is corretly defined.

    Additionally, it validates the expectation using the GE _validate method.

    Args:
        expectation_configuration: Expectation configuration.
        metrics: Test result metrics.
        runtime_configuration: Configuration used when running the expectation.
        execution_engine: Execution engine used in the expectation.
        base_expectation: Base expectation to validate.
    """
    example_unexpected_index_list = _get_example_unexpected_index_list(
        expectation_configuration
    )

    test_unexpected_index_list = _get_test_unexpected_index_list(
        expectation_configuration.map_metric, metrics
    )
    if example_unexpected_index_list:
        if example_unexpected_index_list != test_unexpected_index_list:
            raise AssertionError(
                f"Example unexpected_index_list: {example_unexpected_index_list}\n"
                f"Test unexpected_index_list: {test_unexpected_index_list}"
            )


def _get_example_unexpected_index_list(expectation_configuration: Any) -> list:
    """Retrieves the unexpected index list defined from the example used on the test.

    This needs to be done manually because GE allows us to get either the complete
    output of the test or the complete configuration used on the test.
    To get around this limitation this function is used to fetch the example used
    in the test directly from the expectation itself.

    Args:
        expectation_configuration: Expectation configuration.

    Returns:
        List of unexpected indexes defined in the example used.
    """
    filtered_example: dict = {"out": {"unexpected_index_list": []}}

    for example in expectation_configuration.examples:
        for test in example["tests"]:  # type: ignore
            example_result_format = []
            if "result_format" in expectation_configuration.result_format:
                example_result_format = expectation_configuration.result_format

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
