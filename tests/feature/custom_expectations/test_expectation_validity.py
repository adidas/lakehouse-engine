"""Module with the validation code for the custom expectations."""
import importlib
import re

import pytest

from lakehouse_engine.core.definitions import DQDefaults

"""This value '✔' is used to filter the output from the GE diagnostics"""
CHECKMARK = "\u2714"
DIAGNOSTICS_VALIDATIONS = [
    " ✔ Has a docstring, including a one-line short description",
    " ✔ Has at least one positive and negative example case, and all test cases pass",
    " ✔ Has core logic and passes tests on at least one Execution Engine",
    "    ✔ All [0-9]+ tests for spark are passing",
    " ✔ Has core logic that passes tests for all applicable Execution Engines and SQL"
    " dialects",
    "    ✔ All [0-9]+ tests for spark are passing",
]

METRIC_NAME_TYPES = [
    "column_values",
    "multicolumn_values",
    "column_pair_values",
    "table_rows",
    "table_columns",
]

MAP_METRICS = []


@pytest.mark.parametrize("expectation", DQDefaults.CUSTOM_EXPECTATION_LIST.value)
def test_expectation_validity(expectation: str) -> None:
    """Validates the custom expectations defined in the project.

    Based on the diagnostics of the custom expectations this test validates if all the
    best practices are being followed.
    """
    result, metric_name = _run_diagnostics(expectation)

    _process_diagnostics_output(result)

    if metric_name:
        assert _validate_metric_name_structure(metric_name), (
            f"Metric name {metric_name} has the incorrect format. "
            f"Should be 'metric type'.'metric_name'"
        )

        MAP_METRICS.append(metric_name)

        assert len(MAP_METRICS) == len(
            set(MAP_METRICS)
        ), f"Metric names repeated: {MAP_METRICS}"


def _run_diagnostics(expectation_name: str) -> tuple:
    """Runs the diagnostics of the custom expectation.

    This function both runs the Great Expectations Diagnostics and
    retrieves the diagnostics checklist and the metric name defined.

    Args:
        expectation_name: name of the expectation file.

    Returns:
        The output of the diagnostics command and the expectation's metric name.
    """
    segments = expectation_name.split(".")[0].split("_")
    expectation_class_name = "".join(ele.title() for ele in segments[0:])

    module = importlib.import_module(
        f"lakehouse_engine.dq_processors.custom_expectations.{expectation_name}"
    )
    expectation_class = getattr(module, expectation_class_name)
    expectation = expectation_class()

    metric_name = ""

    if "map_metric" in dir(expectation):
        metric_name = expectation.map_metric

    return expectation.run_diagnostics().generate_checklist(), metric_name


def _process_diagnostics_output(diagnostics_output: str) -> None:
    """Processes the output from the expectation diagnostics.

    Args:
        diagnostics_output: the output from the diagnostics command.
    """
    validations = DIAGNOSTICS_VALIDATIONS
    for line in str(diagnostics_output).split("\n"):
        if CHECKMARK in line:
            for validation in validations:
                if re.match(validation, line):
                    validations.remove(validation)
                    break

    assert not validations, f"Validations not met: {validations}"


def _validate_metric_name_structure(metric_name: str) -> int:
    """Validates the structure of the custom expectation's metric name.

    The metric name must have two parts separated by a '.',
    and the first part must be the type of the expectation.

    Args:
        metric_name: custom expectation's metric name.

    Returns:
        The validation of custom expectation's the metric name.
    """
    parts = metric_name.split(".")

    if len(parts) != 2:
        return False

    if parts[0] not in METRIC_NAME_TYPES:
        return False

    return True
