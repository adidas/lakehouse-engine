"""Test the manual definition of dq functions when using prisma dq framework."""

import pytest

from lakehouse_engine.core.definitions import DQFunctionSpec, DQSpec, DQType
from lakehouse_engine.utils.dq_utils import PrismaUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler

_LOGGER = LoggingHandler(__name__).get_logger()


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "name": "Definition of DQ Functions using parameters without duplicates",
            "spec_id": "spec_without_duplicates",
            "dq_spec": {
                "dq_functions": [
                    {
                        "function": "expect_column_to_exist",
                        "args": {
                            "column": "test_column",
                            "meta": {
                                "dq_rule_id": "rule_2",
                                "execution_point": "in_motion",
                                "schema": "test_db",
                                "table": "dummy_sales",
                                "column": "",
                                "dimension": "",
                                "filters": "",
                                "note": "Test Notes",
                            },
                        },
                    },
                    {
                        "function": "expect_column_to_exist",
                        "args": {
                            "column": "test_column",
                            "meta": {
                                "dq_rule_id": "rule_1",
                                "execution_point": "in_motion",
                                "schema": "test_db",
                                "table": "dummy_sales",
                                "column": "",
                                "dimension": "",
                                "filters": "",
                                "note": "Test Notes",
                            },
                        },
                    },
                    {
                        "function": "expect_column_to_exist",
                        "args": {
                            "column": "test_column",
                            "meta": {
                                "dq_rule_id": "rule_3",
                                "execution_point": "in_motion",
                                "schema": "test_db",
                                "table": "dummy_sales",
                                "column": "",
                                "dimension": "",
                                "filters": "",
                                "note": "Test Notes",
                            },
                        },
                    },
                ],
            },
        },
        {
            "name": "Error: Definition of DQ Functions using parameters "
            "with duplicates",
            "spec_id": "spec_with_duplicates",
            "dq_spec": {
                "dq_functions": [
                    {
                        "function": "expect_column_to_exist",
                        "args": {
                            "column": "test_column",
                            "meta": {
                                "dq_rule_id": "rule_2",
                                "execution_point": "in_motion",
                                "schema": "test_db",
                                "table": "dummy_sales",
                                "column": "",
                                "dimension": "",
                                "filters": "",
                                "note": "Test Notes",
                            },
                        },
                    },
                    {
                        "function": "expect_column_to_exist",
                        "args": {
                            "column": "test_column",
                            "meta": {
                                "dq_rule_id": "rule_1",
                                "execution_point": "in_motion",
                                "schema": "test_db",
                                "table": "dummy_sales",
                                "column": "",
                                "dimension": "",
                                "filters": "",
                                "note": "Test Notes",
                            },
                        },
                    },
                    {
                        "function": "expect_column_to_exist",
                        "args": {
                            "column": "test_column",
                            "meta": {
                                "dq_rule_id": "rule_2",
                                "execution_point": "in_motion",
                                "schema": "test_db",
                                "table": "dummy_sales",
                                "column": "",
                                "dimension": "",
                                "filters": "",
                                "note": "Test Notes",
                            },
                        },
                    },
                ],
            },
        },
    ],
)
def test_prisma_manual_function_definition(scenario: dict) -> None:
    """Test the manual definition of dq functions when using prisma dq framework.

    Args:
        scenario (dict): The test scenario.
    """
    dq_functions = [
        DQFunctionSpec(function=dq_function["function"], args=dq_function["args"])
        for dq_function in scenario["dq_spec"]["dq_functions"]
    ]

    dq_spec_list = [
        DQSpec(
            spec_id=scenario["spec_id"],
            input_id=scenario["name"],
            dq_type=DQType.PRISMA.value,
            dq_functions=dq_functions,
        )
    ]

    if "Error: " in scenario["name"]:
        error = PrismaUtils.validate_rule_id_duplication(specs=dq_spec_list)
        expected_error = {"dq_spec_id: spec_with_duplicates": "rule_2; rule_1; rule_2"}
        _LOGGER.critical(
            f"A duplicate dq_rule_id was found!!!"
            "Please verify the following list:"
            f"{error}"
        )
        assert error == expected_error
    else:
        PrismaUtils.validate_rule_id_duplication(specs=dq_spec_list)
