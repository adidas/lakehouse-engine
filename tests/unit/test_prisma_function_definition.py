"""Test the manual definition of dq functions when using prisma dq framework."""

import pytest

from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.dq_processors.exceptions import DQSpecMalformedException
from lakehouse_engine.utils.dq_utils import DQUtils


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "name": "Error: missing meta parameters",
            "dq_spec": {
                "dq_functions": [
                    {
                        "function": "expect_column_to_exist",
                        "args": {
                            "column": "test_column",
                            "meta": {
                                "table": "test_table",
                                "execution_point": "in_motion",
                            },
                        },
                    },
                ],
            },
            "expected": "The dq function meta field must contain all the "
            "fields defined"
            ": ['dq_rule_id', 'execution_point', 'filters', 'schema', "
            "'table', 'column', 'dimension'].\n"
            "Found fields: ['table', 'execution_point'].\n"
            "Diff: ['column', 'dimension', 'dq_rule_id', 'filters', 'schema']",
        },
        {
            "name": "Error: missing meta",
            "dq_spec": {
                "dq_functions": [
                    {
                        "function": "expect_column_to_exist",
                        "args": {
                            "column": "test_column",
                        },
                    },
                ],
            },
            "expected": "The dq function must have a meta field containing all the "
            "fields defined: ['dq_rule_id', "
            "'execution_point', 'filters', 'schema', 'table', 'column', "
            "'dimension'].",
        },
        {
            "name": "Definition of DQ Functions",
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
                            },
                        },
                    },
                ],
            },
            "expected": None,
        },
        {
            "name": "Definition of DQ Functions with extra params",
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
                ],
            },
            "expected": None,
        },
    ],
)
def test_prisma_manual_function_definition(scenario: dict) -> None:
    """Test the manual definition of dq functions when using prisma dq framework.

    Args:
        scenario (dict): The test scenario.
    """
    dq_spec = scenario["dq_spec"]
    if "Error: " in scenario["name"]:
        with pytest.raises(DQSpecMalformedException) as e:
            DQUtils.validate_dq_functions(
                spec=dq_spec,
                execution_point="in_motion",
                extra_meta_arguments=ExecEnv.ENGINE_CONFIG.dq_functions_column_list,
            )
        assert str(e.value) == scenario["expected"]
    else:
        DQUtils.validate_dq_functions(
            spec=dq_spec,
            execution_point="in_motion",
            extra_meta_arguments=ExecEnv.ENGINE_CONFIG.dq_functions_column_list,
        )
