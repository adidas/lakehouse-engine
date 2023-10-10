"""Test custom expectation validations."""
from json import loads
from typing import Any, Tuple

import pytest
from pyspark.sql import DataFrame

from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.engine import execute_dq_validation
from lakehouse_engine.utils.schema_utils import SchemaUtils
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_NAME = "custom_expectations"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_NAME}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_NAME}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_NAME}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_NAME}"


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "expectation_name": "expect_column_pair_a_to_be_smaller_or_equal_than_b",
            "arguments": {
                "column_A": "salesorder",
                "column_B": "amount",
                "margin": 9.78,
            },
            "read_type": "batch",
            "input_type": "dataframe_reader",
            "custom_expectation_result": "success",
        },
        {
            "expectation_name": "expect_column_pair_a_to_be_smaller_or_equal_than_b",
            "arguments": {"column_A": "salesorder", "column_B": "amount"},
            "read_type": "streaming",
            "input_type": "dataframe_reader",
            "custom_expectation_result": "success",
        },
        {
            "expectation_name": "expect_multicolumn_column_a_must_equal_b_or_c",
            "arguments": {
                "column_list": ["item", "itemcode", "amount"],
            },
            "read_type": "batch",
            "input_type": "dataframe_reader",
            "custom_expectation_result": "success",
        },
        {
            "expectation_name": "expect_multicolumn_column_a_must_equal_b_or_c",
            "arguments": {
                "column_list": ["item", "itemcode", "amount"],
            },
            "read_type": "streaming",
            "input_type": "dataframe_reader",
            "custom_expectation_result": "success",
        },
        {
            "expectation_name": "expect_queried_column_agg_value_to_be",
            "arguments": {
                "template_dict": {
                    "column": "amount",
                    "group_column_list": "year, month, day",
                    "agg_type": "max",
                    "condition": "lesser",
                    "max_value": 10000,
                },
            },
            "read_type": "batch",
            "input_type": "dataframe_reader",
            "custom_expectation_result": "success",
        },
        {
            "expectation_name": "expect_queried_column_agg_value_to_be",
            "arguments": {
                "template_dict": {
                    "column": "amount",
                    "group_column_list": "year,month,day",
                    "agg_type": "count",
                    "condition": "greater",
                    "min_value": 0,
                },
            },
            "read_type": "streaming",
            "input_type": "dataframe_reader",
            "custom_expectation_result": "success",
        },
        {
            "expectation_name": "expect_column_values_to_be_date_not_older_than",
            "arguments": {
                "column": "date",
                "timeframe": {"years": 100},
            },
            "read_type": "streaming",
            "input_type": "dataframe_reader",
            "custom_expectation_result": "success",
        },
        {
            "expectation_name": "expect_column_values_to_be_date_not_older_than",
            "arguments": {
                "column": "date",
                "timeframe": {"years": 100},
            },
            "read_type": "batch",
            "input_type": "dataframe_reader",
            "custom_expectation_result": "success",
        },
    ],
)
def test_custom_expectation(scenario: dict, caplog: Any) -> None:
    """Test the implementation of the custom expectations.

    Args:
        scenario: scenario to test.
        caplog: captured log.
    """
    _clean_folders(scenario["expectation_name"])

    input_spec = {
        "spec_id": "sales_source",
        "read_type": scenario["read_type"],
        "data_format": "dataframe",
        "df_name": _generate_dataframe(
            scenario["read_type"], scenario["expectation_name"]
        ),
    }

    acon = _generate_acon(input_spec, scenario, "validator")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario['expectation_name']}/data/control/*",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario['expectation_name']}/data/",
    )

    execute_dq_validation(acon=acon)

    dq_result_df, dq_control_df = _get_result_and_control_dfs(
        "test_db.sales_order",
        f'dq_control_{scenario["custom_expectation_result"]}',
        True,
        scenario["expectation_name"],
    )

    assert not DataframeHelpers.has_diff(
        dq_result_df.select("spec_id", "input_id", "success"),
        dq_control_df.fillna("").select("spec_id", "input_id", "success"),
    )

    # test if the run_results column is json object
    # test if the json generated has the correct keys
    for key in dq_result_df.rdd.collect():
        assert list(loads(key.run_results).keys()) == [
            "actions_results",
            "validation_result",
        ]


def _clean_folders(expectation_name: str) -> None:
    """Clean test folders and tables."""
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_IN}/{expectation_name}/data")
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_OUT}/{expectation_name}/data")
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_OUT}/{expectation_name}/checkpoint")
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_OUT}/{expectation_name}/dq")
    ExecEnv.SESSION.sql("DROP TABLE IF EXISTS test_db.dq_sales")
    ExecEnv.SESSION.sql("DROP TABLE IF EXISTS test_db.sales_order")


def _generate_acon(
    input_spec: dict,
    scenario: dict,
    dq_type: str,
) -> dict:
    """Generate acon according to test scenario.

    Args:
        input_spec: input specification.
        scenario: the scenario being tested.
        dq_type: the type of data quality process.

    Returns: a dict corresponding to the generated acon.
    """
    dq_spec_add_options = {
        "result_sink_db_table": "test_db.sales_order",
        "result_sink_format": "json",
        "result_sink_explode": False,
        "dq_functions": [
            {
                "function": scenario["expectation_name"],
                "args": scenario["arguments"],
            }
        ],
    }

    return {
        "input_spec": input_spec,
        "dq_spec": {
            "spec_id": "dq_sales",
            "input_id": "sales_source",
            "dq_type": dq_type,
            "store_backend": "file_system",
            "local_fs_root_dir": f"{TEST_LAKEHOUSE_OUT}/{scenario['expectation_name']}/dq",  # noqa: E501
            **dq_spec_add_options,
        },
        "restore_prev_version": scenario.get("restore_prev_version", False),
    }


def _generate_dataframe(load_type: str, expectation_name: str) -> DataFrame:
    """Generate test dataframe.

    Args:
        load_type: batch or streaming.
        expectation_name: name of the expectation to test

    Returns: the generated dataframe.
    """
    if load_type == "batch":
        input_df = (
            ExecEnv.SESSION.read.format("csv")
            .option("header", True)
            .option("delimiter", "|")
            .schema(
                SchemaUtils.from_file(
                    f"file://{TEST_RESOURCES}/{expectation_name}/dq_sales_schema.json"
                )
            )
            .load(f"{TEST_RESOURCES}/{expectation_name}/data/source/part-01.csv")
        )
    else:
        input_df = (
            ExecEnv.SESSION.readStream.format("csv")
            .option("header", True)
            .option("delimiter", "|")
            .schema(
                SchemaUtils.from_file(
                    f"file://{TEST_RESOURCES}/{expectation_name}/dq_sales_schema.json"
                )
            )
            .load(f"{TEST_RESOURCES}/{expectation_name}/data/source/*")
        )

    return input_df


def _get_result_and_control_dfs(
    table: str, file_name: str, infer_schema: bool, expectation_name: str
) -> Tuple[DataFrame, DataFrame]:
    """Helper to get the result and control dataframes.

    Args:
        table: the table to read from.
        file_name: the file name to read from.
        infer_schema: whether to infer the schema or not.
        expectation_name: expectation name.

    Returns: the result and control dataframes.
    """
    dq_result_df = DataframeHelpers.read_from_table(table)

    dq_control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{expectation_name}/data/{file_name}.csv",
        file_format="csv",
        options={"header": True, "delimiter": "|", "inferSchema": infer_schema},
    )

    return dq_result_df, dq_control_df
