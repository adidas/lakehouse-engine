"""Tests for the log lakehouse engine function."""
import os
import re
from datetime import datetime

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.engine import execute_dq_validation, load_data, manage_table
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_LOGS,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_NAME = "engine_usage_stats"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_NAME}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_NAME}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_NAME}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_NAME}"
TIMESTAMP = datetime.now()
YEAR = TIMESTAMP.year
MONTH = TIMESTAMP.month


def custom_transformation(df: DataFrame) -> DataFrame:
    """A sample custom transformation to use in the ACON.

    Args:
        df: DataFrame passed as input.

    Returns:
        DataFrame: the transformed DataFrame.
    """
    return df.withColumn("new_column", lit("literal"))


def _get_test_acon(scenario_name: str) -> dict:
    """Creates a test ACON with the desired logic for the test.

    Args:
        scenario_name: name of the test scenario running.

    Returns:
        dict: the ACON for the algorithm configuration.
    """
    df = ExecEnv.SESSION.read.options(
        header="True", inferSchema="True", delimiter="|"
    ).csv(f"{TEST_LAKEHOUSE_IN}/{scenario_name}/data/")
    input_spec: dict = {
        "spec_id": "sales_source",
        "read_type": "batch",
    }
    transformers = [
        {
            "function": "rename",
            "args": {"cols": {"salesorder": "salesorder1"}},
        }
    ]
    if "simple_acon" not in scenario_name:
        transformers.append(
            {
                "function": "custom_transformation",
                "args": {"custom_transformer": custom_transformation},
            }
        )
        input_spec = {**input_spec, "data_format": "dataframe", "df_name": df}
    else:
        input_spec = {
            **input_spec,
            "data_format": "csv",
            "options": {
                "mode": "FAILFAST",
                "header": True,
                "delimiter": "|",
                "password": "dummy_password",
            },
            "location": f"{TEST_LAKEHOUSE_IN}/{scenario_name}/data/",
        }

    return {
        "input_specs": [input_spec],
        "transform_specs": [
            {
                "spec_id": "renamed_kpi",
                "input_id": "sales_source",
                "transformers": transformers,
            }
        ],
        "output_specs": [
            {
                "spec_id": "sales_bronze",
                "input_id": "renamed_kpi",
                "write_type": "overwrite",
                "data_format": "delta",
                "location": f"{TEST_LAKEHOUSE_OUT}/{scenario_name}/data/",
            }
        ],
        "exec_env": {"dp_name": scenario_name},
    }


@pytest.mark.parametrize("scenario", ["load_simple_acon", "load_custom_transf_and_df"])
def test_load_data(scenario: str) -> None:
    """Test Data Loader with different scenarios.

    Scenarios:
        engine_usage_stats:

    Args:
        scenario: scenario to test.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/control.json",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data/",
    )

    load_data(
        acon=_get_test_acon(scenario),
        spark_confs={"dp_name": "dp_name"},
        collect_engine_usage=True,
    )

    _prepare_and_compare_dfs(scenario)


@pytest.mark.parametrize("scenario", ["table_manager"])
def test_table_manager(scenario: str) -> None:
    """Test Table Manager with different scenarios.

    Scenarios:
        table_manager: table_manager logging behaviour

    Args:
        scenario: scenario to test.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/control.json",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data/",
    )

    acon = {
        "function": "execute_sql",
        "sql": "select 1",
        "exec_env": {"dp_name": scenario},
    }

    manage_table(
        acon=acon, spark_confs={"dp_name": "dp_name"}, collect_engine_usage=True
    )

    _prepare_and_compare_dfs(scenario)


@pytest.mark.parametrize("scenario", ["dq_validator"])
def test_dq_validator(scenario: str) -> None:
    """Test DQ Validator with different scenarios.

    Scenarios:
        dq_validator: dq_validator logging behaviour

    Args:
        scenario: scenario to test.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/control.json",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data/",
    )

    acon = {
        "input_spec": {
            "spec_id": "sales_source",
            "read_type": "batch",
            "data_format": "csv",
            "options": {"mode": "FAILFAST", "header": True, "delimiter": "|"},
            "location": f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
        },
        "dq_spec": {
            "spec_id": "dq_sales",
            "input_id": "sales_source",
            "dq_type": "validator",
            "store_backend": "file_system",
            "local_fs_root_dir": f"{TEST_LAKEHOUSE_OUT}/dq",
            "result_sink_db_table": "test_db.dq_validator",
            "result_sink_format": "json",
            "result_sink_explode": False,
            "dq_functions": [
                {"function": "expect_column_to_exist", "args": {"column": "article"}},
                {
                    "function": "expect_table_row_count_to_be_between",
                    "args": {"min_value": 3, "max_value": 11},
                },
                {
                    "function": "expect_column_pair_a_to_be_smaller_or_equal_than_b",
                    "args": {"column_A": "salesorder", "column_B": "amount"},
                },
            ],
        },
        "exec_env": {"dp_name": scenario},
    }

    execute_dq_validation(
        acon=acon, spark_confs={"dp_name": "dp_name"}, collect_engine_usage=True
    )

    _prepare_and_compare_dfs(scenario)


def _prepare_and_compare_dfs(scenario: str) -> None:
    """Prepare DF and compare test and control dataframes.

    Args:
        scenario: Scenario to load dataframes to compare.
    """
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data",
        "json",
        options={"inferSchema": True},
    )

    log_folder_path = f"{LAKEHOUSE_FEATURE_LOGS}/{scenario}/{YEAR}/{MONTH}/"
    log_file_path = os.listdir(log_folder_path)[-1]

    eng_usage_df = DataframeHelpers.read_from_file(
        f"{log_folder_path}{log_file_path}", "json"
    )

    assert eng_usage_df.columns == control_df.columns
    assert (
        eng_usage_df.select("start_timestamp").first()[0]
        >= control_df.select("start_timestamp").first()[0]
    )

    assert _prepare_df_comparison(eng_usage_df) == _prepare_df_comparison(control_df)


def _prepare_df_comparison(df: DataFrame) -> str:
    """Prepared DF to be comparable by dropping columns and converting it to string.

    Args:
        df: DataFrame to be prepared.

    Returns: a string representation of the Dataframe, ready to be compared.
    """
    cols_to_ignore = ["start_timestamp", "engine_version"]
    str_df = str(df.drop(*cols_to_ignore).first()[0])
    str_df = re.sub("'<function ", "", str_df)
    return re.sub(" at.*'", "", str_df)
