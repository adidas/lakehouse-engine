"""Test extractions from SAP B4."""
from datetime import datetime, timezone

import pytest
from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import OutputFormat
from lakehouse_engine.engine import load_data
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.schema_utils import SchemaUtils
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_PATH = "extract_from_sap_b4"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"
LOGGER = LoggingHandler(__name__).get_logger()
DB_TABLE = "dummy_table"
"""Scenario - Description:
    no_part_col_no_lower_and_upper_bound_extra_cols - no strategy to split the
        extraction. Moreover, test adding single extra column from the activation
        requests table.
    int_part_col_provide_upper_bound_&_min_timestamp - partition column of type int,
        manually provided upper_bound to parallelize the extraction. Moreover, it
        provides the min_timestamp to use to get the data from the changelog in the
        delta extraction after the init, which mimics the possible situation, in
        which people might need to provide a specific timestamp for backfilling,
        instead of deriving it from an existing location.
    int_part_col_generate_predicates_multi_extra_cols - partition column of type int to
        automatically generate predicates and parallelize the extraction. Moreover, test
        adding multiple extra columns from the activation requests table.
    str_part_col_generate_predicates - partition column of type str to
        automatically generate predicates and parallelize the extraction.
    str_part_col_predicates_list - partition column of type str,
        manually provided predicates list to parallelize the extraction.
    date_part_col_calculate_upper_bound - partition column of type date to automatically
        calculate the upper_bound and parallelize the extraction.
    timestamp_part_col_calculate_upper_bound - partition column of type timestamp to
        automatically calculate the upper_bound and parallelize the extraction from.
    default_calc_upper_bound - empty partition of type int to force the default on
        the upper bound calculation.
    no_part_col_join_condition - no strategy to split the extraction. Test to
        validate custom join condition on activation table.
"""
TEST_SCENARIOS = [
    {
        "scenario_name": "no_part_col_no_lower_and_upper_bound_extra_cols",
        "calculate_upper_bound": False,
        "calculate_upper_bound_schema": None,
        "part_col": None,
        "lower_bound": None,
        "upper_bound": None,
        "min_timestamp": None,
        "generate_predicates": False,
        "predicates_list": None,
        "extra_cols_req_status_tbl": "req.records_read",
        "act_req_join_condition": None,
    },
    {
        "scenario_name": "int_part_col_provide_upper_bound_&_min_timestamp",
        "calculate_upper_bound": False,
        "calculate_upper_bound_schema": "upper_bound int",
        "part_col": "item",
        "lower_bound": 1,
        "upper_bound": 3,
        "min_timestamp": "20210713151010000000000",
        "generate_predicates": False,
        "predicates_list": None,
        "extra_cols_req_status_tbl": None,
        "act_req_join_condition": None,
    },
    {
        "scenario_name": "int_part_col_generate_predicates_multi_extra_cols",
        "calculate_upper_bound": False,
        "calculate_upper_bound_schema": None,
        "part_col": "item",
        "lower_bound": None,
        "upper_bound": None,
        "min_timestamp": None,
        "generate_predicates": True,
        "predicates_list": None,
        "extra_cols_req_status_tbl": "req.records_read, req.records_updated",
        "act_req_join_condition": None,
    },
    {
        "scenario_name": "str_part_col_generate_predicates",
        "calculate_upper_bound": False,
        "calculate_upper_bound_schema": None,
        "part_col": '"/bic/article"',
        "lower_bound": None,
        "upper_bound": None,
        "min_timestamp": None,
        "generate_predicates": True,
        "predicates_list": None,
        "extra_cols_req_status_tbl": None,
        "act_req_join_condition": None,
    },
    {
        "scenario_name": "str_part_col_predicates_list",
        "calculate_upper_bound": False,
        "calculate_upper_bound_schema": None,
        "part_col": None,
        "lower_bound": None,
        "upper_bound": None,
        "min_timestamp": None,
        "generate_predicates": False,
        "predicates_list": [
            "\"/bic/article\"='article1'",
            "\"/bic/article\"='article2'",
            "\"/bic/article\"='article3'",
            "\"/bic/article\"='article4'",
            "\"/bic/article\"='article5'",
            "\"/bic/article\"='article6'",
            "\"/bic/article\"='article7'",
            "\"/bic/article\"='article33'",
            "\"/bic/article\"='article60'",
            '"/bic/article" IS NULL',
        ],
        "extra_cols_req_status_tbl": None,
        "act_req_join_condition": None,
    },
    {
        "scenario_name": "date_part_col_calculate_upper_bound",
        "calculate_upper_bound": True,
        "calculate_upper_bound_schema": "upper_bound date",
        "part_col": "date",
        "lower_bound": "2000-01-01",
        "upper_bound": None,
        "min_timestamp": None,
        "generate_predicates": False,
        "predicates_list": None,
        "extra_cols_req_status_tbl": None,
        "act_req_join_condition": None,
    },
    {
        "scenario_name": "timestamp_part_col_calculate_upper_bound",
        "calculate_upper_bound": True,
        "calculate_upper_bound_schema": "upper_bound timestamp",
        "part_col": "time",
        "lower_bound": "2000-01-01 01:01:01.000",
        "upper_bound": None,
        "min_timestamp": None,
        "generate_predicates": False,
        "predicates_list": None,
        "extra_cols_req_status_tbl": None,
        "act_req_join_condition": None,
    },
    {
        "scenario_name": "no_part_col_join_condition",
        "calculate_upper_bound": False,
        "calculate_upper_bound_schema": None,
        "part_col": None,
        "lower_bound": None,
        "upper_bound": None,
        "min_timestamp": None,
        "generate_predicates": False,
        "predicates_list": None,
        "extra_cols_req_status_tbl": None,
        "act_req_join_condition": "tbl.reqtsn = req.request_tsn "
        "AND tbl.reqtsn = req.last_process_tsn",
    },
]


@pytest.mark.parametrize("scenario", TEST_SCENARIOS)
def test_extract_aq_dso(scenario: dict) -> None:
    """Test the extraction from SAP B4 AQ DSO.

    Args:
        scenario: scenario to test.
    """
    extra_params = {
        "changelog_table": DB_TABLE,
        "test_name": "extract_aq_dso",
        "adso_type": "AQ",
    }

    LOGGER.info(f"Starting Scenario {scenario['scenario_name']}")
    _prepare_files(scenario["scenario_name"], extra_params)
    _load_test_table("rspmrequest", scenario["scenario_name"], extra_params)

    _execute_and_validate(scenario, extra_params)


@pytest.mark.parametrize("scenario", TEST_SCENARIOS)
def test_extract_cl_dso(scenario: dict) -> None:
    """Test the extraction from SAP B4 CL DSO.

    Args:
        scenario: scenario to test.
    """
    extra_params = {
        "changelog_table": f"{DB_TABLE}_cl",
        "test_name": "extract_cl_dso",
        "adso_type": "CL",
    }

    LOGGER.info(f"Starting Scenario {scenario['scenario_name']}")
    _prepare_files(scenario["scenario_name"], extra_params)
    _load_test_table("rspmrequest", scenario["scenario_name"], extra_params)

    _execute_and_validate(scenario, extra_params)


def _execute_and_validate(scenario: dict, extra_params: dict) -> None:
    """Helper function to reuse for triggering the load data and validation of results.

    Args:
        scenario: scenario being tested.
        extra_params: extra params for the scenario being tested.
    """
    _execute_load(scenario=scenario, extraction_type="init", extra_params=extra_params)

    _execute_load(
        scenario=scenario,
        extraction_type="delta",
        iteration=1,
        extra_params=extra_params,
    )

    _execute_load(
        scenario=scenario,
        extraction_type="delta",
        iteration=2,
        extra_params=extra_params,
    )

    _validate(
        scenario["scenario_name"],
        extra_params,
        scenario["min_timestamp"] is not None,
    )


def _execute_load(
    scenario: dict,
    extra_params: dict,
    extraction_type: str,
    iteration: int = None,
) -> None:
    """Helper function to reuse for loading the data for the scenario tests.

    Args:
        scenario: scenario being tested.
        extra_params: extra params for the scenario being tested.
        extraction_type: type of extraction (delta or init).
        iteration: number of the iteration, in case it is to test a delta.
    """
    write_type = "overwrite" if extraction_type == "init" else "append"

    _load_test_table(
        extra_params["changelog_table"] if extraction_type != "init" else DB_TABLE,
        scenario["scenario_name"],
        extra_params,
        iteration,
    )

    # if it is an init, we need to provide an extraction_timestamp, otherwise the
    # current time would be used and data would be filtered accordingly.
    acon = _get_test_acon(
        extraction_timestamp="20210713151010"
        if extraction_type == "init"
        else datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"),
        extraction_type=extraction_type,
        write_type=write_type,
        scenario=scenario,
        extra_params=extra_params,
    )

    load_data(acon=acon)


def _get_test_acon(
    extraction_type: str,
    write_type: str,
    scenario: dict,
    extra_params: dict,
    extraction_timestamp: str = None,
) -> dict:
    """Creates a test ACON with the desired logic for the algorithm.

    Args:
        extraction_type: type of extraction (delta or init).
        write_type: the spark write type to be used.
        scenario: the scenario being tested.
        extra_params: extra params for the scenario being tested.
        extraction_timestamp: timestamp of the extraction. For local tests
            we specify it in the init, otherwise would be calculated and
            tests would fail.

    Returns:
        dict: the ACON for the algorithm configuration.
    """
    return {
        "input_specs": [
            {
                "spec_id": "sales_source",
                "read_type": "batch",
                "data_format": "sap_b4",
                "calculate_upper_bound": scenario["calculate_upper_bound"],
                "calc_upper_bound_schema": scenario["calculate_upper_bound_schema"],
                "generate_predicates": scenario["generate_predicates"],
                "options": {
                    "driver": "org.sqlite.JDBC",
                    "user": "dummy_user",
                    "password": "dummy_pwd",
                    "url": f"jdbc:sqlite:{TEST_LAKEHOUSE_IN}/"
                    f"{scenario['scenario_name']}/{extra_params['test_name']}/tests.db",
                    "dbtable": DB_TABLE,
                    "data_target": "dummy_table",
                    "act_req_join_condition": scenario["act_req_join_condition"],
                    "changelog_table": extra_params["changelog_table"],
                    "customSchema": "reqtsn DECIMAL(23,0), datapakid STRING, "
                    "record INTEGER, extraction_start_timestamp DECIMAL(15,0)",
                    "request_status_tbl": "rspmrequest",
                    "extra_cols_req_status_tbl": scenario["extra_cols_req_status_tbl"],
                    "latest_timestamp_data_location": f"file:///{TEST_LAKEHOUSE_OUT}/"
                    f"{scenario['scenario_name']}/{extra_params['test_name']}/data",
                    "extraction_type": extraction_type,
                    "numPartitions": 2,
                    "partitionColumn": scenario["part_col"],
                    "lowerBound": scenario["lower_bound"],
                    "upperBound": scenario["upper_bound"],
                    "default_upper_bound": scenario.get("default_upper_bound", "Null"),
                    "extraction_timestamp": extraction_timestamp,
                    "min_timestamp": scenario["min_timestamp"],
                    "predicates": scenario["predicates_list"],
                    "adso_type": extra_params["adso_type"],
                },
            }
        ],
        "output_specs": [
            {
                "spec_id": "sales_bronze",
                "input_id": "sales_source",
                "write_type": write_type,
                "data_format": "delta",
                "partitions": ["reqtsn"],
                "location": f"file:///{TEST_LAKEHOUSE_OUT}/{scenario['scenario_name']}/"
                f"{extra_params['test_name']}/data",
            }
        ],
        "exec_env": {
            "spark.databricks.delta.schema.autoMerge.enabled": True
            if scenario["extra_cols_req_status_tbl"]
            else False
        },
    }


def _prepare_files(scenario: str, extra_params: dict) -> None:
    """Copy all the files needed for the tests.

    Args:
         scenario: scenario being tested.
         extra_params: extra params for the scenario being tested.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{extra_params['test_name']}/data/source/*.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/{extra_params['test_name']}/source/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{extra_params['test_name']}/*.json",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/{extra_params['test_name']}/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{extra_params['test_name']}/data/control/*_schema.json",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/{extra_params['test_name']}/",
    )

    if scenario == "no_part_col_join_condition":
        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/"
            f"{extra_params['test_name']}/data/control/"
            f"dummy_table_join_condition.csv",
            f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/{extra_params['test_name']}/data/",
        )
    else:
        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/{extra_params['test_name']}/data/control/"
            f"dummy_table.csv",
            f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/{extra_params['test_name']}/data/",
        )


def _load_test_table(
    db_table: str, scenario: str, extra_params: dict, iteration: int = None
) -> DataFrame:
    """Load the JDBC tables for the tests and return a Dataframe with the content.

    Args:
        db_table: table being loaded.
        scenario: scenario being tested.
        extra_params: extra params for the scenario being tested.
        iteration: number of the iteration, in case it is to test a delta.

    Returns:
        A Dataframe with the content of the JDBC table loaded.
    """
    file_name = f"{db_table}_{iteration}" if iteration else db_table

    source_df = DataframeHelpers.read_from_file(
        location=f"{TEST_LAKEHOUSE_IN}/{scenario}/{extra_params['test_name']}/"
        f"source/{file_name}.csv",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_IN}/{scenario}/{extra_params['test_name']}/"
            f"{db_table}_schema.json"
        ),
        options={"header": True, "delimiter": "|", "dateFormat": "yyyyMMdd"},
    )

    DataframeHelpers.write_into_jdbc_table(
        source_df,
        f"jdbc:sqlite:{TEST_LAKEHOUSE_IN}/{scenario}/"
        f"{extra_params['test_name']}/tests.db",
        db_table,
    )

    return DataframeHelpers.read_from_jdbc(
        f"jdbc:sqlite:{TEST_LAKEHOUSE_IN}/{scenario}/"
        f"{extra_params['test_name']}/tests.db",
        db_table,
    )


def _validate(scenario: str, extra_params: dict, min_timestamp: bool) -> None:
    """Perform the validation part of the local tests.

    Args:
        scenario: the scenario being tested.
        extra_params: extra params for the scenario being tested.
        min_timestamp: whether the min_timestamp is provided or not.
    """
    control_df = DataframeHelpers.read_from_file(
        location=f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/{extra_params['test_name']}/"
        f"data",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_CONTROL}/{scenario}/"
            f"{extra_params['test_name']}/dummy_table_schema.json"
        ),
        options={"header": True, "delimiter": "|", "dateFormat": "yyyyMMdd"},
    )

    control_df_columns = control_df.columns
    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/{extra_params['test_name']}/data",
        file_format=OutputFormat.DELTAFILES.value,
    ).select(control_df_columns)

    if min_timestamp:
        # when we fill the min_timestamp, it means it can either skip or
        # re-extract things, depending on the timestamp provided. In our scenario
        # is expected to re-extract, causing duplicates, thus if we remove the
        # duplicates we expect to match the non-duplicated control dataframe
        result_df = result_df.drop_duplicates()

    assert not DataframeHelpers.has_diff(control_df, result_df)
