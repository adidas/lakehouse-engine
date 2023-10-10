"""Test extractions from SAP BW."""
import re
from datetime import datetime, timezone

import pytest
from _pytest.logging import LogCaptureFixture
from pyspark.sql import DataFrame
from pyspark.sql.utils import ParseException

from lakehouse_engine.core.definitions import OutputFormat
from lakehouse_engine.engine import load_data
from lakehouse_engine.utils.extraction.sap_bw_extraction_utils import (
    SAPBWExtraction,
    SAPBWExtractionUtils,
)
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

TEST_PATH = "extract_from_sap_bw"
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
    init_timestamp_from_actrequest - get the init timestamp from act_request
        table instead of assuming a given timestamp.
    fail_calc_upper_bound - empty partition of type date to force failure on
        the upper bound calculation.
    pushed_down_filter_with_slash - no strategy to split the
        extraction. Moreover, test applying pushed down filter to a column with
        slashes.
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
        "extra_cols_act_request": "act_req.request as activation_request",
        "act_req_join_condition": None,
    },
    {
        "scenario_name": "int_part_col_provide_upper_bound_&_min_timestamp",
        "calculate_upper_bound": False,
        "calculate_upper_bound_schema": "upper_bound int",
        "part_col": "item",
        "lower_bound": 1,
        "upper_bound": 3,
        "min_timestamp": "20211004151010",
        "generate_predicates": False,
        "predicates_list": None,
        "extra_cols_act_request": None,
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
        "extra_cols_act_request": "act_req.request as actrequest_request, status",
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
        "extra_cols_act_request": None,
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
        "extra_cols_act_request": None,
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
        "extra_cols_act_request": None,
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
        "extra_cols_act_request": None,
        "act_req_join_condition": None,
    },
    {
        "scenario_name": "init_timestamp_from_actrequest",
        "calculate_upper_bound": True,
        "calculate_upper_bound_schema": "upper_bound timestamp",
        "part_col": "time",
        "lower_bound": "2000-01-01 01:01:01.000",
        "upper_bound": None,
        "min_timestamp": None,
        "generate_predicates": False,
        "predicates_list": None,
        "extra_cols_act_request": None,
        "get_timestamp_from_act_request": True,
        "act_req_join_condition": None,
    },
    {
        "scenario_name": "fail_calc_upper_bound",
        "calculate_upper_bound": True,
        "calculate_upper_bound_schema": "upper_bound date",
        "part_col": "order_date",
        "lower_bound": "2000-01-01",
        "upper_bound": None,
        "min_timestamp": None,
        "generate_predicates": False,
        "predicates_list": None,
        "extra_cols_act_request": None,
        "act_req_join_condition": None,
    },
    {
        "scenario_name": "pushed_down_filter_with_slash",
        "calculate_upper_bound": False,
        "calculate_upper_bound_schema": None,
        "part_col": None,
        "lower_bound": None,
        "upper_bound": None,
        "min_timestamp": None,
        "generate_predicates": False,
        "predicates_list": None,
        "extra_cols_act_request": "act_req.request as activation_request",
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
        "extra_cols_act_request": None,
        "act_req_join_condition": "changelog_tbl.request = act_req.actrequest "
        "AND changelog_tbl.request = act_req.request",
    },
]


@pytest.mark.parametrize("scenario", TEST_SCENARIOS)
def test_extract_dso(scenario: dict, caplog: LogCaptureFixture) -> None:
    """Test the extraction from SAP BW DSO.

    Args:
        scenario: scenario to test.
        caplog: fixture to capture console logs.
    """
    extra_params = {
        "request_col_name": "actrequest",
        "changelog_table": f"{DB_TABLE}_cl",
        "test_name": "extract_dso",
        "include_changelog_tech_cols": True,
    }

    LOGGER.info(f"Starting Scenario {scenario['scenario_name']}")
    _prepare_files(scenario["scenario_name"], extra_params)
    _load_test_table("rsodsactreq", scenario["scenario_name"], extra_params)

    _execute_and_validate("extract_dso", scenario, extra_params, caplog)


@pytest.mark.parametrize("scenario", TEST_SCENARIOS)
def test_extract_write_optimised_dso(scenario: dict, caplog: LogCaptureFixture) -> None:
    """Test the extraction from SAP BW Write Optimised DSO.

    Args:
        scenario: scenario to test.
        caplog: fixture to capture console logs.
    """
    extra_params = {
        "request_col_name": "request",
        "changelog_table": DB_TABLE,
        "test_name": "extract_write_optimised_dso",
        "include_changelog_tech_cols": False,
    }

    LOGGER.info(f"Starting Scenario {scenario['scenario_name']}")
    _prepare_files(scenario["scenario_name"], extra_params)
    _load_test_table("rsodsactreq", scenario["scenario_name"], extra_params)

    _execute_and_validate("extract_wodso", scenario, extra_params, caplog)


def _execute_and_validate(
    test_name: str, scenario: dict, extra_params: dict, caplog: LogCaptureFixture
) -> None:
    """Helper function to reuse for trigger loading data and validation of results.

    Args:
        test_name: test being executed (for dso or wodso).
        scenario: scenario being tested.
        extra_params: extra params for the scenario being tested.
        caplog: fixture to capture console logs.
    """
    if scenario["scenario_name"] == "fail_calc_upper_bound":
        with pytest.raises(AttributeError, match="Not able to calculate upper bound"):
            _execute_load(
                scenario=scenario, extraction_type="init", extra_params=extra_params
            )
    elif test_name == "extract_dso" and "from_actrequest" in scenario["scenario_name"]:
        with pytest.raises(
            AttributeError, match="Not able to get the extraction query"
        ):
            _execute_load(
                scenario=scenario, extraction_type="init", extra_params=extra_params
            )
    elif scenario["scenario_name"] == "pushed_down_filter_with_slash":
        with pytest.raises(ParseException, match=".*Syntax error at or near '/'*"):
            # should fail on spark version 3.3.0
            # pushed filters for columns with '/' aren't working.
            _execute_load(
                scenario=scenario, extraction_type="init", extra_params=extra_params
            )
    else:
        _execute_load(
            scenario=scenario, extraction_type="init", extra_params=extra_params
        )

        changelog_table = extra_params["changelog_table"]
        assert f"The changelog table derived is: '{changelog_table}'" in caplog.text

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
        DB_TABLE if extraction_type == "init" else extra_params["changelog_table"],
        scenario["scenario_name"],
        extra_params,
        iteration,
    )

    # if it is an init, we need to provide an extraction_timestamp, otherwise the
    # current time would be used and data would be filtered accordingly.
    acon = _get_test_acon(
        extraction_timestamp="20211004151010"
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
                "data_format": "sap_bw",
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
                    "changelog_table": extra_params["changelog_table"]
                    if "changelog_table" in extra_params.keys()
                    else None,
                    "customSchema": "actrequest_timestamp DECIMAL(15,0), "
                    "datapakid STRING, request STRING, "
                    "partno INTEGER, record INTEGER, "
                    "extraction_start_timestamp DECIMAL(15,0)",
                    "act_request_table": "rsodsactreq",
                    "extra_cols_act_request": scenario["extra_cols_act_request"],
                    "latest_timestamp_data_location": f"file:///{TEST_LAKEHOUSE_OUT}/"
                    f"{scenario['scenario_name']}/{extra_params['test_name']}/data",
                    "extraction_type": extraction_type,
                    "numPartitions": 2,
                    "partitionColumn": scenario["part_col"],
                    "lowerBound": scenario["lower_bound"],
                    "upperBound": scenario["upper_bound"],
                    "default_upper_bound": "Null",
                    "extraction_timestamp": extraction_timestamp,
                    "min_timestamp": scenario["min_timestamp"],
                    "request_col_name": extra_params["request_col_name"],
                    "act_req_join_condition": scenario["act_req_join_condition"],
                    "include_changelog_tech_cols": extra_params[
                        "include_changelog_tech_cols"
                    ],
                    "predicates": scenario["predicates_list"],
                    "get_timestamp_from_act_request": scenario.get(
                        "get_timestamp_from_act_request", False
                    ),
                },
            }
        ],
        "transform_specs": [
            {
                "spec_id": "filtered_sales",
                "input_id": "sales_source",
                "transformers": [
                    {
                        "function": "expression_filter",
                        "args": {"exp": "`/bic/article` like 'article%'"},
                    }
                ],
            }
        ],
        "output_specs": [
            {
                "spec_id": "sales_bronze",
                "input_id": "filtered_sales"
                if scenario["scenario_name"] == "pushed_down_filter_with_slash"
                else "sales_source",
                "write_type": write_type,
                "data_format": "delta",
                "partitions": ["actrequest_timestamp"],
                "location": f"file:///{TEST_LAKEHOUSE_OUT}/{scenario['scenario_name']}/"
                f"{extra_params['test_name']}/data",
            }
        ],
        "exec_env": {
            "spark.databricks.delta.schema.autoMerge.enabled": True
            if scenario["extra_cols_act_request"]
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

    if (
        "optimised_dso" in extra_params["test_name"]
        and scenario == "init_timestamp_from_actrequest"
    ):
        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/"
            f"{extra_params['test_name']}/data/control/"
            f"dummy_table_actreq_timestamp.csv",
            f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/{extra_params['test_name']}/data/",
        )
    elif scenario == "no_part_col_join_condition":
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


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "name": "derive_changelog_table_name",
            "odsobject": "testtable",
            "table_name": "RSTSODS",
        },
        {
            "name": "derive_changelog_table_name",
            "odsobject": "test_table",
            "table_name": "RSTSODS",
        },
    ],
)
def test_changelog_table_name_derivation(scenario: dict) -> None:
    """Test the changelog table name derivation.

    Args:
        scenario: scenario to be tested.
    """
    LocalStorage.copy_file(
        f"""{TEST_RESOURCES}/{scenario["name"]}/data/source/*.csv""",
        f"""{TEST_LAKEHOUSE_IN}/{scenario["name"]}/source/""",
    )
    LocalStorage.copy_file(
        f"""{TEST_RESOURCES}/{scenario["name"]}/*.json""",
        f"""{TEST_LAKEHOUSE_IN}/{scenario["name"]}/""",
    )

    source_df = DataframeHelpers.read_from_file(
        location=f"""{TEST_LAKEHOUSE_IN}/{scenario["name"]}/"""
        f"""source/{scenario["table_name"]}.csv""",
        schema=SchemaUtils.from_file_to_dict(
            f"""file://{TEST_LAKEHOUSE_IN}/{scenario["name"]}/"""
            f"""{scenario["table_name"]}_schema.json"""
        ),
        options={"header": True, "delimiter": "|", "dateFormat": "yyyyMMdd"},
    )

    DataframeHelpers.write_into_jdbc_table(
        source_df,
        f"""jdbc:sqlite:{TEST_LAKEHOUSE_IN}/{scenario["name"]}/tests.db""",
        "RSTSODS",
    )

    extraction_utils = SAPBWExtractionUtils(
        SAPBWExtraction(  # nosec B106
            sap_bw_schema="",
            odsobject=scenario["odsobject"],
            dbtable="dummy_table",
            driver="org.sqlite.JDBC",
            user="dummy_user",
            password="dummy_pwd",
            url=f"""jdbc:sqlite:{TEST_LAKEHOUSE_IN}/{scenario["name"]}/tests.db""",
        )
    )

    assert re.match(
        f"""{scenario["odsobject"]}_OA""",
        extraction_utils.get_changelog_table(),
    )
