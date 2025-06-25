"""Test data quality process in different types of data loads."""

from json import loads
from typing import Any

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import array_sort, col, regexp_replace, transform
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from lakehouse_engine.core.definitions import (
    DQExecutionPoint,
    DQFunctionSpec,
    DQSpec,
    DQType,
)
from lakehouse_engine.dq_processors.dq_factory import DQFactory
from lakehouse_engine.dq_processors.exceptions import DQValidationsFailedException
from lakehouse_engine.engine import load_data
from lakehouse_engine.utils.dq_utils import PrismaUtils
from lakehouse_engine.utils.schema_utils import SchemaUtils
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.dq_rules_table_utils import _create_dq_functions_source_table
from tests.utils.local_storage import LocalStorage

TEST_PATH = "data_quality"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "name": "delta_with_duplicates",
            "read_type": "streaming",
            "results_exploded": True,
            "tag_source_data": False,
        },
        {
            "name": "delta_with_duplicates_tag",
            "read_type": "streaming",
            "results_exploded": True,
            "tag_source_data": True,
        },
        {
            "name": "delta_with_dupl_tag_gen_fail",
            "read_type": "streaming",
            "results_exploded": True,
            "tag_source_data": True,
        },
        {
            "name": "no_transformers",
            "read_type": "streaming",
            "results_exploded": False,
            "tag_source_data": False,
        },
        {
            "name": "full_overwrite",
            "read_type": "batch",
            "results_exploded": True,
            "tag_source_data": False,
        },
        {
            "name": "full_overwrite_tag",
            "read_type": "batch",
            "results_exploded": True,
            "tag_source_data": True,
        },
    ],
)
def test_load_with_dq_validator(scenario: dict) -> None:
    """Test the data quality validator process as part of the load_data algorithm.

    Description of the test scenarios:
        - delta_with_duplicates - test the DQ process for a streaming
        init and delta load with duplicates and merge strategy scenario.
        It's generated a DQ result_sink where some columns are exploded to make easier
        the analysis.
        - delta_with_duplicates_tag - similar to delta_with_duplicates but using DQ Row
        Tagging. The scenarios with tagging, test not only the loads and the result
        DQ sink, but also the resulting data to assert the "dq_validations" column
        that gets added into the source data used. This scenario covers different
        kinds of expectations (table, column aggregated, column, multi-column,
        column pair) with successes and failures.
        - delta_with_dupl_tag_gen_fail - similar to delta_with_duplicates_tag, but
        tests DQ success on init and then only general failures (not row level).
        - no_transformers - test the DQ process for a streaming init and delta
        without transformers or micro batch transformers. It's generated a DQ
        result_sink in a raw format.
        - full_overwrite - test the DQ process for a batch full overwrite scenario.
        It's generated a DQ result_sink where some columns are exploded to make easier
        the analysis, in which includes some extra columns set by
        the user to be included (using parameter result_sink_extra_columns).
        - full_overwrite_tag - similar to full_overwrite but using DQ Row
        Tagging. This scenario covers different kinds of expectations, all succeeded.

    Args:
        scenario: scenario to test.
            name - name of the scenario.
            read_type - type of read, namely batch or streaming.
            results_exploded - flag to generate a DQ result_sink in a raw format
                (False) or an exploded format easier for analysis (True).
            tag_source_data - whether the test scenario tests tagging the source
                data with the DQ results or not.
    """
    test_name = "load_with_dq_validator"
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{test_name}/{scenario['name']}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{test_name}/{scenario['name']}/data/",
    )
    load_data(
        f"file://{TEST_RESOURCES}/{test_name}/{scenario['name']}/"
        f"{scenario['read_type']}_init.json"
    )

    if "full_overwrite" in scenario["name"]:
        LocalStorage.clean_folder(
            f"{TEST_LAKEHOUSE_IN}/{test_name}/{scenario['name']}/data",
        )

    result_sink_df = DataframeHelpers.read_from_table(
        f"test_db.validator_{scenario['name']}"
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{test_name}/{scenario['name']}/"
        f"data/source/part-0[2,3,4].csv",
        f"{TEST_LAKEHOUSE_IN}/{test_name}/{scenario['name']}/data/",
    )
    load_data(
        f"file://{TEST_RESOURCES}/{test_name}/{scenario['name']}/"
        f"{scenario['read_type']}_new.json"
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{test_name}/{scenario['name']}/"
        f"data/control/data_validator.json",
        f"{TEST_LAKEHOUSE_CONTROL}/{test_name}/{scenario['name']}/validator/data/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{test_name}/{scenario['name']}/data/control/sales.json",
        f"{TEST_LAKEHOUSE_CONTROL}/{test_name}/{scenario['name']}/data/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{test_name}/{scenario['name']}/"
        f"data/control/*_schema.json",
        f"{TEST_LAKEHOUSE_CONTROL}/{test_name}/{scenario['name']}/validator/",
    )

    result_sink_df = DataframeHelpers.read_from_table(
        f"test_db.validator_{scenario['name']}"
    )

    control_sink_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{test_name}/{scenario['name']}/validator/data/",
        file_format="json",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_CONTROL}/{test_name}/"
            f"{scenario['name']}/validator/data_validator_schema.json"
        ),
    )

    # drop columns for which the values vary from run to run (ex: depending on date)
    cols_to_drop = [
        "checkpoint_config",
        "run_name",
        "run_time",
        "run_results",
        "validation_results",
        "validation_result_identifier",
        "exception_info",
        "batch_id",
        "run_time_year",
        "run_time_month",
        "run_time_day",
        "kwargs",
        "processed_keys",
    ]

    assert (
        result_sink_df.columns
        == control_sink_df.select(*result_sink_df.columns).columns
    )

    assert not DataframeHelpers.has_diff(
        result_sink_df.drop(*cols_to_drop),
        control_sink_df.drop(*cols_to_drop),
    )

    if scenario["tag_source_data"]:
        result_data_df = _prepare_validation_df(
            DataframeHelpers.read_from_file(
                f"{TEST_LAKEHOUSE_OUT}/{test_name}/{scenario['name']}/data",
                file_format="delta",
            )
        )

        control_data_df = _prepare_validation_df(
            DataframeHelpers.read_from_file(
                f"{TEST_LAKEHOUSE_CONTROL}/{test_name}/{scenario['name']}/data/",
                file_format="json",
                schema=SchemaUtils.from_file_to_dict(
                    f"file://{TEST_LAKEHOUSE_CONTROL}/{test_name}/"
                    f"{scenario['name']}/validator/sales_schema.json"
                ),
            )
        )

        assert not DataframeHelpers.has_diff(result_data_df, control_data_df)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "name": "delta_with_duplicates_tag",
            "read_type": "streaming",
            "results_exploded": True,
        },
        {
            "name": "delta_with_dupl_tag_gen_fail",
            "read_type": "streaming",
            "results_exploded": True,
        },
        {
            "name": "full_overwrite_tag",
            "read_type": "batch",
            "results_exploded": True,
        },
    ],
)
def test_load_with_dq_validator_table(scenario: dict) -> None:
    """Test the data quality validator process as part of the load_data algorithm.

    Description of the test scenarios:
        - delta_with_duplicates_tag - test the DQ process for a streaming
        init and delta load with duplicates and merge strategy scenario.
        It's generated a DQ result_sink where some columns are exploded to make easier
        the analysis using DQ Row Tagging. The scenarios with tagging, test
        not only the loads and the result DQ sink, but also the resulting data to
        assert the "dq_validations" column that gets added into the source data used.
        This scenario covers different kinds of expectations (table, column aggregated,
        column, multi-column, column pair) with successes and failures.
        - delta_with_dupl_tag_gen_fail - similar to delta_with_duplicates_tag, but
        tests DQ success on init and then only general failures (not row level).
        - full_overwrite_tag - test the DQ process for a batch full overwrite scenario.
        It's generated a DQ result_sink where some columns are exploded to make easier
        the analysis, in which includes some extra columns set by
        the user to be included (using parameter result_sink_extra_columns).
        This scenario covers different kinds of expectations, all succeeded.

    Args:
        scenario: scenario to test.
            name - name of the scenario.
            read_type - type of read, namely batch or streaming.
            results_exploded - flag to generate a DQ result_sink in a raw format
                (False) or an exploded format easier for analysis (True).
            tag_source_data - whether the test scenario tests tagging the source
                data with the DQ results or not.
    """
    test_name = "load_with_dq_table"

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{test_name}/{scenario['name']}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{test_name}/{scenario['name']}/data/",
    )
    _create_dq_functions_source_table(
        test_resources_path=TEST_RESOURCES,
        lakehouse_in_path=TEST_LAKEHOUSE_IN,
        lakehouse_out_path=TEST_LAKEHOUSE_OUT,
        test_name=f"{test_name}/{scenario['name']}",
        scenario=scenario["name"],
        table_name=f"test_db.dq_functions_source_{test_name}_{scenario['name']}_init",
    )
    load_data(
        f"file://{TEST_RESOURCES}/{test_name}/{scenario['name']}/"
        f"{scenario['read_type']}_init.json"
    )

    if "full_overwrite" in scenario["name"]:
        LocalStorage.clean_folder(
            f"{TEST_LAKEHOUSE_IN}/{test_name}/{scenario['name']}/data",
        )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{test_name}/{scenario['name']}/"
        f"data/source/part-0[2,3,4].csv",
        f"{TEST_LAKEHOUSE_IN}/{test_name}/{scenario['name']}/data/",
    )
    _create_dq_functions_source_table(
        test_resources_path=TEST_RESOURCES,
        lakehouse_in_path=TEST_LAKEHOUSE_IN,
        lakehouse_out_path=TEST_LAKEHOUSE_OUT,
        test_name=f"{test_name}/{scenario['name']}",
        scenario=scenario["name"],
        table_name=f"test_db.dq_functions_source_{test_name}_{scenario['name']}_new",
    )
    load_data(
        f"file://{TEST_RESOURCES}/{test_name}/{scenario['name']}/"
        f"{scenario['read_type']}_new.json"
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{test_name}/{scenario['name']}/"
        f"data/control/data_validator.json",
        f"{TEST_LAKEHOUSE_CONTROL}/{test_name}/{scenario['name']}/validator/data/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{test_name}/{scenario['name']}/data/control/sales.json",
        f"{TEST_LAKEHOUSE_CONTROL}/{test_name}/{scenario['name']}/data/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{test_name}/{scenario['name']}/"
        f"data/control/*_schema.json",
        f"{TEST_LAKEHOUSE_CONTROL}/{test_name}/{scenario['name']}/validator/",
    )

    result_sink_df = DataframeHelpers.read_from_file(
        location=f"{LAKEHOUSE_FEATURE_OUT}/{scenario['name']}/result_sink/",
        file_format="delta",
    )

    control_sink_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{test_name}/{scenario['name']}/validator/data/",
        file_format="json",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_CONTROL}/{test_name}/"
            f"{scenario['name']}/validator/data_validator_schema.json"
        ),
    )

    # drop columns for which the values vary from run to run (ex: depending on date)
    cols_to_drop = [
        "checkpoint_config",
        "run_name",
        "run_time",
        "run_results",
        "validation_results",
        "validation_result_identifier",
        "exception_info",
        "batch_id",
        "run_time_year",
        "run_time_month",
        "run_time_day",
        "kwargs",
        "meta",
    ]

    assert (
        result_sink_df.columns
        == control_sink_df.select(*result_sink_df.columns).columns
    )

    assert not DataframeHelpers.has_diff(
        result_sink_df.drop(*cols_to_drop),
        control_sink_df.drop(*cols_to_drop),
    )

    result_data_df = _prepare_validation_df(
        DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_OUT}/{test_name}/{scenario['name']}/data",
            file_format="delta",
        )
    )

    control_data_df = _prepare_validation_df(
        DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_CONTROL}/{test_name}/{scenario['name']}/data/",
            file_format="json",
            schema=SchemaUtils.from_file_to_dict(
                f"file://{TEST_LAKEHOUSE_CONTROL}/{test_name}/"
                f"{scenario['name']}/validator/sales_schema.json"
            ),
        )
    )

    assert not DataframeHelpers.has_diff(result_data_df, control_data_df)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "spec_id": "dq_success",
            "dq_type": "validator",
            "dq_functions": [
                DQFunctionSpec("expect_column_to_exist", {"column": "article"}),
                DQFunctionSpec(
                    "expect_table_row_count_to_be_between",
                    {"min_value": 0, "max_value": 50},
                ),
            ],
            "fail_on_error": True,
            "critical_functions": None,
            "max_percentage_failure": None,
        },
        {
            "spec_id": "dq_failure",
            "dq_type": "validator",
            "dq_functions": [
                DQFunctionSpec("expect_column_to_exist", {"column": "article"}),
                DQFunctionSpec(
                    "expect_table_row_count_to_be_between",
                    {"min_value": 0, "max_value": 1},
                ),
            ],
            "fail_on_error": True,
            "critical_functions": None,
            "max_percentage_failure": None,
        },
        {
            "spec_id": "dq_failure_error_disabled",
            "dq_type": "validator",
            "dq_functions": [
                DQFunctionSpec("expect_column_to_exist", {"column": "article"}),
                DQFunctionSpec(
                    "expect_table_row_count_to_be_between",
                    {"min_value": 0, "max_value": 1},
                ),
            ],
            "fail_on_error": False,
            "critical_functions": None,
            "max_percentage_failure": None,
        },
        {
            "spec_id": "dq_failure_critical_functions",
            "dq_type": "validator",
            "dq_functions": [
                DQFunctionSpec("expect_column_to_exist", {"column": "article"}),
            ],
            "fail_on_error": False,
            "critical_functions": [
                DQFunctionSpec(
                    "expect_table_row_count_to_be_between",
                    {
                        "min_value": 0,
                        "max_value": 1,
                    },
                ),
            ],
            "max_percentage_failure": None,
        },
        {
            "spec_id": "dq_failure_max_percentage",
            "dq_type": "validator",
            "dq_functions": [
                DQFunctionSpec("expect_column_to_exist", {"column": "article"}),
            ],
            "fail_on_error": False,
            "critical_functions": [
                DQFunctionSpec(
                    "expect_table_row_count_to_be_between",
                    {
                        "min_value": 0,
                        "max_value": 1,
                    },
                ),
            ],
            "max_percentage_failure": 0.2,
        },
        {
            "spec_id": "dq_success",
            "dq_type": "prisma",
            "dq_db_table": "test_db.dq_functions_source_dq_success",
            "dq_table_table_filter": "dummy_sales",
            "data_product_name": "dq_success",
            "unexpected_rows_pk": ["salesorder", "item", "date", "customer"],
        },
        {
            "spec_id": "dq_failure_error_disabled",
            "dq_type": "prisma",
            "fail_on_error": False,
            "dq_db_table": None,
            "dq_functions": [
                {
                    "function": "expect_table_row_count_to_be_between",
                    "args": {
                        "min_value": 0,
                        "max_value": 1,
                        "meta": {
                            "dq_rule_id": "rule_1",
                            "execution_point": "in_motion",
                            "schema": "test_db",
                            "table": "dummy_sales",
                            "column": "",
                            "dimension": "",
                            "filters": "",
                        },
                    },
                },
                {
                    "function": "expect_table_column_count_to_be_between",
                    "args": {
                        "min_value": 0,
                        "max_value": 50,
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
            "critical_functions": [],
            "data_product_name": "dq_failure_error_disabled",
            "unexpected_rows_pk": ["salesorder", "item", "date", "customer"],
            "max_percentage_failure": None,
        },
    ],
)
def test_validator_dq_spec(scenario: dict, caplog: Any) -> None:
    """Test the data quality process using DQSpec.

    Data Quality Functions tested using validator:
    - dq_success: it tests two expectations and both are succeeded.
    - dq_failure: it tests two expectations and one of them fails, raising an exception
    in the DQ process.
    - dq_failure_error_disabled: it tests one expectation and it fails, but no exception
    is raised, because the fail_on_error is set to false.
    - dq_failure_critical_functions: it tests two expectations where one fails, since
    the one that fails is part of the "critical_functions" an exception is raised.
    - dq_failure_max_percentage: it tests two expectations where one fails, since the
    "max_percentage_failure" variable is not respected, an exception is thrown.
    - dq_success: it tests two expectations defined using prisma and both succeed.
    - dq_failure_error_disabled: it tests one expectation defined in prisma, by
    manually defining the functions in the acon, and it fails, but no exception
    is raised, because the fail_on_error is set to false.


    Args:
        scenario: scenario to test.
        caplog: captured log.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/validator/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario['dq_type']}/{scenario['spec_id']}/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/validator/data/control/data_validator.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario['dq_type']}/{scenario['spec_id']}/data/",
    )
    input_data = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_IN}/{scenario['dq_type']}/{scenario['spec_id']}/data",
        file_format="csv",
        options={"header": True, "delimiter": "|", "inferSchema": True},
    )
    location = TEST_LAKEHOUSE_OUT.replace("file://", "")

    if scenario["dq_type"] == DQType.PRISMA.value:
        if scenario["dq_db_table"]:
            _create_dq_functions_source_table(
                test_resources_path=TEST_RESOURCES,
                lakehouse_in_path=TEST_LAKEHOUSE_IN,
                lakehouse_out_path=TEST_LAKEHOUSE_OUT,
                test_name="validator",
                scenario=scenario["spec_id"],
                table_name=scenario["dq_db_table"],
            )
            dq_functions = PrismaUtils.build_prisma_dq_spec(
                scenario,
                DQExecutionPoint.AT_REST.value,
            )["dq_functions"]
        else:
            dq_functions = scenario["dq_functions"]

        dq_spec = DQSpec(
            spec_id=scenario["spec_id"],
            input_id="sales_orders",
            dq_type=scenario["dq_type"],
            dq_db_table=scenario["dq_db_table"],
            store_backend="file_system",
            local_fs_root_dir=f"{location}/{scenario['dq_type']}/"
            f"{scenario['spec_id']}/",
            result_sink_format="json",
            result_sink_explode=False,
            processed_keys_location=f"{TEST_LAKEHOUSE_OUT}/{scenario['dq_type']}/"
            f"{scenario['spec_id']}/processed_keys",
            dq_functions=[
                DQFunctionSpec(
                    function=dq_function["function"], args=dq_function["args"]
                )
                for dq_function in dq_functions
            ],
            unexpected_rows_pk=scenario["unexpected_rows_pk"],
            result_sink_location=f"{TEST_LAKEHOUSE_OUT}/{scenario['dq_type']}/"
            f"{scenario['spec_id']}/data",
            fail_on_error=scenario["fail_on_error"],
            max_percentage_failure=scenario["max_percentage_failure"],
        )
    else:
        dq_spec = DQSpec(
            spec_id=scenario["spec_id"],
            input_id="sales_orders",
            dq_type=scenario["dq_type"],
            store_backend="file_system",
            local_fs_root_dir=f"{location}/{scenario['dq_type']}/"
            f"{scenario['spec_id']}/",
            result_sink_format="json",
            result_sink_explode=False,
            unexpected_rows_pk=[
                "salesorder",
                "item",
                "date",
                "customer",
            ],
            dq_functions=scenario["dq_functions"],
            result_sink_location=f"{TEST_LAKEHOUSE_OUT}/{scenario['dq_type']}/"
            f"{scenario['spec_id']}/data",
            fail_on_error=scenario["fail_on_error"],
            critical_functions=scenario["critical_functions"],
            max_percentage_failure=scenario["max_percentage_failure"],
        )

    if scenario["spec_id"] == "dq_failure":
        with pytest.raises(DQValidationsFailedException) as ex:
            DQFactory.run_dq_process(dq_spec, input_data)
        assert "Data Quality Validations Failed!" in str(ex.value)
    elif scenario["spec_id"] == "dq_failure_critical_functions":
        if scenario["dq_type"] != DQType.PRISMA.value:
            with pytest.raises(DQValidationsFailedException) as ex:
                DQFactory.run_dq_process(dq_spec, input_data)
            assert (
                "Data Quality Validations Failed, the following critical expectations "
                "failed: ['expect_table_row_count_to_be_between']." in str(ex.value)
            )
        else:
            DQFactory.run_dq_process(dq_spec, input_data)
    elif scenario["spec_id"] == "dq_failure_max_percentage":
        with pytest.raises(DQValidationsFailedException) as ex:
            DQFactory.run_dq_process(dq_spec, input_data)
        assert "Max error threshold is being surpassed!" in str(ex.value)
    else:
        DQFactory.run_dq_process(dq_spec, input_data)

        result_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_OUT}/{scenario['dq_type']}/"
            f"{scenario['spec_id']}/data",
            file_format="json",
        )

        if scenario["spec_id"] == "dq_failure_error_disabled":
            assert (
                "1 out of 2 Data Quality Expectation(s) have failed! "
                "Failed Expectations" in caplog.text
            )

        control_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_CONTROL}/{scenario['dq_type']}/"
            f"{scenario['spec_id']}/data",
            file_format="csv",
            options={"header": True, "delimiter": "|", "inferSchema": True},
        ).fillna("")

        assert not DataframeHelpers.has_diff(
            result_df.filter(result_df["spec_id"] == scenario["spec_id"]).select(
                "spec_id", "input_id", "success"
            ),
            control_df.filter(control_df["spec_id"] == scenario["spec_id"]).select(
                "spec_id", "input_id", "success"
            ),
        )

        assert result_df.columns == control_df.select(*result_df.columns).columns

        _test_result_structure(result_df)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "result": "success",
            "tag_source_data": False,
            "num_chunks": 2,
            "num_rows": 10,
            "dq_functions": [
                {
                    "function": "expect_column_value_lengths_to_be_between",
                    "args": {
                        "column": "id",
                        "min_value": 0,
                        "max_value": 5,
                        "meta": {
                            "dq_rule_id": "rule_2",
                            "execution_point": "in_motion",
                            "schema": "test_db",
                            "table": "dummy_data",
                            "column": "",
                            "dimension": "",
                            "filters": "",
                        },
                    },
                },
                {
                    "function": "expect_column_value_lengths_to_be_between",
                    "args": {
                        "column": "static_column",
                        "min_value": 0,
                        "max_value": 5,
                        "meta": {
                            "dq_rule_id": "rule_3",
                            "execution_point": "in_motion",
                            "schema": "test_db",
                            "table": "dummy_data",
                            "column": "",
                            "dimension": "",
                            "filters": "",
                        },
                    },
                },
            ],
        },
        {
            "result": "failure",
            "tag_source_data": False,
            "num_chunks": 20,
            "num_rows": 15,
            "dq_functions": [
                {
                    "function": "expect_column_value_lengths_to_be_between",
                    "args": {
                        "column": "id",
                        "min_value": 0,
                        "max_value": 1,
                        "meta": {
                            "dq_rule_id": "rule_2",
                            "execution_point": "in_motion",
                            "schema": "test_db",
                            "table": "dummy_data",
                            "column": "",
                            "dimension": "",
                            "filters": "",
                        },
                    },
                },
                {
                    "function": "expect_column_value_lengths_to_be_between",
                    "args": {
                        "column": "static_column",
                        "min_value": 0,
                        "max_value": 1,
                        "meta": {
                            "dq_rule_id": "rule_3",
                            "execution_point": "in_motion",
                            "schema": "test_db",
                            "table": "dummy_data",
                            "column": "",
                            "dimension": "",
                            "filters": "",
                        },
                    },
                },
            ],
        },
        {
            "result": "success",
            "tag_source_data": True,
            "num_chunks": 6,
            "num_rows": 15,
            "dq_functions": [
                {
                    "function": "expect_column_value_lengths_to_be_between",
                    "args": {
                        "column": "id",
                        "min_value": 0,
                        "max_value": 1,
                        "meta": {
                            "dq_rule_id": "rule_2",
                            "execution_point": "in_motion",
                            "schema": "test_db",
                            "table": "dummy_data",
                            "column": "",
                            "dimension": "",
                            "filters": "",
                        },
                    },
                },
                {
                    "function": "expect_column_value_lengths_to_be_between",
                    "args": {
                        "column": "static_column",
                        "min_value": 0,
                        "max_value": 20,
                        "meta": {
                            "dq_rule_id": "rule_3",
                            "execution_point": "in_motion",
                            "schema": "test_db",
                            "table": "dummy_data",
                            "column": "",
                            "dimension": "",
                            "filters": "",
                        },
                    },
                },
            ],
        },
    ],
)
def test_chunked_result_sink(scenario: dict, caplog: Any) -> None:
    """Test the chunked result sink for data quality validation.

    Scenario 0: test two expectations and both are successful.
    Scenario 1: test two expectations, both with errors
    Scenario 2: test two expectations, one with error and one without and
        the tagging functionality when multiple chunks exist.

    Args:
        scenario: scenario to test.
        caplog: captured log.
    """
    LocalStorage.clean_folder(f"{LAKEHOUSE_FEATURE_OUT}/test_dp/")
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("static_column", StringType(), False),
        ]
    )

    data = []
    for x in range(0, scenario["num_rows"]):
        data.append((x, True))

    df = DataframeHelpers.create_dataframe(data=data, schema=schema)

    acon = {
        "input_specs": [
            {
                "spec_id": "test_in",
                "read_type": "batch",
                "data_format": "dataframe",
                "df_name": df,
            },
        ],
        "dq_specs": [
            {
                "spec_id": "test_dq",
                "input_id": "test_in",
                "dq_type": DQType.PRISMA.value,
                "store_backend": "file_system",
                "local_fs_root_dir": f"{TEST_LAKEHOUSE_OUT}/chunked_result_sink/",
                "result_sink_format": "json",
                "data_product_name": "test_dp",
                "unexpected_rows_pk": ["id", "static_column"],
                "result_sink_chunk_size": 1,
                "dq_functions": scenario["dq_functions"],
                "tag_source_data": scenario["tag_source_data"],
            }
        ],
        "output_specs": [
            {
                "spec_id": "test_out",
                "input_id": "test_dq",
                "data_format": "dataframe",
                "write_type": "overwrite",
            }
        ],
    }

    result_df = load_data(acon=acon)["test_out"]

    result_sink = DataframeHelpers.read_from_file(
        location=f"{LAKEHOUSE_FEATURE_OUT}/test_dp/result_sink/", file_format="json"
    )
    assert result_sink.count() == scenario["num_chunks"]
    processed_keys = DataframeHelpers.read_from_file(
        location=f"{LAKEHOUSE_FEATURE_OUT}/test_dp/dq_processed_keys/",
        file_format="json",
    )
    assert processed_keys.count() == scenario["num_rows"]

    if scenario["result"] == "failure":
        assert (
            "2 out of 2 Data Quality Expectation(s) have failed! Failed Expectations"
            in caplog.text
        )

    if scenario["tag_source_data"]:
        final_df = result_df.groupBy("dq_validations").count()

        assert final_df.count() == 2
        for ele in final_df.collect():
            if ele.dq_validations.dq_failure_details:
                assert ele["count"] == 5
            else:
                assert ele["count"] == 10


def _test_result_structure(df: DataFrame) -> None:
    """Test if a dataframe has the expected keys in its structure.

    Tests the validity of a dataframe, by checking if some keys are part of the
    base structure of that dataframe.

    Args:
        df: dataframe to test.
    """
    for key in df.collect():
        for result in loads(key.validation_results):
            assert {
                "success",
                "expectation_config",
            }.issubset(result.keys())


def _prepare_validation_df(df: DataFrame) -> DataFrame:
    """Given a DataFrame apply necessary transformations to prepare it for validations.

    It performs necessary transformations like removing the date from the run_name and
    removing the batch_id from the dq_failure_details.

    Args:
        df: dataframe to transform.

    Returns: the transformed dataframe
    """
    return df.withColumn(
        "dq_validations",
        col("dq_validations")
        .withField(
            "run_name", regexp_replace(col("dq_validations.run_name"), "[0-9]", "")
        )
        .withField(
            "dq_failure_details",
            array_sort(
                transform(
                    "dq_validations.dq_failure_details",
                    lambda x: x.withField(
                        "kwargs",
                        regexp_replace(
                            x.kwargs,
                            '"batch_id":.*?,',
                            "",
                        ),
                    ),
                ),
            ),
        ),
    )
