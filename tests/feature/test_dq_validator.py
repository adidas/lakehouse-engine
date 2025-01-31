"""Test data quality validator."""

from json import loads
from typing import Any, Dict, List, Tuple, Union

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.utils import StreamingQueryException

from lakehouse_engine.core.definitions import DQType
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.dq_processors.exceptions import (
    DQDuplicateRuleIdException,
    DQValidationsFailedException,
)
from lakehouse_engine.engine import execute_dq_validation, load_data
from lakehouse_engine.utils.logging_handler import LoggingHandler
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

_LOGGER = LoggingHandler(__name__).get_logger()

TEST_NAME = "dq_validator"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_NAME}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_NAME}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_NAME}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_NAME}"


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "spec_id": "spec_without_duplicate",
            "name": "table_batch_dq_rule",
            "dq_type": "prisma",
            "read_type": "batch",
            "input_type": "file_reader",
            "dq_table_table_filter": "dummy_sales",
            "dq_validator_result": "success",
            "restore_prev_version": False,
            "fail_on_error": False,
            "critical_functions": None,
            "dq_db_table": "test_db.dq_table_rule_id_success",
            "max_percentage_failure": None,
        },
        {
            "spec_id": "spec_with_duplicate",
            "name": "table_batch_dq_rule",
            "dq_type": "prisma",
            "read_type": "batch",
            "input_type": "file_reader",
            "dq_table_table_filter": "dummy_sales",
            "dq_validator_result": "failed",
            "restore_prev_version": False,
            "fail_on_error": False,
            "critical_functions": None,
            "dq_db_table": "test_db.dq_table_rule_id_failure",
            "max_percentage_failure": None,
        },
        {
            "spec_id": "streaming_spec_without_duplicate",
            "name": "table_streaming_dq_rule",
            "dq_type": "prisma",
            "read_type": "streaming",
            "input_type": "file_reader",
            "dq_table_table_filter": "dummy_sales",
            "dq_validator_result": "success",
            "restore_prev_version": False,
            "fail_on_error": False,
            "critical_functions": None,
            "dq_db_table": "test_db.dq_table_rule_id_success",
            "max_percentage_failure": None,
        },
        {
            "spec_id": "streaming_spec_with_duplicate",
            "name": "table_streaming_dq_rule",
            "dq_type": "prisma",
            "read_type": "streaming",
            "input_type": "file_reader",
            "dq_table_table_filter": "dummy_sales",
            "dq_validator_result": "failed",
            "restore_prev_version": False,
            "fail_on_error": False,
            "critical_functions": None,
            "dq_db_table": "test_db.dq_table_rule_id_failure",
            "max_percentage_failure": None,
        },
    ],
)
def test_dq_rule_id_uniqueness(scenario: dict, caplog: Any) -> None:
    """Test the function to detect duplicate dq_rule_id.

    Dq_rule_id scenarios:
    - scenario 1: using the file reader in batch to test if the dq_db_table
    has duplicated dq_rule_id. This scenario do not have duplicates.
    - scenario 2: Using the file reader in batch mode to check for duplicate
    dq_rule_id values in the dq_db_table. In this scenario, duplicates are found
    in rule_3 and rule_4.
    - scenario 3: using the file reader in streaming to test if the dq_db_table
    has duplicated dq_rule_id. This scenario do not have duplicates.
    - scenario 4: using the file reader in streaming mode to check for duplicate
    dq_rule_id values in the dq_db_table. In this scenario, duplicates are found
    in rule_3 and rule_5.

    Args:
        scenario: scenario to test.
        caplog: captured log.
    """
    _clean_folders()

    _create_table("dq_sales")

    _execute_load(scenario["read_type"])

    input_spec = {
        "spec_id": "sales_source",
        "data_format": "delta",
        "read_type": scenario["read_type"],
        "location": f"{TEST_LAKEHOUSE_OUT}/data/",
    }

    _create_dq_functions_source_table(
        test_resources_path=TEST_RESOURCES,
        lakehouse_in_path=TEST_LAKEHOUSE_IN,
        lakehouse_out_path=TEST_LAKEHOUSE_OUT,
        test_name=scenario["name"],
        scenario=scenario["read_type"],
        table_name=scenario["dq_db_table"],
    )

    acon = _generate_acon(
        input_spec, scenario, scenario.get("dq_type", DQType.VALIDATOR.value)
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/control/*",
        f"{TEST_LAKEHOUSE_CONTROL}/data/",
    )

    if (scenario["dq_validator_result"] == "failed") and ("batch" in scenario["name"]):
        with pytest.raises(DQDuplicateRuleIdException) as error:
            execute_dq_validation(acon=acon)
        assert "rule_3" and "rule_4" in error.value.args[0]
        _LOGGER.critical(error.value.args[0])
    elif (scenario["dq_validator_result"] == "failed") and (
        "streaming" in scenario["name"]
    ):
        with pytest.raises(DQDuplicateRuleIdException) as error:
            execute_dq_validation(acon=acon)
        assert "rule_3" and "rule_5" in error.value.args[0]
        _LOGGER.critical(error.value.args[0])
    else:
        execute_dq_validation(acon=acon)
        assert "A duplicate dq_rule_id was found!!!" not in caplog.text


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "name": "batch_dataframe_success",
            "read_type": "batch",
            "input_type": "dataframe_reader",
            "dq_validator_result": "success",
            "restore_prev_version": False,
            "fail_on_error": True,
            "critical_functions": None,
            "max_percentage_failure": None,
        },
        {
            "name": "streaming_dataframe_failure",
            "read_type": "streaming",
            "input_type": "dataframe_reader",
            "dq_validator_result": "failure",
            "restore_prev_version": False,
            "fail_on_error": True,
            "critical_functions": None,
            "max_percentage_failure": None,
        },
        {
            "name": "streaming_failure_disabled",
            "read_type": "streaming",
            "input_type": "table_reader",
            "dq_validator_result": "failure_disabled",
            "restore_prev_version": False,
            "fail_on_error": False,
            "critical_functions": None,
            "max_percentage_failure": None,
        },
        {
            "name": "batch_failure",
            "read_type": "batch",
            "input_type": "table_reader",
            "dq_validator_result": "failure",
            "restore_prev_version": True,
            "fail_on_error": True,
            "critical_functions": None,
            "max_percentage_failure": None,
        },
        {
            "name": "streaming_failure",
            "read_type": "streaming",
            "input_type": "file_reader",
            "dq_validator_result": "failure",
            "restore_prev_version": True,
            "fail_on_error": True,
            "critical_functions": None,
            "max_percentage_failure": None,
        },
        {
            "name": "streaming_failure_critical",
            "read_type": "streaming",
            "input_type": "file_reader",
            "dq_validator_result": "failure",
            "restore_prev_version": True,
            "fail_on_error": True,
            "critical_functions": [
                {
                    "function": "expect_table_row_count_to_be_between",
                    "args": {"min_value": 3, "max_value": 11},
                }
            ],
            "max_percentage_failure": None,
        },
        {
            "name": "streaming_failure_critical_notes",
            "read_type": "streaming",
            "input_type": "file_reader",
            "dq_validator_result": "failure",
            "restore_prev_version": True,
            "fail_on_error": True,
            "critical_functions": [
                {
                    "function": "expect_table_row_count_to_be_between",
                    "args": {
                        "min_value": 3,
                        "max_value": 11,
                        "meta": {"notes": "Test notes"},
                    },
                }
            ],
            "max_percentage_failure": None,
        },
        {
            "name": "streaming_failure_critical_markdown",
            "read_type": "streaming",
            "input_type": "file_reader",
            "dq_validator_result": "failure",
            "restore_prev_version": True,
            "fail_on_error": True,
            "critical_functions": [
                {
                    "function": "expect_table_row_count_to_be_between",
                    "args": {
                        "min_value": 3,
                        "max_value": 11,
                        "meta": {
                            "notes": {"format": "markdown", "content": "**Test Notes**"}
                        },
                    },
                }
            ],
            "max_percentage_failure": None,
        },
        {
            "name": "streaming_failure_percentage",
            "read_type": "streaming",
            "input_type": "file_reader",
            "dq_validator_result": "failure",
            "restore_prev_version": True,
            "fail_on_error": True,
            "critical_functions": None,
            "max_percentage_failure": 0.2,
        },
        {
            "name": "table_batch_success",
            "dq_type": "prisma",
            "read_type": "batch",
            "input_type": "file_reader",
            "dq_validator_result": "success_explode",
            "restore_prev_version": False,
            "fail_on_error": False,
            "critical_functions": None,
            "dq_db_table": "test_db.dq_functions_source_table_success",
            "max_percentage_failure": None,
        },
        {
            "name": "table_batch_failure_disabled",
            "dq_type": "prisma",
            "read_type": "batch",
            "input_type": "file_reader",
            "dq_validator_result": "success_explode_disabled",
            "restore_prev_version": False,
            "fail_on_error": False,
            "critical_functions": None,
            "dq_db_table": "test_db.dq_functions_source_table_failure",
            "max_percentage_failure": None,
        },
        {
            "name": "table_streaming_success",
            "dq_type": "prisma",
            "read_type": "streaming",
            "input_type": "file_reader",
            "dq_validator_result": "success_explode",
            "restore_prev_version": False,
            "fail_on_error": False,
            "critical_functions": None,
            "dq_db_table": "test_db.dq_functions_source_table_success",
            "max_percentage_failure": None,
        },
        {
            "name": "table_streaming_failure_disabled",
            "dq_type": "prisma",
            "read_type": "streaming",
            "input_type": "file_reader",
            "dq_validator_result": "success_explode_disabled",
            "restore_prev_version": False,
            "fail_on_error": False,
            "critical_functions": None,
            "dq_db_table": "test_db.dq_functions_source_table_failure",
            "max_percentage_failure": None,
        },
        {
            "name": "table_batch_dataframe_success",
            "dq_type": "prisma",
            "read_type": "batch",
            "input_type": "dataframe_reader",
            "dq_validator_result": "success_explode",
            "restore_prev_version": False,
            "fail_on_error": False,
            "critical_functions": None,
            "dq_db_table": "test_db.dq_functions_source_table_success",
            "max_percentage_failure": None,
        },
        {
            "name": "table_batch_dataframe_failure_disabled",
            "dq_type": "prisma",
            "read_type": "streaming",
            "input_type": "dataframe_reader",
            "dq_validator_result": "success_explode_disabled",
            "restore_prev_version": False,
            "fail_on_error": False,
            "critical_functions": None,
            "dq_db_table": "test_db.dq_functions_source_table_failure",
            "max_percentage_failure": None,
        },
    ],
)
def test_dq_validator(scenario: dict, caplog: Any) -> None:
    """Test the Data Quality Validator algorithm with DQ Type Validator.

    Data Quality Validator scenarios:
    - scenario 1: test DQ Validator having a generated dataframe as input
    that passes all the expectations defined.
    - scenario 2: test DQ Validator, reading a generated dataframe as
    stream that fails one of the expectations defined.
    - scenario 3: test DQ Validator, reading as streaming a delta table,
    failing one of the expectations but not failing the complete DQ process
    as fail_on_error is disabled.
    - scenario 4: test DQ Validator, reading a delta table (batch),
    that fails one of the expectations defined and a previous version of the
    delta table is restored.
    - scenario 5: test DQ Validator, reading as streaming a set of files in a
    specific location, that fail one of the expectations defined and a
    previous version of the delta table is restored.
    - scenario 6: test DQ Validator, reading as streaming a set of files in a
    specific location, that fails one of the expectations that is defined as
    critical.
    - scenario 7: test DQ Validator, reading as streaming a set of files in a
    specific location, that fails one of the expectations that is defined as
    critical and notes in default format.
    - scenario 8: test DQ Validator, reading as streaming a set of files in a
    specific location, that fails one of the expectations that is defined as
    critical and notes with markdown.
    - scenario 9: test DQ Validator, reading as streaming a set of files in a
    specific location, that fails the whole expectation suite because the
    maximum percentage threshold is surpassed.

    Args:
        scenario: scenario to test.
        caplog: captured log.
    """
    _clean_folders()

    if "dataframe" in scenario["input_type"]:
        input_spec = {
            "spec_id": "sales_source",
            "read_type": scenario["read_type"],
            "data_format": "dataframe",
            "df_name": _generate_dataframe(scenario["read_type"]),
        }
    else:
        _create_table("dq_sales")

        _execute_load(scenario["read_type"])

        if "table" in scenario["input_type"]:
            input_spec = {
                "spec_id": "sales_source",
                "read_type": scenario["read_type"],
                "db_table": "test_db.dq_sales",
            }
        else:
            input_spec = {
                "spec_id": "sales_source",
                "data_format": "delta",
                "read_type": scenario["read_type"],
                "location": f"{TEST_LAKEHOUSE_OUT}/data/",
            }

    if "dq_db_table" in scenario.keys():
        _create_dq_functions_source_table(
            test_resources_path=TEST_RESOURCES,
            lakehouse_in_path=TEST_LAKEHOUSE_IN,
            lakehouse_out_path=TEST_LAKEHOUSE_OUT,
            test_name=scenario["name"],
            scenario=scenario["read_type"],
            table_name=scenario["dq_db_table"],
        )

    acon = _generate_acon(
        input_spec, scenario, scenario.get("dq_type", DQType.VALIDATOR.value)
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/control/*",
        f"{TEST_LAKEHOUSE_CONTROL}/data/",
    )

    if scenario["dq_validator_result"] == "failure":
        with pytest.raises(
            (DQValidationsFailedException, StreamingQueryException),
            match=".*Data Quality Validations Failed!.*",
        ):
            execute_dq_validation(acon=acon)
    else:
        execute_dq_validation(acon=acon)

    if scenario["restore_prev_version"] is True:
        data_result_df, data_control_df = _get_result_and_control_dfs(
            "test_db.dq_sales", "data_restore_control", False
        )

        assert not DataframeHelpers.has_diff(data_result_df, data_control_df)
        assert "Data Quality Expectation(s) have failed!" in caplog.text

    if scenario["dq_validator_result"] == "failure_disabled":
        assert (
            "1 out of 3 Data Quality Expectation(s) have failed! "
            "Failed Expectations" in caplog.text
        )

    dq_result_df, dq_control_df = _get_result_and_control_dfs(
        result=f"{LAKEHOUSE_FEATURE_OUT}/{scenario['name']}/result_sink/",
        control=f'dq_control_{scenario["dq_validator_result"]}',
        infer_schema=True,
        result_is_table=False,
    )

    assert not DataframeHelpers.has_diff(
        dq_result_df.select("spec_id", "input_id", "success"),
        dq_control_df.fillna("").select("spec_id", "input_id", "success"),
    )

    # test if the run_results column is json object
    # test if the json generated has the correct keys
    for key in dq_result_df.collect():
        assert list(loads(key.run_results).keys()) == [
            "actions_results",
            "validation_result",
        ]


def _clean_folders() -> None:
    """Clean test folders and tables."""
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_IN}/data")
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_OUT}/data")
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_OUT}/checkpoint")
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_OUT}/dq")
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_OUT}/profiling")
    ExecEnv.SESSION.sql("DROP TABLE IF EXISTS test_db.dq_sales")
    ExecEnv.SESSION.sql("DROP TABLE IF EXISTS test_db.dq_validator")


def _create_table(table_name: str) -> None:
    """Create test table.

    Args:
        table_name: name of the test table.
    """
    ExecEnv.SESSION.sql(
        f"""
        CREATE TABLE IF NOT EXISTS test_db.{table_name} (
            salesorder string,
            item string,
            date string,
            customer string,
            article string,
            amount string
        )
        USING delta
        LOCATION '{TEST_LAKEHOUSE_OUT}/data'
        TBLPROPERTIES(
          'lakehouse.primary_key'='salesorder, `item`, date ,`customer`',
          'delta.enableChangeDataFeed'='false'
        )
        """
    )


def _execute_load(load_type: str) -> None:
    """Helper function to reuse for loading the data for the scenario tests.

    Args:
        load_type: batch or streaming.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/data/",
    )

    load_data(f"file://{TEST_RESOURCES}/{load_type}.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/part-02.csv",
        f"{TEST_LAKEHOUSE_IN}/data/",
    )

    load_data(f"file://{TEST_RESOURCES}/{load_type}.json")


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

    Returns:
        A dict corresponding to the generated acon.
    """
    if "dataframe" in scenario["input_type"]:
        unexpected_rows_pk: Dict[str, Union[str, List[str]]] = {
            "unexpected_rows_pk": ["salesorder", "item", "date", "customer"]
        }
    else:
        unexpected_rows_pk = {"tbl_to_derive_pk": "test_db.dq_sales"}

    if dq_type == DQType.VALIDATOR.value or dq_type == DQType.PRISMA.value:
        dq_spec_add_options = {
            "result_sink_location": f"{LAKEHOUSE_FEATURE_OUT}/"
            f"{scenario['name']}/result_sink/",
            "dq_db_table": scenario.get("dq_db_table"),
            "dq_table_table_filter": "dummy_sales",
            "result_sink_format": "json",
            "fail_on_error": scenario["fail_on_error"],
            "critical_functions": scenario["critical_functions"],
            "max_percentage_failure": scenario["max_percentage_failure"],
            "result_sink_explode": False,
            "data_product_name": scenario["name"],
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
        }
        dq_spec_add_options.update(unexpected_rows_pk)

    return {
        "input_spec": input_spec,
        "dq_spec": {
            "spec_id": "dq_sales",
            "input_id": "sales_source",
            "dq_type": dq_type,
            "store_backend": "file_system",
            "local_fs_root_dir": f"{TEST_LAKEHOUSE_OUT}/dq",
            **dq_spec_add_options,
        },
        "restore_prev_version": scenario.get("restore_prev_version", False),
    }


def _generate_dataframe(load_type: str) -> DataFrame:
    """Generate test dataframe.

    Args:
        load_type: batch or streaming.

    Returns: the generated dataframe.
    """
    if load_type == "batch":
        input_df = (
            ExecEnv.SESSION.read.format("csv")
            .schema(
                SchemaUtils.from_file(f"file://{TEST_RESOURCES}/dq_sales_schema.json")
            )
            .load(f"{TEST_RESOURCES}/data/source/part-01.csv")
        )
    else:
        input_df = (
            ExecEnv.SESSION.readStream.format("csv")
            .schema(
                SchemaUtils.from_file(f"file://{TEST_RESOURCES}/dq_sales_schema.json")
            )
            .load(f"{TEST_RESOURCES}/data/source/*")
        )

    return input_df


def _get_result_and_control_dfs(
    result: str, control: str, infer_schema: bool, result_is_table: bool = True
) -> Tuple[DataFrame, DataFrame]:
    """Helper to get the result and control dataframes.

    Args:
        result: the table to read from.
        control: the file name to read from.
        infer_schema: whether to infer the schema or not.
        result_is_table: whether the result is a table or a file.

    Returns: the result and control dataframes.
    """
    if result_is_table:
        dq_result_df = DataframeHelpers.read_from_table(result)
    else:
        dq_result_df = DataframeHelpers.read_from_file(
            location=result,
            file_format="json",
        )

    dq_control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data/{control}.csv",
        file_format="csv",
        options={"header": True, "delimiter": "|", "inferSchema": infer_schema},
    )

    return dq_result_df, dq_control_df
