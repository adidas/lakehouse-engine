"""Test data quality process in different types of data loads."""
from os.path import exists
from typing import Any

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import array_sort, col, from_json, regexp_replace, transform

from lakehouse_engine.core.definitions import DQDefaults, DQFunctionSpec, DQSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.dq_processors.dq_factory import DQFactory
from lakehouse_engine.dq_processors.exceptions import DQValidationsFailedException
from lakehouse_engine.engine import load_data
from lakehouse_engine.utils.schema_utils import SchemaUtils
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
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
            "name": "delta_with_duplicates",
            "read_type": "streaming",
            "expectations": 72,
            "row_count": 19,
        },
        {
            "name": "full_overwrite",
            "read_type": "batch",
            "expectations": 38,
            "row_count": 19,
        },
        {"name": "console_writer", "read_type": "batch", "expectations": 38},
    ],
)
def test_load_with_dq_assistant(scenario: dict, caplog: Any, capsys: Any) -> None:
    """Test the data quality assistant process as part of the load_data algorithm.

    Description of the test scenarios:
        - delta_with_duplicates - tests generating expectations in a streaming test with
        transformations and writing into a target location.
        - full_overwrite - generating expectations using batch with no transformations
        and writing fully into a target.
        - console_writer - test generating expectations using batch and a console writer
        which will become of the most common real use cases, generate expectations while
        exploring the data.

    Args:
        scenario: scenario to test.
        caplog: captured log.
        capsys: captured stdout and stderr.
    """
    test_name = "load_with_dq_assistant"
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{test_name}/{scenario['name']}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{test_name}/{scenario['name']}/data/",
    )
    load_data(
        f"file://{TEST_RESOURCES}/{test_name}/{scenario['name']}/"
        f"{scenario['read_type']}_init.json"
    )

    if scenario["name"] != "console_writer":
        result_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_OUT}/{test_name}/{scenario['name']}/data",
            file_format="delta",
        )
        assert result_df.count() == scenario["row_count"]
    else:
        assert (
            "|         4|   1|20170430|customer3|article3|   800|"
            in capsys.readouterr().out
        )

    assert (
        f"{scenario['expectations']} expectations were generated by the "
        f"Onboarding Data Assistant." in caplog.text
    )

    # we don't check for the args content because the order is not ensured,
    # making the tests fail.
    assert (
        """'dq_function': '{"function": "expect_table_row_count_to_be_between","""
        + """ "args":"""
        in caplog.text
    )

    assert (
        """'dq_function': '{"function": "expect_table_columns_to_match_set","""
        + """ "args":"""
        in caplog.text
    )


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
                DQFunctionSpec(
                    "expect_table_row_count_to_be_between",
                    {"min_value": 0, "max_value": 1},
                )
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

    Args:
        scenario: scenario to test.
        caplog: captured log.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario['dq_type']}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario['dq_type']}/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario['dq_type']}"
        f"/data/control/data_{scenario['dq_type']}.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario['dq_type']}/data/",
    )
    input_data = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_IN}/{scenario['dq_type']}/data",
        file_format="csv",
        options={"header": True, "delimiter": "|", "inferSchema": True},
    )
    location = TEST_LAKEHOUSE_OUT.replace("file://", "")
    dq_spec = DQSpec(
        spec_id=scenario["spec_id"],
        input_id="sales_orders",
        dq_type=scenario["dq_type"],
        store_backend="file_system",
        local_fs_root_dir=f"{location}/{scenario['dq_type']}/",
        assistant_options=None,
        result_sink_format="json",
        result_sink_explode=False,
        unexpected_rows_pk=[
            "salesorder",
            "item",
            "date",
            "customer",
        ],
        dq_functions=scenario["dq_functions"],
        result_sink_location=f"{TEST_LAKEHOUSE_OUT}/{scenario['dq_type']}/data",
        fail_on_error=scenario["fail_on_error"],
        critical_functions=scenario["critical_functions"],
        max_percentage_failure=scenario["max_percentage_failure"],
    )

    if scenario["spec_id"] == "dq_failure":
        with pytest.raises(DQValidationsFailedException) as ex:
            DQFactory.run_dq_process(dq_spec, input_data)
        assert "Data Quality Validations Failed!" in str(ex.value)
    elif scenario["spec_id"] == "dq_failure_critical_functions":
        with pytest.raises(DQValidationsFailedException) as ex:
            DQFactory.run_dq_process(dq_spec, input_data)
        assert (
            "Data Quality Validations Failed, the following critical expectations "
            "failed: ['expect_table_row_count_to_be_between']." in str(ex.value)
        )
    elif scenario["spec_id"] == "dq_failure_max_percentage":
        with pytest.raises(DQValidationsFailedException) as ex:
            DQFactory.run_dq_process(dq_spec, input_data)
        assert "Max error threshold is being surpassed!" in str(ex.value)
    else:
        DQFactory.run_dq_process(dq_spec, input_data)

        result_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_OUT}/{scenario['dq_type']}/data", file_format="json"
        )

        if scenario["spec_id"] == "dq_failure_error_disabled":
            assert (
                "1 out of 1 Data Quality Expectation(s) have failed! "
                "Failed Expectations" in caplog.text
            )

        control_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_CONTROL}/{scenario['dq_type']}/data",
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

        # test if the run_results column has the correct keys
        json_schema = ExecEnv.SESSION.read.json(
            result_df.rdd.map(lambda row: str(row.run_results))
        ).schema
        result_df = result_df.withColumn(
            "run_results", from_json(col("run_results"), json_schema)
        )
        assert result_df.select("run_results.*").columns == [
            "actions_results",
            "validation_result",
        ]

        assert exists(
            f"{location}/{scenario['dq_type']}/{DQDefaults.DATA_DOCS_PREFIX.value}"
        )


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "spec_id": "generate_expectations",
            "dq_type": "assistant",
            "assistant_options": {"log_expectations": True},
        },
        {
            "spec_id": "generate_plot_and_display_profile",
            "dq_type": "assistant",
            "assistant_options": {
                "log_expectations": True,
                "plot_metrics": True,
                "plot_expectations_and_metrics": True,
                "display_profiling": True,
            },
        },
        {
            "spec_id": "generate_plot_and_profile_path",
            "dq_type": "assistant",
            "assistant_options": {
                "log_expectations": True,
                "profiling_path": f"file://{TEST_LAKEHOUSE_OUT}/profiling",
            },
        },
    ],
)
def test_assistant_dq_spec(scenario: dict, caplog: Any) -> None:
    """Test the data quality assistant process using DQSpec.

    Data Quality Functions tested using validator:
        - generate_expectations: scenario using the default behavior of the assistant,
        which is the generation of expectations. It only adds one assistant_option to
        log the generated expectations into the console, so that we can assert them.
        - generate_plot_and_display_profile: scenario testing to plot the metrics and
        expectations generated and also to display the profiling of the data.
        - generate_plot_and_profile_path: scenario testing to generate expectations and
        store the profiling in a html file.

    Args:
        scenario: scenario to test.
        caplog: captured log.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario['dq_type']}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario['dq_type']}/data/",
    )

    input_data = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_IN}/{scenario['dq_type']}/data",
        file_format="csv",
        options={"header": True, "delimiter": "|", "inferSchema": True},
    )

    dq_spec = DQSpec(
        spec_id=scenario["spec_id"],
        input_id="sales_orders",
        dq_type=scenario["dq_type"],
        store_backend="file_system",
        local_fs_root_dir=f'{TEST_LAKEHOUSE_OUT}/{scenario["dq_type"]}/',
        assistant_options=scenario["assistant_options"],
    )

    DQFactory.run_dq_process(dq_spec, input_data)

    assert (
        "112 expectations were generated by the Onboarding Data Assistant."
        in caplog.text
    )

    # we don't check for the args content because the order is not ensured,
    # making the tests fail.
    assert (
        """'dq_function': '{"function": "expect_table_row_count_to_be_between","""
        + """ "args":"""
        in caplog.text
    )

    assert (
        """'dq_function': '{"function": "expect_table_columns_to_match_set","""
        + """ "args":"""
        in caplog.text
    )

    if scenario["spec_id"] in [
        "generate_plot_and_display_profile",
        "generate_plot_and_profile_path",
    ]:
        assert "Finished Profiling with Pandas" in caplog.text

    if scenario["spec_id"] == "generate_plot_and_profile_path":
        assert exists(f"{TEST_LAKEHOUSE_OUT}/profiling")


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
