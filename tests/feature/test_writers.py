"""Test engine writers.

Delta merge tests writers weren't added because it is always batch,
micro batch or normal batch, but always batch. Also, we have another
test like delta_load that uses delta_merge_writer.
Kafka writer weren't added also, because we cannot
simulate kafka on local tests. All other writers were covered.
"""
import logging
import os
import random
import string
from typing import Any, Optional, OrderedDict

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from lakehouse_engine.core.definitions import OutputFormat, OutputSpec
from lakehouse_engine.engine import load_data
from lakehouse_engine.io.exceptions import NotSupportedException
from lakehouse_engine.io.writers.dataframe_writer import DataFrameWriter
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_NAME = "writers"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_NAME}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_NAME}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_NAME}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_NAME}"


@pytest.mark.parametrize(
    "scenario",
    [
        {"scenario_name": "write_batch_files"},
        {"scenario_name": "write_streaming_files"},
        {
            "scenario_name": "write_streaming_foreachBatch_files",
        },
    ],
)
def test_write_to_files(scenario: dict) -> None:
    """Test file writer.

    Args:
        scenario: scenario to test.
    """
    _prepare_files()

    load_data(f"file://{TEST_RESOURCES}/acons/{scenario['scenario_name']}.json")

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario['scenario_name']}/data",
        file_format=OutputFormat.DELTAFILES.value,
    )

    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data/writers_control.csv"
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)


@pytest.mark.parametrize(
    "scenario",
    [
        {"scenario_name": "write_batch_jdbc"},
        {"scenario_name": "write_streaming_foreachBatch_jdbc"},
    ],
)
def test_write_to_jdbc(scenario: dict) -> None:
    """Test jdbc writer.

    Args:
        scenario: scenario to test.
    """
    _prepare_files()

    os.mkdir(f"{TEST_LAKEHOUSE_OUT}/{scenario['scenario_name']}/")

    load_data(f"file://{TEST_RESOURCES}/acons/{scenario['scenario_name']}.json")

    result_df = DataframeHelpers.read_from_jdbc(
        f"jdbc:sqlite:{TEST_LAKEHOUSE_OUT}/{scenario['scenario_name']}/test.db",
        f"{scenario['scenario_name']}",
    )

    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data/writers_control.csv"
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)


@pytest.mark.parametrize(
    "scenario",
    [
        {"scenario_name": "write_batch_table"},
        {"scenario_name": "write_streaming_table"},
        {"scenario_name": "write_streaming_foreachBatch_table"},
    ],
)
def test_write_to_table(scenario: dict) -> None:
    """Test table writer.

    Args:
        scenario: scenario to test.
    """
    _prepare_files()

    load_data(f"file://{TEST_RESOURCES}/acons/{scenario['scenario_name']}.json")

    result_df = DataframeHelpers.read_from_table(f"test_db.{scenario['scenario_name']}")

    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data/writers_control.csv"
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)


@pytest.mark.parametrize(
    "scenario",
    [
        {"scenario_name": "write_batch_console"},
        {"scenario_name": "write_streaming_console"},
        {"scenario_name": "write_streaming_foreachBatch_console"},
    ],
)
def test_write_to_console(scenario: dict, capsys: Any) -> None:
    """Test console writer.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """
    _prepare_files()

    load_data(f"file://{TEST_RESOURCES}/acons/{scenario['scenario_name']}.json")

    captured = capsys.readouterr()

    logging.info(captured.out)

    assert "20140601|customer1|article3|" in captured.out


@pytest.mark.parametrize(
    "scenario",
    [
        {"scenario_name": "write_batch_dataframe"},
        {"scenario_name": "write_streaming_dataframe"},
        {"scenario_name": "write_streaming_foreachBatch_dataframe"},
    ],
)
def test_write_to_dataframe(scenario: dict, capsys: Any) -> None:
    """Test dataframe writer returning the output by OutputSpec.

    Description of the test scenarios:
        - write_batch_dataframe - test writing a DataFrame from two batch sources,
        uniting both sources.
        It's generated a DataFrame containing the data from both sources.
        - write_streaming_dataframe - similar to write_batch_dataframe but inputting
        data from a stream.
        - write_streaming_foreachBatch_dataframe - similar to write_batch_dataframe but
        mixing batch and streaming,
        so the first source from batch and the second from a stream.
        This test have the responsibility to
        execute the writer using micro batch strategy.


    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_IN}/source")
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_OUT}/{scenario['scenario_name']}")
    _prepare_files()

    result = load_data(
        f"file://{TEST_RESOURCES}/acons/{scenario['scenario_name']}.json"
    )

    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data/writers_control.csv"
    )
    expected_keys = ["sales"]

    assert not DataframeHelpers.has_diff(result.get("sales"), control_df)
    assert len(result.keys()) == len(expected_keys)
    assert all(
        subject == expected for subject, expected in zip(result.keys(), expected_keys)
    )


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "write_streaming_df_with_checkpoint",
            "control": "streaming_dataframe",
        },
        {
            "scenario_name": "write_streaming_foreachBatch_df_with_checkpoint",
            "control": "streaming_dataframe_foreachBatch",
        },
    ],
)
def test_write_to_dataframe_checkpoints(scenario: dict, capsys: Any) -> None:
    """Test dataframe writer using checkpoint for the next run.

    In this test our InputSpecs have the option `maxFilesPerTrigger`,
    this option forces our stream to read a maximum files per iteration,
    this property also needs to have a checkpoint location
    because spark internally needs to control the state of reading the
    files.

    Description of the test scenarios:
        - write_streaming_dataframe - test if the checkpoint is working
         as expected when writing the data
         from stream to DataFrame.
         We have two different input files for each source
         we expect to read just the first
         in the first execution and the second in the next one.
         - write_streaming_foreachBatch_dataframe - test if the
         checkpoint is working as expected when writing
         the data from stream and batch using
         the micro batch strategy to DataFrame.
         As we have two different input files for each source
         we expect to read just the first file
         in the first execution and the second in the
         next one for the stream with checkpoint source.
         On the batch source we expect to read the first
         file in the first run and both files in the second run.


    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_IN}/source")
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_OUT}/{scenario['scenario_name']}")

    for iteration in range(1, 2):
        _prepare_files(iteration)
        result = load_data(
            f"file://{TEST_RESOURCES}/acons/{scenario['scenario_name']}.json"
        )

        control_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_CONTROL}/data/"
            f"writers_control_{scenario['control']}_{iteration}.csv"
        )
        expected_keys = ["sales"]

        assert not DataframeHelpers.has_diff(result.get("sales"), control_df)
        assert len(result.keys()) == len(expected_keys)
        assert all(
            subject == expected
            for subject, expected in zip(result.keys(), expected_keys)
        )


@pytest.mark.parametrize(
    "scenario",
    [
        {"scenario_name": "write_streaming_multiple_dfs"},
    ],
)
def test_multiple_write_to_dataframe(scenario: dict, capsys: Any) -> None:
    """Test dataframe writer chaining ACON calls.

    This test have the objective to demonstrate how you can use
    the output from an ACON as input to another ACON,
    showing the flexibility of this writer unlock to us.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """
    _prepare_files()

    multiple_df_result = load_data(
        f"file://{TEST_RESOURCES}/acons/{scenario['scenario_name']}.json"
    )

    generated_acon = _generate_acon_from_source(multiple_df_result)

    result = load_data(acon=generated_acon)
    result_keys = list(multiple_df_result.keys()) + list(result.keys())

    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data/writers_control.csv"
    )
    expected_keys = ["sales_historical", "sales_new", "sales"]

    assert not DataframeHelpers.has_diff(result.get("sales"), control_df)
    assert len(result_keys) == len(expected_keys)
    assert all(
        subject == expected for subject, expected in zip(result_keys, expected_keys)
    )


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "write_streaming_processing_time_dataframe",
            "streaming_processing_time": "2 seconds",
        },
        {
            "scenario_name": "write_streaming_continuous_dataframe",
            "streaming_continuous": "2 seconds",
        },
    ],
)
def test_write_to_dataframe_exception(scenario: dict, capsys: Any) -> None:
    """Test expected exception for dataframe writer on stream cases.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """

    def dataframe_writer(
        df: DataFrame = None,
        data: OrderedDict = None,
        streaming_processing_time: Optional[str] = None,
        streaming_continuous: Optional[str] = None,
    ) -> DataFrameWriter:
        """Create DataFrame Writer.

        Args:
            df: dataframe containing the data to append.
            data: list of all dfs generated on previous steps before writer.
            streaming_processing_time: if streaming query is to be kept alive,
                this indicates the processing time of each micro batch.
            streaming_continuous: set a trigger that runs
                a continuous query with a given
        checkpoint interval.
        """
        if not df:
            df = DataframeHelpers.create_empty_dataframe(StructType([]))

        spec = OutputSpec(
            spec_id=random.choice(string.ascii_letters),  # nosec
            input_id=random.choice(string.ascii_letters),  # nosec
            write_type=None,
            data_format=OutputFormat.DATAFRAME.value,
            streaming_processing_time=streaming_processing_time,
            streaming_continuous=streaming_continuous,
        )

        return DataFrameWriter(output_spec=spec, df=df.coalesce(1), data=data)

    with pytest.raises(NotSupportedException) as exception:
        dataframe_writer(
            streaming_processing_time=scenario.get("streaming_processing_time"),
            streaming_continuous=scenario.get("streaming_continuous"),
        ).write()

    assert (
        "DataFrame writer doesn't support processing time or continuous streaming"
        in str(exception.value)
    )


def _generate_acon_from_source(source: OrderedDict) -> dict:
    """Create an ACON from dictionary source containing resulted dataframes.

    Args:
        source: Dictionary containing source computed dataframes.
    """
    return {
        "input_specs": [
            {
                "spec_id": "sales_historical",
                "read_type": "batch",
                "data_format": "dataframe",
                "df_name": source.get("sales_historical"),
            },
            {
                "spec_id": "sales_new",
                "read_type": "batch",
                "data_format": "dataframe",
                "df_name": source.get("sales_new"),
            },
        ],
        "transform_specs": [
            {
                "spec_id": "union_dataframes",
                "input_id": "sales_historical",
                "transformers": [
                    {"function": "union", "args": {"union_with": ["sales_new"]}}
                ],
            }
        ],
        "output_specs": [
            {
                "spec_id": "sales",
                "input_id": "union_dataframes",
                "data_format": "dataframe",
            }
        ],
    }


def _prepare_files(iteration: int = 0) -> None:
    file_suffix = "*" if iteration == 0 else f"{iteration}"

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/source/sales_historical_{file_suffix}.csv",
        f"{TEST_LAKEHOUSE_IN}/source/sales_historical/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/source/sales_new_{file_suffix}.csv",
        f"{TEST_LAKEHOUSE_IN}/source/sales_new/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/schema/*.json",
        f"{TEST_LAKEHOUSE_IN}/schema/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/control/*.*",
        f"{TEST_LAKEHOUSE_CONTROL}/data/",
    )
