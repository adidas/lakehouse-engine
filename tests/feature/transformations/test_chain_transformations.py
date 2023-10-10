"""Test chain transformer."""
from typing import Any

import pytest
from pyspark.sql.utils import StreamingQueryException

from lakehouse_engine.core.definitions import InputFormat, OutputFormat
from lakehouse_engine.engine import load_data
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_PATH = "transformations/chain_transformations"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    [
        {"scenario_name": "batch"},
        {"scenario_name": "streaming"},
        {"scenario_name": "streaming_batch"},
        {"scenario_name": "write_streaming_struct_data"},
        {"scenario_name": "write_streaming_struct_data_fail"},
    ],
)
def test_chain_transformations(scenario: dict, caplog: Any) -> None:
    """Test chain transformation.

    Args:
        scenario: scenario to test.
            batch - scenario where we are using batch dataframes;
            streaming - scenario where we are using streaming dataframes;
            streaming_batch - scenario where we are using batch and streaming
                dataframes;
            write_streaming_struct_data - scenario where are we making transformations
                in first place, use this result to apply other transform and write
                in micro batch;
            write_streaming_struct_data_fail - scenario where we are trying to use a
                result from micro batch transformation into another transform, this
                one should fail because we cannot have dependency from micro batch.
        caplog: captured log.
    """
    _prepare_files()

    if scenario["scenario_name"] == "write_streaming_struct_data_fail":
        with pytest.raises(
            StreamingQueryException,
            match=".*An exception was raised by the Python Proxy.*",
        ):
            load_data(f"file://{TEST_RESOURCES}/acons/{scenario['scenario_name']}.json")

        assert "Column 'sample_json_field1' does not exist" in caplog.text
    else:
        load_data(f"file://{TEST_RESOURCES}/acons/{scenario['scenario_name']}.json")

        result_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_OUT}/{scenario['scenario_name']}/data",
            file_format=OutputFormat.DELTAFILES.value,
        )

        if scenario["scenario_name"] == "write_streaming_struct_data":
            control_df = DataframeHelpers.read_from_file(
                f"{TEST_LAKEHOUSE_CONTROL}/data/struct_data.json",
                file_format=InputFormat.JSON.value,
                options={"multiLine": "true"},
            ).select(
                "salesorder",
                "item",
                "article",
                "sample_json_field1",
                "sample_json_field4",
                "item_amount_json",
            )
        else:
            control_df = DataframeHelpers.read_from_file(
                f"{TEST_LAKEHOUSE_CONTROL}/data/chain_control.csv"
            )

        assert not DataframeHelpers.has_diff(result_df, control_df)


def _prepare_files() -> None:
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/source/sales_historical.csv",
        f"{TEST_LAKEHOUSE_IN}/source/sales_historical/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/source/sales_new.csv",
        f"{TEST_LAKEHOUSE_IN}/source/sales_new/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/source/customers.csv",
        f"{TEST_LAKEHOUSE_IN}/source/customers/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/source/struct_data.csv",
        f"{TEST_LAKEHOUSE_IN}/source/struct_data/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/schema/*.json",
        f"{TEST_LAKEHOUSE_IN}/schema/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/control/*.*",
        f"{TEST_LAKEHOUSE_CONTROL}/data/",
    )
