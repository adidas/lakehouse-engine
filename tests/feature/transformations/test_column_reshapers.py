"""Test Column Reshaping Transformers."""
import pytest

from lakehouse_engine.core.definitions import OutputFormat
from lakehouse_engine.engine import load_data
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_PATH = "transformations/column_reshapers"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    [
        {"type": "batch", "scenario_name": "flatten_schema"},
        {"type": "streaming", "scenario_name": "flatten_schema"},
        {"type": "batch", "scenario_name": "explode_arrays"},
        {"type": "streaming", "scenario_name": "explode_arrays"},
        {"type": "batch", "scenario_name": "flatten_and_explode_arrays_and_maps"},
        {"type": "streaming", "scenario_name": "flatten_and_explode_arrays_and_maps"},
    ],
)
def test_column_reshapers(scenario: dict) -> None:
    """Test column reshaping transformers.

    Args:
        scenario: scenario to test.
            flatten_schema: This test flattens the struct.
            explode_arrays: This test explode the array columns specified.
            flatten_and_explode_arrays_and_maps: This test flattens the struct
                and explode the array  and map columns specified.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario['scenario_name']}/data/source/*.json",
        f"{TEST_LAKEHOUSE_IN}/{scenario['scenario_name']}/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario['scenario_name']}/*schema.json",
        f"{TEST_LAKEHOUSE_IN}/{scenario['scenario_name']}/",
    )

    load_data(
        f"file://{TEST_RESOURCES}/{scenario['scenario_name']}/{scenario['type']}.json"
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario['scenario_name']}/data/control/*.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario['scenario_name']}/data/",
    )

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario['scenario_name']}/{scenario['type']}/data",
        file_format=OutputFormat.DELTAFILES.value,
    )

    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario['scenario_name']}/data/"
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)
