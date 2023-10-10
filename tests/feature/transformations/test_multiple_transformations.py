"""Test multiple transformations and output specs on the same ACON."""
import pytest

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

TEST_PATH = "transformations/multiple_transform"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    ["batch"],
)
def test_multiple_transformations(scenario: str) -> None:
    """Tests multiple transformations available in the ACON transform_specs.\
    Transformations are saved in different locations, according to the output_specs.

    Args:
        scenario: scenario to test.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/*.csv",
        f"{TEST_LAKEHOUSE_IN}/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/*schema.json",
        f"{TEST_LAKEHOUSE_IN}/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario}.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/control/*.json",
        f"{TEST_LAKEHOUSE_CONTROL}/data/",
    )

    result_transform_df1 = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/orders_customer_cols/data",
        file_format=OutputFormat.DELTAFILES.value,
    )
    result_transform_df2 = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/orders_kpi_cols/data",
        file_format=OutputFormat.DELTAFILES.value,
    )
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data",
        file_format=InputFormat.JSON.value,
        options={"multiLine": "true"},
    )

    assert not DataframeHelpers.has_diff(
        result_transform_df1, control_df.select("date", "country", "customer_number")
    )
    assert not DataframeHelpers.has_diff(
        result_transform_df2, control_df.select("date", "city", "amount")
    )
