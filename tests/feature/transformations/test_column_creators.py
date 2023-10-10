"""Test Column Creator Transformers."""
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

TEST_PATH = "transformations/column_creators"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    ["streaming", "batch"],
)
def test_column_creators(scenario: str) -> None:
    """Test column creators.

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

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/data",
        file_format=OutputFormat.DELTAFILES.value,
    )
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data",
        file_format=InputFormat.JSON.value,
        options={"multiLine": "true"},
    ).select(
        "salesorder",
        "item",
        "date",
        "customer",
        "article",
        "amount",
        "dummy_string",
        "dummy_int",
        "dummy_double",
        "dummy_boolean",
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)
