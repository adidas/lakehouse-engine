"""Test Data Masking Transformers."""

import pytest

from lakehouse_engine.core.definitions import OutputFormat
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

TEST_PATH = "transformations/data_maskers"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    ["drop_columns", "hash_masking"],
)
def test_data_maskers(scenario: str) -> None:
    """Test data masking transformers.

    Args:
        scenario: scenario to test.
            drop_columns - scenario where we mask data by dropping columns;
            hash_masking - scenario where we mask data by hashing columns.
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
        f"{TEST_RESOURCES}/data/control/*.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/data/",
    )

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/data",
        file_format=OutputFormat.DELTAFILES.value,
    )
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data/{scenario}.csv",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_IN}/{scenario}_control_schema.json"
        ),
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)
