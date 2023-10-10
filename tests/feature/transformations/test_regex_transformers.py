"""Test Regex Transformers."""
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

TEST_PATH = "transformations/regex_transformers"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    ["with_regex_value"],
)
def test_regex_transformers(scenario: str) -> None:
    """Test regex transformers.

    Args:
        scenario: scenario to test.
            with_regex_value - test with_regex_value feature in the regex transformers.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/*.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/*schema.json",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario}/batch.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/control/part-01.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data/",
    )

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/data",
        file_format=OutputFormat.DELTAFILES.value,
    )
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_IN}/{scenario}/control_schema.json"
        ),
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)
