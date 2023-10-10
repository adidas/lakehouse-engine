"""Test drop_duplicate_rows function."""
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

TEST_PATH = "transformations/drop_duplicate_rows"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    [
        ["batch"],
        ["streaming"],
    ],
)
def test_drop_duplicate_rows(scenario: str) -> None:
    """Tests drop duplicate rows transformer available in the ACON transform_specs.

    Args:
        scenario: scenario to test.
            batch - test the transformer utilization in batch mode.
                The transformer is tested 3 times: 1) without providing arguments;
                2) providing an empty list ([]); and 3) providing a list with
                columns names (["order_number","item_number"]). This happens
                using 3 different dataframes saved in different locations
                specified in the ACON. In the 2 first times, the transformer
                should have the same behaviour has using the pyspark
                function distinct().
            streaming - the same as batch but using streaming.

    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/*schema.json",
        f"{TEST_LAKEHOUSE_IN}/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/*.csv",
        f"{TEST_LAKEHOUSE_IN}/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/control/{scenario[0]}_*.json",
        f"{TEST_LAKEHOUSE_CONTROL}/data/",
    )

    load_data(f"file://{TEST_RESOURCES}/{scenario[0]}.json")

    control_drop_duplicates = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data/{scenario[0]}_drop_duplicates.json",
        file_format=InputFormat.JSON.value,
        options={"multiLine": "true"},
    )

    control_distinct = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data/{scenario[0]}_distinct.json",
        file_format=InputFormat.JSON.value,
        options={"multiLine": "true"},
    )

    df_transform_columns = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario[0]}/columns/data",
        file_format=OutputFormat.DELTAFILES.value,
    )
    assert not DataframeHelpers.has_diff(df_transform_columns, control_drop_duplicates)

    df_transform_no_args = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario[0]}/orders_duplicate_no_args/data",
        file_format=OutputFormat.DELTAFILES.value,
    )
    assert not DataframeHelpers.has_diff(df_transform_no_args, control_distinct)

    df_transform_empty = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario[0]}/orders_duplicate_empty/data",
        file_format=OutputFormat.DELTAFILES.value,
    )
    assert not DataframeHelpers.has_diff(df_transform_empty, control_distinct)
