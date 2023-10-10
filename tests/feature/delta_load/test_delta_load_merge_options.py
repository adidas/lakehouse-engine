"""Test delta loads with different merge options."""
from typing import List

import pytest

from lakehouse_engine.core.definitions import InputFormat
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

TEST_PATH = "delta_load/merge_options"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    ["update_column_set", "insert_column_set", "update_all"],
)
def test_delta_load_merge_options(scenario: List[str]) -> None:
    """Test upsert for specific columns in batch mode.

    Args:
        scenario: scenario to test.
            update_column_set - This test uses whenMatchedUpdate option. It allows to
                update a matched table row based on the rules defined in
                update_column_set, instead of updating all the columns of the matched
                table row with the values of the corresponding columns in the source
                 row.
            insert_column_set - This test uses whenNotMatchedInsert option. It allows to
                insert a new row to the target table based on the rules defined in
                insert_column_set, instead of inserting a new target Delta table row
                by assigning the target columns to the values of the corresponding
                columns in the source row.
            update_all - This test uses whenMatchedUpdateAll option. It allows to
                update a matched table updating all the columns with the values
                of the corresponding columns in the source row.
    """
    execute_loads(scenario)

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/data",
        file_format=InputFormat.DELTAFILES.value,
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/control/batch.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data/",
    )

    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data/batch.csv",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_IN}/{scenario}/control_batch_schema.json"
        ),
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)


def execute_loads(scenario: List[str]) -> None:
    """Execute the data loads.

    Args:
        scenario: scenario to test.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/*schema.json",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/WE_SO_SCL_202108111400000000.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/WE_SO_SCL_202108111400000000.csv",
    )

    load_data(f"file://{TEST_RESOURCES}/{scenario}/batch_init.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/WE_SO_SCL_202108111500000000.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/WE_SO_SCL_202108111500000000.csv",
    )

    load_data(f"file://{TEST_RESOURCES}/{scenario}/batch_delta.json")
