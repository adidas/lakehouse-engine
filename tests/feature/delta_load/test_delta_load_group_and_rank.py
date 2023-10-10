"""Test delta loads with group and rank."""
from typing import List

import pytest

from lakehouse_engine.core.definitions import InputFormat
from lakehouse_engine.core.exec_env import ExecEnv
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

TEST_PATH = "delta_load/group_and_rank"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    [
        ["with_duplicates_in_same_file", "batch"],
        ["with_duplicates_in_same_file", "streaming"],
        ["fail_with_duplicates_in_same_file", "batch"],
        ["fail_with_duplicates_in_same_file", "streaming"],
    ],
)
def test_delta_load_group_and_rank(scenario: List[str]) -> None:
    """Test delta loads in batch mode.

    Args:
        scenario: scenario to test.
            with_duplicates_in_same_file - This test includes duplicated rows in the
            same file produced by the source (e.g., an order is cancelled and created
            within the same file).
            fail_with_duplicates_in_same_file - purposely checks if the delta load fails
            (result has a diff compared to the control data), because sales order 7 item
            1 as cancelled status before created in the second source data file.
    """
    _create_table(scenario)

    execute_loads(scenario, 1)

    if scenario[1] == "streaming":
        # simulate a scenario where the same data is loaded twice in streaming mode
        execute_loads(scenario, 2)

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/control/{scenario[1]}.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario[0]}/{scenario[1]}/data/",
    )

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario[0]}/{scenario[1]}/data",
        file_format=InputFormat.DELTAFILES.value,
    )
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario[0]}/{scenario[1]}/data/{scenario[1]}.csv",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_IN}/{scenario[0]}/{scenario[1]}/"
            f"control_{scenario[1]}_schema.json"
        ),
    )

    if scenario[0] == "fail_with_duplicates_in_same_file":
        # sales order 7 item 1 in second file has event cancelled before created
        assert DataframeHelpers.has_diff(result_df, control_df)
    else:
        assert not DataframeHelpers.has_diff(result_df, control_df)


def execute_loads(scenario: List[str], iteration: int) -> None:
    """Execute the data loads.

    Args:
        scenario: scenario to test.
        iteration: number indicating the iteration in the testing process.
            This is useful because in this test we want to repeat the same loading
            process twice, to simulate a scenario where the same data is loaded twice.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/source/WE_SO_SCL_202108111400000000.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/{scenario[1]}"
        f"/data/WE_SO_SCL_202108111400000000.csv{iteration}",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/*schema.json",
        f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/{scenario[1]}/",
    )
    load_data(
        f"file://{TEST_RESOURCES}/{scenario[0]}/"
        f"{scenario[1] + ('_init' if scenario[1] == 'batch' else '_delta')}.json"
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/source/WE_SO_SCL_202108111500000000.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/{scenario[1]}"
        f"/data/WE_SO_SCL_202108111500000000.csv{iteration}",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario[0]}/{scenario[1]}_delta.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/source/WE_SO_SCL_202108111600000000.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/{scenario[1]}"
        f"/data/WE_SO_SCL_202108111600000000.csv{iteration}",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario[0]}/{scenario[1]}_delta.json")


def _create_table(scenario: List[str]) -> None:
    """Create test table.

    Args:
        scenario: scenario being tested.
    """
    ExecEnv.SESSION.sql(
        f"""
        CREATE TABLE IF NOT EXISTS test_db.{scenario[0]}_{scenario[1]} (
            salesorder int,
            item int,
            event string,
            changed_on int,
            date int,
            customer string,
            article string,
            amount int,
            {"extraction_date string,"
        if scenario[1] == "streaming" else "lhe_row_id int,"}
            {"lhe_batch_id int," if scenario[1] == "streaming" else ""}
            {"lhe_row_id int"
        if scenario[1] == "streaming" else "extraction_date string"}
        )
        USING delta
        LOCATION '{TEST_LAKEHOUSE_OUT}/{scenario[0]}/{scenario[1]}/data'
        """
    )
