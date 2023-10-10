"""Test Join Transformers."""
from typing import List

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

TEST_PATH = "transformations/joiners"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    [
        ["streaming", "control_scenario_1_and_2"],
        ["streaming_without_broadcast", "control_scenario_1_and_2"],
        ["streaming_without_column_rename", "control_scenario_3"],
        ["streaming_foreachBatch", "control_scenario_1_and_2"],
        ["batch", "control_scenario_1_and_2"],
    ],
)
def test_joiners(scenario: List[str]) -> None:
    """Test join transformers.

    Args:
        scenario: scenario to test.
            streaming - join streaming scenario.
            streaming_without_broadcast - same as streaming scenario but without
            broadcast join. Note: also differs by partitioning by customer and date,
            not only date.
            streaming_without_column_rename - same as streaming scenario but without
            renaming name column to customer_name.
            streaming_foreachBatch - join streaming scenario in foreachBatch mode.
            batch - join batch scenario.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/customer-part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/data/customers/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/sales-part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/data/sales/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/*schema.json",
        f"{TEST_LAKEHOUSE_IN}/",
    )

    if scenario[0] != "batch":
        load_data(f"file://{TEST_RESOURCES}/{scenario[0]}.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/sales-part-02.csv",
        f"{TEST_LAKEHOUSE_IN}/data/sales/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario[0]}.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/control/*.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/data/",
    )

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario[0]}/data",
        file_format=OutputFormat.DELTAFILES.value,
    )
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data/{scenario[1]}.csv",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_IN}/{scenario[1]}_schema.json"
        ),
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)
