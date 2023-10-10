"""Test full loads."""
from typing import List

import pytest

from lakehouse_engine.core.definitions import InputFormat
from lakehouse_engine.engine import load_data
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_PATH = "full_load"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    [
        ["with_filter", InputFormat.PARQUET.value],
        ["with_filter_partition_overwrite", InputFormat.DELTAFILES.value],
        ["full_overwrite", InputFormat.DELTAFILES.value],
    ],
)
def test_batch_full_load(scenario: List[str]) -> None:
    """Test full loads in batch mode.

    Args:
        scenario: scenario to test.
             with_filter - loads in full but applies a filter to the source.
             with_filter_partition_overwrite - loads in full but only overwrites
             partitions that are contained in the data being loaded, keeping
             untouched partitions in the target table, therefore not doing a
             complete overwrite.
             full_overwrite - loads in full and overwrites target table.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario[0]}/batch_init.json")

    LocalStorage.clean_folder(
        f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/data",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/source/part-02.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario[0]}/batch.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/control/part-01.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario[0]}/data/",
    )

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario[0]}/data",
        file_format=scenario[1],
    )
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario[0]}/data"
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)
