"""Test delta loads with record mode based cdc."""
from typing import List

import pytest

from lakehouse_engine.core.definitions import InputFormat
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.engine import load_data
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_PATH = "delta_load/record_mode_cdc"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    [
        ["with_deletes_additional_columns", "csv"],
        ["with_duplicates", "csv"],
        ["with_upserts_only_removed_columns", "json"],
    ],
)
def test_batch_delta_load(scenario: List[str]) -> None:
    """Test delta loads in batch mode.

    Args:
        scenario: scenario to test (name and file format).
    """
    _create_table(f"{scenario[0]}", f"{TEST_LAKEHOUSE_OUT}/{scenario[0]}/data")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/source/part-01.{scenario[1]}",
        f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario[0]}/batch_init.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/source/part-0[2,3,4].{scenario[1]}",
        f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/data/",
    )

    load_data(f"file://{TEST_RESOURCES}/{scenario[0]}/batch_delta.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/control/part-01.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario[0]}/data/",
    )

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario[0]}/data",
        file_format=InputFormat.DELTAFILES.value,
    )
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario[0]}/data"
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)


@pytest.mark.parametrize(
    "scenario",
    [
        ["late_arriving_changes", "batch"],
        ["out_of_order_changes", "batch"],
        ["late_arriving_changes", "streaming"],
        ["out_of_order_changes", "streaming"],
    ],
)
def test_file_by_file(scenario: str) -> None:
    """Test delta loads in batch mode.

    Args:
        scenario: scenario to test.
            late_arriving_changes - This test checks if if changes arrive late (certain
            changes on part-02 are incomplete and only arrive in part-03), the data
            stays consistent.
            out_of_order_changes - This test checks if by loading the data out of order
            (part-03 is loaded before part-02) the delta table stays consistent.
    """
    _create_table(
        f"{scenario[0]}_{scenario[1]}",
        f"{TEST_LAKEHOUSE_OUT}/{scenario[0]}/{scenario[1]}/data",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/{scenario[1]}/data/",
    )
    load_data(
        f"file://{TEST_RESOURCES}/{scenario[0]}/"
        f"{scenario[1] + ('_init' if scenario[1] == 'batch' else '_delta')}.json"
    )

    if scenario[0] == "out_of_order_changes":
        second_file = "part-03"
        third_file = "part-02"
    else:
        second_file = "part-02"
        third_file = "part-03"

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/source/{second_file}.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/{scenario[1]}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario[0]}/{scenario[1]}_delta.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/source/{third_file}.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/{scenario[1]}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario[0]}/{scenario[1]}_delta.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/source/part-04.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/{scenario[1]}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario[0]}/{scenario[1]}_delta.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/control/part-01.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario[0]}/{scenario[1]}/data/",
    )

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario[0]}/{scenario[1]}/data",
        file_format=InputFormat.DELTAFILES.value,
    )
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario[0]}/{scenario[1]}/data"
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)


@pytest.mark.parametrize("scenario", ["backfill"])
def test_backfill(scenario: str) -> None:
    """Test backfill process of a delta load based table.

    Args:
        scenario: scenario to test.
            This test performs a regular delta load and, after that, backfills from the
            source where we simulate that all data contained in part-2, part-3 and
            part-04 has changed to be amount * 10.
    """
    _create_table(f"{scenario}", f"{TEST_LAKEHOUSE_OUT}/{scenario}/data")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario}/batch_init.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-0[2,3,4].csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )

    load_data(f"file://{TEST_RESOURCES}/{scenario}/batch_delta.json")

    LocalStorage.delete_file(f"{TEST_LAKEHOUSE_IN}/{scenario}/data/part-0[2,3,4].csv")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-05.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )

    load_data(f"file://{TEST_RESOURCES}/{scenario}/batch_backfill.json")

    LocalStorage.delete_file(f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data/part-01.csv")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/control/part-01.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data/",
    )

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/data",
        file_format=InputFormat.DELTAFILES.value,
    )
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data"
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)


@pytest.mark.parametrize("scenario", ["direct_silver_load"])
def test_direct_silver_load(scenario: str) -> None:
    """Test a delta load based process that loads to bronze and silver in the same run.

    We get data from the source, load it to bronze and then into silver, without needing
    to run two separate algorithms.

    Args:
        scenario: scenario to test.
    """
    _create_table(f"{scenario}_bronze", f"{TEST_LAKEHOUSE_OUT}/{scenario}/bronze/data")
    _create_table(f"{scenario}_silver", f"{TEST_LAKEHOUSE_OUT}/{scenario}/silver/data")

    scenario = "direct_silver_load"
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario}/batch_init.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-0[2,3,4].csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )

    load_data(f"file://{TEST_RESOURCES}/{scenario}/batch_delta.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/control/part-01.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/bronze/data/",
    )

    bronze_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/bronze/data",
        file_format=InputFormat.DELTAFILES.value,
    )
    control_bronze_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/bronze/data", file_format="csv"
    )

    assert not DataframeHelpers.has_diff(bronze_df, control_bronze_df)

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/control/part-02.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/silver/data/",
    )

    silver_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/silver/data",
        file_format=InputFormat.DELTAFILES.value,
    )
    control_silver_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/silver/data", file_format="csv"
    )

    assert not DataframeHelpers.has_diff(silver_df, control_silver_df)


def _create_table(table_name: str, location: str) -> None:
    """Create test table.

    Args:
        table_name: name of the table.
        location: location of the table.
    """
    ExecEnv.SESSION.sql(
        f"""
        CREATE TABLE IF NOT EXISTS test_db.{table_name} (
            extraction_timestamp string,
            actrequest_timestamp string,
            request string,
            datapakid int,
            partno int,
            record int,
            salesorder int,
            item int,
            recordmode string,
            date int,
            customer string,
            article string,
            amount int
        )
        USING delta
        LOCATION '{location}'
        """
    )
