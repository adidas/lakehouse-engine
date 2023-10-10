"""Test append loads."""
from typing import Any

import pytest
from py4j.protocol import Py4JJavaError

from lakehouse_engine.engine import load_data
from lakehouse_engine.utils.configs.config_utils import ConfigUtils
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_NAME = "append_load"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_NAME}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_NAME}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_NAME}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_NAME}"


@pytest.mark.parametrize("scenario", ["jdbc_permissive"])
def test_permissive_jdbc_append_load(scenario: str) -> None:
    """Test append loads from jdbc source with permissive read mode.

    Args:
        scenario: scenario to test.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    _append_data_into_source(scenario)
    load_data(f"file://{TEST_RESOURCES}/{scenario}/batch_init.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-02.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    _append_data_into_source(scenario)
    load_data(f"file://{TEST_RESOURCES}/{scenario}/batch.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-03.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    _append_data_into_source(scenario)
    load_data(f"file://{TEST_RESOURCES}/{scenario}/batch.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/control/part-01.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data/",
    )

    result_df = DataframeHelpers.read_from_table(f"test_db.{scenario}_table")
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data"
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)


@pytest.mark.parametrize("scenario", ["failfast"])
def test_failfast_append_load(scenario: str) -> None:
    """Test append loads with failfast read mode.

    Args:
        scenario: scenario to test.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario}/batch_init.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-0[2,3].csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )

    with pytest.raises(Py4JJavaError) as e:
        # should raise malformed records due to failfast, as amount column was
        # renamed to amount2 and there is one more column in the pat-03.csv file.
        load_data(f"file://{TEST_RESOURCES}/{scenario}/batch.json")

    assert "Malformed CSV record" in str(e.value)


@pytest.mark.parametrize("scenario", ["streaming_dropmalformed"])
def test_streaming_dropmalformed(scenario: str) -> None:
    """Test append loads, in streaming mode, with dropmalformed read mode.

    Args:
        scenario: scenario to test.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario}/streaming.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-02.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario}/streaming.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-03.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario}/streaming.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/control/part-01.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data/",
    )

    result_df = DataframeHelpers.read_from_table(f"test_db.{scenario}_table")
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data",
        schema=ConfigUtils.read_json_acon(
            f"file://{TEST_RESOURCES}/{scenario}/streaming.json"
        )["input_specs"][0]["schema"],
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)


@pytest.mark.parametrize("scenario", ["streaming_with_terminators"])
def test_streaming_with_terminators(scenario: str, caplog: Any) -> None:
    """Test append loads, in streaming mode, with terminator functions.

    Args:
        scenario: scenario to test.
        caplog: captured log.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/{scenario}/streaming.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/control/part-01.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data/",
    )

    result_df = DataframeHelpers.read_from_table(f"test_db.{scenario}_table")
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data",
        schema=ConfigUtils.read_json_acon(
            f"file://{TEST_RESOURCES}/{scenario}/streaming.json"
        )["input_specs"][0]["schema"],
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)
    assert (
        "sql command: OPTIMIZE test_db.streaming_with_terminators_table" in caplog.text
    )
    assert "Vacuuming table: test_db.streaming_with_terminators_table" in caplog.text
    assert (
        "sql command: ANALYZE TABLE test_db.streaming_with_terminators_table "
        "COMPUTE STATISTICS" in caplog.text
    )


def _append_data_into_source(scenario: str) -> None:
    """Append data into jdbc sql lite table used as source for append load tests.

    Args:
        scenario: scenario being tested.
    """
    source_df = DataframeHelpers.read_from_file(f"{TEST_LAKEHOUSE_IN}/{scenario}/data")
    DataframeHelpers.write_into_jdbc_table(
        source_df, f"jdbc:sqlite:{TEST_LAKEHOUSE_IN}/{scenario}/tests.db", f"{scenario}"
    )
