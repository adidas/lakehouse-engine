"""Test materialize cdf to external location."""
from typing import Any

import pytest
from delta.tables import DeltaTable

from lakehouse_engine.core.definitions import InputFormat
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.engine import load_data, manage_table
from lakehouse_engine.utils.schema_utils import SchemaUtils
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_PATH = "materialize_cdf"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize("scenario", ["streaming_with_cdf"])
def test_streaming_with_cdf(scenario: str, caplog: Any) -> None:
    """Test materialize cdf function.

    Args:
        scenario: scenario name.
        caplog: captured log.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/table/streaming_with_cdf.sql",
        f"{TEST_LAKEHOUSE_IN}/data/table/",
    )
    manage_table(f"file://{TEST_RESOURCES}/acon_create_table.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/streaming_without_clean_cdf.json")

    assert "Writing CDF to external table..." in caplog.text

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/control/part-01_cdf.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/control_schema.json",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/control_schema.json",
    )

    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_CONTROL}/{scenario}/control_schema.json"
        ),
    )

    result_df_delta = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/cdf_data",
        file_format=InputFormat.DELTAFILES.value,
    ).drop("_commit_timestamp")

    # once we are writing the cdf as delta, it can also be read as parquet.
    # because the _commit_timestamp field is a partition field (comes from the folder),
    # not from the parquet file, we need to enforce a schema where _commit_timestamp is
    # a string, not an int (as automatically inferred from the folder by spark).
    result_df_parquet = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/cdf_data",
        file_format=InputFormat.PARQUET.value,
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_CONTROL}/{scenario}/control_schema.json"
        ),
    ).drop("_commit_timestamp")

    assert not DataframeHelpers.has_diff(result_df_delta, control_df)
    assert not DataframeHelpers.has_diff(result_df_parquet, control_df)

    # to be able to execute vacuum on expose cdf terminator spec it is
    # necessary to update _commit_timestamp to an old value, for that we
    # are enforcing the timestamp with the following delta commands.
    delta_table = DeltaTable.forPath(
        ExecEnv.SESSION,
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/cdf_data",
    )
    delta_table.update(set={"_commit_timestamp": "'20211105132711'"})

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/part-02.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    load_data(f"file://{TEST_RESOURCES}/streaming_with_clean_and_vacuum.json")

    assert "Writing CDF to external table..." in caplog.text
    assert "Cleaning CDF table..." in caplog.text
    assert "Vacuuming CDF table..." in caplog.text

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/cdf_data",
        file_format=InputFormat.DELTAFILES.value,
    )

    assert result_df.count() == 6
