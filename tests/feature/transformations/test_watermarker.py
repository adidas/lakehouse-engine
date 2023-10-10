"""Test Watermarker Transformers."""
import pytest

from lakehouse_engine.core.definitions import OutputFormat
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

TEST_PATH = "transformations/watermarker"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    [
        {"scenario_name": "streaming_drop_duplicates", "loads": 2},
        {"scenario_name": "streaming_drop_duplicates_overall_watermark", "loads": 2},
    ],
)
def test_drop_duplicates_with_watermark(scenario: dict) -> None:
    """Test deduplication applying watermarking.

    For both test scenarios if there is late data coming out of the
    watermark time, this data won't be integrated. It won't be in the
    target destination (and so it is also not in the control data).

    Args:
        scenario: scenario to test.
            streaming_drop_duplicates - apply drop duplicates over a streaming
             dataframe.
            streaming_drop_duplicates_overall_watermark - apply drop duplicates over
             a streaming dataframe defined as an independent transformation.
             It also uses the Group and rank transformation which ignores the watermark
             because that transformation is applied over a foreach batch operation.
    """
    scenario_name = scenario["scenario_name"]
    loads = scenario["loads"]
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario_name}/data/control/*",
        f"{TEST_LAKEHOUSE_CONTROL}/data/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario_name}/*schema.json",
        f"{TEST_LAKEHOUSE_IN}/{scenario_name}/",
    )

    for load in range(1, loads + 1):
        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/{scenario_name}/data/source/part-0{str(load)}.csv",
            f"{TEST_LAKEHOUSE_IN}/{scenario_name}/data/",
        )
        load_data(f"file://{TEST_RESOURCES}/{scenario_name}/{scenario_name}.json")

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario_name}/data",
        file_format=OutputFormat.DELTAFILES.value,
    )

    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data/{scenario_name}.csv",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_IN}/{scenario_name}/source_schema.json"
        ),
    )
    assert not DataframeHelpers.has_diff(result_df, control_df)


@pytest.mark.parametrize(
    "scenario",
    [
        {"scenario_name": "streaming_inner_join", "loads": 2},
        {"scenario_name": "streaming_right_outer_join", "loads": 2},
        {"scenario_name": "streaming_left_outer_join", "loads": 5},
    ],
)
def test_joins_with_watermark(scenario: dict) -> None:
    """Test join operations applying watermarking.

    Args:
        scenario: scenario to test.
            streaming_inner_join - apply inner join over 2 streaming dataframes.
            streaming_right_outer_join - apply right outer join over 2 streaming
             dataframes.
            streaming_left_outer_join - apply left outer join over 2 streaming
             dataframes.
    """
    scenario_name = scenario["scenario_name"]
    loads = scenario["loads"]
    if scenario_name == "streaming_right_outer_join":
        _drop_and_create_table(
            "streaming_outer_join", f"{TEST_LAKEHOUSE_OUT}/{scenario_name}/data"
        )

    for load in range(1, loads + 1):
        file_prefix = f"part-0{str(load)}.csv"
        if load >= 1 and not scenario_name == "streaming_inner_join":
            LocalStorage.copy_file(
                f"{TEST_RESOURCES}/{scenario_name}/data/source/customer-{file_prefix}",
                f"{TEST_LAKEHOUSE_IN}/{scenario_name}/data/customers/",
            )
        elif load == 1 and scenario_name == "streaming_inner_join":
            LocalStorage.copy_file(
                f"{TEST_RESOURCES}/{scenario_name}/data/source/customer-part-01.csv",
                f"{TEST_LAKEHOUSE_IN}/{scenario_name}/data/customers/",
            )

        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/{scenario_name}/data/source/sales-{file_prefix}",
            f"{TEST_LAKEHOUSE_IN}/{scenario_name}/data/sales/",
        )
        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/{scenario_name}/*schema.json",
            f"{TEST_LAKEHOUSE_IN}/{scenario_name}/",
        )

        load_data(f"file://{TEST_RESOURCES}/{scenario_name}/{scenario_name}.json")

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario_name}/data",
        file_format=OutputFormat.DELTAFILES.value,
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario_name}/data/control/*",
        f"{TEST_LAKEHOUSE_CONTROL}/data/",
    )

    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data/{scenario_name}.csv",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_IN}/{scenario_name}/"
            f"{scenario_name}_control_schema.json"
        ),
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)


def _drop_and_create_table(table_name: str, location: str) -> None:
    """Create test table.

    Args:
        table_name: name of the table.
        location: location of the table.
    """
    ExecEnv.SESSION.sql(f"DROP TABLE IF EXISTS test_db.{table_name}")
    ExecEnv.SESSION.sql(
        f"""
        CREATE TABLE IF NOT EXISTS test_db.{table_name} (
            salesorder int,
            item int,
            date timestamp,
            customer string,
            article string,
            amount int,
            customer_name string
        )
        USING delta
        LOCATION '{location}'
        """
    )
