"""Tests for the DataLoader algorithm with custom transformations."""
import pytest
from pyspark.sql import DataFrame

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

TEST_PATH = "data_loader_custom_transformer"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


def yet_another_kpi_calculator(df: DataFrame) -> DataFrame:
    """An example custom transformer that will be provided in the ACON.

    Args:
        df: DataFrame passed as input.

    Returns:
        DataFrame: the transformed DataFrame.
    """
    session = ExecEnv.SESSION
    df.createOrReplaceTempView("sales")
    kpi_df = session.sql(
        """
            SELECT date, SUM(amount) AS amount
            FROM sales
            GROUP BY date
        """
    )
    return kpi_df


def get_test_acon() -> dict:
    """Creates a test ACON with the desired logic for the algorithm.

    Returns:
        dict: the ACON for the algorithm configuration.
    """
    return {
        "input_specs": [
            {
                "spec_id": "sales_source",
                "read_type": "batch",
                "data_format": "csv",
                "options": {"mode": "FAILFAST", "header": True, "delimiter": "|"},
                "schema_path": "file:///app/tests/lakehouse/in/feature/"
                "data_loader_custom_transformer/calculate_kpi/"
                "source_schema.json",
                "location": "file:///app/tests/lakehouse/in/feature/"
                "data_loader_custom_transformer/calculate_kpi/data",
            }
        ],
        "transform_specs": [
            {
                "spec_id": "calculated_kpi",
                "input_id": "sales_source",
                "transformers": [
                    {
                        "function": "custom_transformation",
                        "args": {"custom_transformer": yet_another_kpi_calculator},
                    }
                ],
            }
        ],
        "output_specs": [
            {
                "spec_id": "sales_bronze",
                "input_id": "calculated_kpi",
                "write_type": "overwrite",
                "data_format": "delta",
                "location": "file:///app/tests/lakehouse/out/feature/"
                "data_loader_custom_transformer/calculate_kpi/data",
            }
        ],
    }


@pytest.mark.parametrize("scenario", ["calculate_kpi"])
def test_calculate_kpi_and_merge(scenario: str) -> None:
    """Test full load with a custom transformation function.

    Args:
        scenario: scenario to test.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/*_schema.json",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/*.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )

    load_data(acon=get_test_acon())

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/control/*.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data/",
    )

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/data",
        file_format=InputFormat.DELTAFILES.value,
    )
    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario}/data",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_IN}/{scenario}/control_schema.json"
        ),
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)
