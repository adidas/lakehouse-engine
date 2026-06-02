"""Test Optimizer transformers."""

from collections.abc import Callable

import pytest
from pyspark.sql.dataframe import DataFrame

from lakehouse_engine.engine import load_data
from tests.conftest import FEATURE_RESOURCES, LAKEHOUSE_FEATURE_IN
from tests.utils.local_storage import LocalStorage

TEST_PATH = "transformations/optimizers"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    [
        "batch",
        "streaming",
    ],
)
def test_optimizer(scenario: str) -> None:
    """Test the optimizer transformer both in batch and streaming."""

    def is_df_cached(df: DataFrame) -> DataFrame:
        """Check if the dataframe is cached.

        Args:
            df: DataFrame passed as input.

        Returns:
            DataFrame: same as the input DataFrame.
        """
        if not df.is_cached:
            raise Exception

        return df

    def is_df_not_cached(df: DataFrame) -> DataFrame:
        """Check if the dataframe is not cached.

        Args:
            df: DataFrame passed as input.

        Returns:
            DataFrame: same as the input DataFrame.
        """
        if df.is_cached:
            raise Exception

        return df

    acon = _get_test_acon(scenario, is_df_cached, is_df_not_cached)

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/data/",
    )

    load_data(acon=acon)


def _get_test_acon(
    read_type: str, is_df_cached_func: Callable, is_df_not_cached_func: Callable
) -> dict:
    """Creates a test ACON with the desired logic for the algorithm.

    The functions are passed as arguments so they are available in the context
    created when using Spark Connect tests.

    Args:
        read_type: the read type (streaming or batch).
        is_df_cached_func: function to check if dataframe is cached.
        is_df_not_cached_func: function to check if dataframe is not cached.

    Returns:
        dict: the ACON for the algorithm configuration.
    """
    acon = {
        "input_specs": [
            {
                "spec_id": "sales_source",
                "read_type": read_type,
                "data_format": "csv",
                "options": {"header": True, "delimiter": "|", "inferSchema": True},
                "location": f"file:///{TEST_LAKEHOUSE_IN}/data/",
            }
        ],
        "transform_specs": [
            {
                "spec_id": "transformed_sales_source",
                "input_id": "sales_source",
                "transformers": [
                    {
                        "function": "persist",
                        "args": {"storage_level": "MEMORY_AND_DISK"},
                    },
                    {
                        "function": "custom_transformation",
                        "args": {"custom_transformer": is_df_cached_func},
                    },
                    {
                        "function": "unpersist",
                    },
                    {
                        "function": "custom_transformation",
                        "args": {"custom_transformer": is_df_not_cached_func},
                    },
                    {
                        "function": "cache",
                    },
                    {
                        "function": "custom_transformation",
                        "args": {"custom_transformer": is_df_cached_func},
                    },
                ],
            }
        ],
        "output_specs": [
            {
                "spec_id": "sales_bronze",
                "input_id": "transformed_sales_source",
                "data_format": "console",
            }
        ],
    }

    if read_type == "streaming":
        acon["transform_specs"][0][  # type: ignore
            "force_streaming_foreach_batch_processing"
        ] = True
        acon["exec_env"] = {"spark.sql.streaming.schemaInference": True}

    return acon
