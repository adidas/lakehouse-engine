"""Tests for the DataLoader algorithm with custom transformations."""
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

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

TEST_PATH = "data_loader_custom_transformer"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


def multiply_by_100(df: DataFrame) -> DataFrame:
    """An example custom transformer that will be provided in the ACON.

    Args:
        df: DataFrame passed as input.

    Returns:
        DataFrame: the transformed DataFrame.
    """
    multiplied_df = df.withColumn("amount", col("amount") * 100)
    return multiplied_df


def get_test_acon() -> dict:
    """Creates a test ACON with the desired logic for the algorithm.

    Returns:
        dict: the ACON for the algorithm configuration.
    """
    return {
        "input_specs": [
            {
                "spec_id": "sales_source",
                "read_type": "streaming",
                "data_format": "csv",
                "options": {"header": True, "delimiter": "|"},
                "location": "file:///app/tests/lakehouse/in/feature/"
                "data_loader_custom_transformer/delta_load/data",
            }
        ],
        "transform_specs": [
            {
                "spec_id": "transformed_sales_source",
                "input_id": "sales_source",
                "transformers": [
                    {
                        "function": "custom_transformation",
                        "args": {"custom_transformer": multiply_by_100},
                    },
                    {
                        "function": "condense_record_mode_cdc",
                        "args": {
                            "business_key": ["salesorder", "item"],
                            "ranking_key_desc": [
                                "actrequest_timestamp",
                                "datapakid",
                                "partno",
                                "record",
                            ],
                            "record_mode_col": "recordmode",
                            "valid_record_modes": ["", "N", "R", "D", "X"],
                        },
                    },
                ],
            }
        ],
        "dq_specs": [
            {
                "spec_id": "checked_transformed_sales_source",
                "input_id": "transformed_sales_source",
                "dq_type": "validator",
                "store_backend": "file_system",
                "local_fs_root_dir": "/app/tests/lakehouse/out/feature/"
                "data_loader_custom_transformer/dq",
                "unexpected_rows_pk": ["salesorder", "item", "date", "customer"],
                "dq_functions": [
                    {
                        "function": "expect_column_values_to_not_be_null",
                        "args": {"column": "article"},
                    }
                ],
            },
        ],
        "output_specs": [
            {
                "spec_id": "sales_bronze",
                "input_id": "checked_transformed_sales_source",
                "write_type": "merge",
                "data_format": "delta",
                "location": "file:///app/tests/lakehouse/out/feature/"
                "data_loader_custom_transformer/delta_load/data",
                "options": {
                    "checkpointLocation": "file:///app/tests/lakehouse/out/feature/"
                    "data_loader_custom_transformer/delta_load/checkpoint"
                },
                "merge_opts": {
                    "merge_predicate": "current.salesorder = new.salesorder "
                    "and current.item = new.item "
                    "and current.date <=> new.date",
                    "update_predicate": "new.actrequest_timestamp > "
                    "current.actrequest_timestamp or ( "
                    "new.actrequest_timestamp = "
                    "current.actrequest_timestamp and "
                    "new.datapakid > current.datapakid) or ( "
                    "new.actrequest_timestamp = "
                    "current.actrequest_timestamp and "
                    "new.datapakid = current.datapakid and "
                    "new.partno > current.partno) or ( "
                    "new.actrequest_timestamp = "
                    "current.actrequest_timestamp and "
                    "new.datapakid = current.datapakid and "
                    "new.partno = current.partno and new.record "
                    ">= current.record)",
                    "delete_predicate": "new.recordmode in ('R','D','X')",
                    "insert_predicate": "new.recordmode is null or new.recordmode "
                    "not in ('R','D','X')",
                },
            }
        ],
        "exec_env": {"spark.sql.streaming.schemaInference": True},
    }


@pytest.mark.parametrize("scenario", ["delta_load"])
def test_delta_load(scenario: str) -> None:
    """Test full load with a custom transformation function.

    Args:
        scenario: scenario to test.
    """
    _create_table(
        f"{scenario}",
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/data",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    load_data(acon=get_test_acon())

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-03.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    load_data(acon=get_test_acon())

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-02.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    load_data(acon=get_test_acon())

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario}/data/source/part-04.csv",
        f"{TEST_LAKEHOUSE_IN}/{scenario}/data/",
    )
    load_data(acon=get_test_acon())

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


def _create_table(table_name: str, location: str) -> None:
    """Create test table.

    Args:
        table_name: name of the table.
        location: location of the table.
    """
    ExecEnv.SESSION.sql(
        f"""
        CREATE TABLE IF NOT EXISTS test_db.{table_name} (
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
