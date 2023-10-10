"""Test Union Transformers."""
from typing import List

import pytest
from pyspark.sql.utils import AnalysisException

from lakehouse_engine.core.definitions import OutputFormat
from lakehouse_engine.engine import load_data
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_PATH = "transformations/unions"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenario",
    [
        ["batch", "union", "control_sales"],
        ["batch", "union_diff_schema", ""],
        ["batch", "unionByName", "control_sales"],
        ["batch", "unionByName_diff_schema", "control_sales_shipment"],
        ["batch", "unionByName_diff_schema_error", ""],
        ["streaming", "union", "control_sales_streaming"],
        ["streaming", "unionByName_diff_schema", "control_sales_shipment_streaming"],
        ["streaming", "union_foreachBatch", "control_sales_streaming_foreachBatch"],
        [
            "streaming",
            "unionByName_diff_schema_foreachBatch",
            "control_sales_shipment_streaming_foreachBatch",
        ],
    ],
)
def test_unions(scenario: List[str]) -> None:
    """Test union transformers.

    Args:
        scenario: scenario to test.
            batch_union - union batch scenario, using union function based on
            columns' position.
            batch_union_diff_schema - same as batch_union scenario but tries
            to union data with different schema, throwing an exception.
            batch_unionByName - union batch scenario, using unionByName
            function based on columns' names.
            batch_unionByName_diff_schema - same as batch_unionByName
            scenario but allows the union of datasets with different schemas
            enabling the allowMissingColumns param.
            batch_unionByName_diff_schema_error - same as
            batch_unionByName_diff_schema but disabling the allowMissingColumns
            param and therefore, throwing an exception.
            streaming_union - union streaming scenario, using union function
            based on columns' position.
            streaming_unionByName_diff_schema - union streaming scenario,
            using unionByName function based on columns' names and allowing the
            union of datasets with different schemas.
            streaming_union_foreachBatch - union streaming scenario, using union
            function based on columns' position in foreachBatch mode.
            streaming_unionByName_diff_schema_foreachBatch - union streaming scenario,
            using unionByName function based on columns' names and allowing the
            union of datasets with different schemas in foreachBatch mode.

    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/*schema.json",
        f"{TEST_LAKEHOUSE_IN}/",
    )

    copy_data_files(1)

    if "union_diff_schema" in scenario[1] or "error" in scenario[1]:
        with pytest.raises(
            AnalysisException,
            match=".*Union can only be performed on tables with the same number.*",
        ):
            load_data(f"file://{TEST_RESOURCES}/{scenario[0]}_{scenario[1]}.json")

    else:
        if scenario[0] != "batch":
            load_data(f"file://{TEST_RESOURCES}/{scenario[0]}_{scenario[1]}.json")

            copy_data_files(2)

        load_data(f"file://{TEST_RESOURCES}/{scenario[0]}_{scenario[1]}.json")

        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/data/control/*.csv",
            f"{TEST_LAKEHOUSE_CONTROL}/data/",
        )

        result_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_OUT}/{scenario[0]}_{scenario[1]}/data",
            file_format=OutputFormat.DELTAFILES.value,
        )
        control_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_CONTROL}/data/{scenario[2]}.csv"
        )

        assert not DataframeHelpers.has_diff(result_df, control_df)


def copy_data_files(iteration: int) -> None:
    """Copies the data files to the tests input location.

    Args:
        iteration: number indicating the file to load.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/sales-historical-part-0{iteration}.csv",
        f"{TEST_LAKEHOUSE_IN}/data/sales/sales_historical/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/sales-new-part-0{iteration}.csv",
        f"{TEST_LAKEHOUSE_IN}/data/sales/sales_new/",
    )

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/sales-shipment-part-0{iteration}.csv",
        f"{TEST_LAKEHOUSE_IN}/data/sales/sales_shipment/",
    )
