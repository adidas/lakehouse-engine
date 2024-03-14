"""Test table manager."""
import logging
from typing import Any

import pytest
from pyspark.sql.utils import AnalysisException

from lakehouse_engine.engine import manage_table
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.local_storage import LocalStorage

TEST_PATH = "table_manager"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    "scenarios",
    [
        {
            "table_and_view_name": ["SimpleSplitScenario"],
            "locations_name": ["simple_split_scenario"],
            "create_tbl_sql": "test_table_simple_split_scenario.sql",
            "create_tbl_json": "acon_create_table_simple_split_scenario",
            "execute_sql_json": "acon_execute_sql_simple_split_scenario",
            "create_vw_sql": "test_view_simple_split_scenario",
            "create_vw_json": "acon_create_view_simple_split_scenario",
            "describe_tbl_json": "acon_describe_simple_split_scenario",
            "vacuum_tbl_json": "acon_vacuum_table_simple_split_scenario",
            "vacuum_loc_json": "acon_vacuum_location_simple_split_scenario",
            "optimize_tbl_json_": "optimize_table_simple_split_scenario",
            "optimize_loc_json": "optimize_location_simple_split_scenario",
            "compute_statistics_tbl_json": ["table_stats_simple_split_scenario"],
            "show_tbl_prop_json": "show_tbl_properties_simple_split_scenario",
            "tbl_primary_keys_json": "get_tbl_pk_simple_split_scenario",
            "drop_vw_json": "acon_drop_view_simple_split_scenario",
            "delete_json": "acon_delete_where_table_simple_split_scenario",
            "drop_tbl_json": "acon_drop_table_simple_split_scenario",
        },
        {
            "table_and_view_name": [
                "ComplexDefaultScenario1",
                "ComplexDefaultScenario2",
            ],
            "locations_name": [
                "complex_default_scenario1",
                "complex_default_scenario2",
            ],
            "create_tbl_sql": "test_table_complex_default_scenario.sql",
            "create_tbl_json": "acon_create_table_complex_default_scenario",
            "execute_sql_json": "acon_execute_sql_complex_default_scenario",
            "create_vw_sql": "test_view_complex_default_scenario",
            "create_vw_json": "acon_create_view_complex_default_scenario",
            "compute_statistics_tbl_json": [
                "table_stats_complex_default_scenario1",
                "table_stats_complex_default_scenario2",
            ],
        },
        {
            "table_and_view_name": [
                "ComplexDifferentDelimiterScenario1",
                "ComplexDifferentDelimiterScenario2",
            ],
            "locations_name": [
                "complex_different_delimiter_scenario1",
                "complex_different_delimiter_scenario2",
            ],
            "create_tbl_sql": "test_table_complex_different_delimiter_scenario.sql",
            "create_tbl_json": "acon_create_table_complex_different_delimiter_scenario",
            "execute_sql_json": "acon_execute_sql_complex_different_delimiter_scenario",
            "create_vw_sql": "test_view_complex_different_delimiter_scenario",
            "create_vw_json": "acon_create_view_complex_different_delimiter_scenario",
            "compute_statistics_tbl_json": [
                "table_stats_complex_different_delimiter_scenario1",
                "table_stats_complex_different_delimiter_scenario2",
            ],
        },
    ],
)
def test_table_manager(scenarios: dict, caplog: Any) -> None:
    """Test functions from table manager.

    Args:
        scenarios: scenarios to test.
        caplog: captured log.
    """
    with caplog.at_level(logging.INFO):
        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/create/table/{scenarios['create_tbl_sql']}",
            f"{TEST_LAKEHOUSE_IN}/create/table/",
        )

        manage_table(
            f"file://{TEST_RESOURCES}/create/{scenarios['create_tbl_json']}.json"
        )
        assert "create_table successfully executed!" in caplog.text

        manage_table(
            f"file://{TEST_RESOURCES}/execute_sql/{scenarios['execute_sql_json']}.json"
        )
        assert "sql successfully executed!" in caplog.text

        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/create/view/{scenarios['create_vw_sql']}.sql",
            f"{TEST_LAKEHOUSE_IN}/create/view/",
        )

        manage_table(
            f"file://{TEST_RESOURCES}/create/{scenarios['create_vw_json']}.json"
        )
        assert "create_view successfully executed!" in caplog.text

        if scenarios.get("describe_tbl_json") is not None:
            manage_table(
                f"file://{TEST_RESOURCES}/describe/"
                f"{scenarios['describe_tbl_json']}.json"
            )
            assert (
                "DataFrame[col_name: string, data_type: string, comment: string]"
                in caplog.text
            )

        if scenarios.get("vacuum_tbl_json") is not None:
            manage_table(
                f"file://{TEST_RESOURCES}/vacuum/{scenarios['vacuum_tbl_json']}.json"
            )
            assert (
                "Vacuuming table: test_db.DummyTableBronzeSimpleSplitScenario"
                in caplog.text
            )

        if scenarios.get("vacuum_loc_json") is not None:
            manage_table(
                f"file://{TEST_RESOURCES}/vacuum/{scenarios['vacuum_loc_json']}.json"
            )
            assert (
                "Vacuuming location: file:///app/tests/lakehouse/out/feature/"
                "table_manager/dummy_table_bronze/data_simple_split_scenario"
                in caplog.text
            )

        if scenarios.get("optimize_tbl_json") is not None:
            manage_table(
                f"file://{TEST_RESOURCES}/optimize/"
                f"{scenarios['optimize_tbl_json']}.json"
            )
            assert (
                "sql command: OPTIMIZE test_db.DummyTableBronzeSimpleSplitScenario "
                "WHERE year >= 2021 and month >= 09 and day > 01 ZORDER BY (col1,col2)"
                in caplog.text
            )

        if scenarios.get("optimize_loc_json") is not None:
            manage_table(
                f"file://{TEST_RESOURCES}/optimize/"
                f"{scenarios['optimize_loc_json']}.json"
            )
            assert (
                f"sql command: OPTIMIZE delta.`file://{TEST_LAKEHOUSE_OUT}/"
                "dummy_table_bronze/data_simple_split_scenario` WHERE year >= 2021 "
                "and month >= 09 and day > 01 ZORDER BY (col1,col2)" in caplog.text
            )

        with pytest.raises(
            AnalysisException, match=".*ANALYZE TABLE is not supported for v2 tables.*"
        ):
            # compute table stats is still not supported in current OS delta lake.
            if scenarios.get("compute_statistics_tbl_json") is not None:
                for (
                    compute_statistics_table_index,
                    compute_statistics_table_json_file,
                ) in enumerate(scenarios["compute_statistics_tbl_json"]):
                    manage_table(
                        f"file://{TEST_RESOURCES}/compute_table_statistics/"
                        f"{compute_statistics_table_json_file}.json"
                    )
                    scenario_name = scenarios["table_and_view_name"][
                        compute_statistics_table_index
                    ]
                    assert (
                        "sql command: ANALYZE TABLE test_db.DummyTable"
                        f"Bronze{scenario_name} COMPUTE STATISTICS" in caplog.text
                    )

        if scenarios.get("show_tbl_prop_json") is not None:
            manage_table(
                f"file://{TEST_RESOURCES}/show_tbl_properties/"
                f"{scenarios['show_tbl_prop_json']}.json"
            )
            assert (
                "sql command: SHOW TBLPROPERTIES test_db.DummyTable"
                "BronzeSimpleSplitScenario" in caplog.text
            )

        if scenarios.get("tbl_primary_keys_json") is not None:
            manage_table(
                f"file://{TEST_RESOURCES}/get_tbl_pk/"
                f"{scenarios['tbl_primary_keys_json']}.json"
            )
            assert "['id', 'col1']" in caplog.text

        if scenarios.get("drop_vw_json") is not None:
            manage_table(
                f"file://{TEST_RESOURCES}/drop/{scenarios['drop_vw_json']}.json"
            )
            assert "View successfully dropped!" in caplog.text

        if scenarios.get("delete_json") is not None:
            manage_table(
                f"file://{TEST_RESOURCES}/delete/{scenarios['delete_json']}.json"
            )
            assert (
                "sql command: DELETE FROM test_db.DummyTable"
                "BronzeSimpleSplitScenario WHERE year=2021"
                in caplog.text  # nosec: B608
            )

        if scenarios.get("drop_tbl_json") is not None:
            manage_table(
                f"file://{TEST_RESOURCES}/drop/{scenarios['drop_tbl_json']}.json"
            )
            assert "Table successfully dropped!" in caplog.text
