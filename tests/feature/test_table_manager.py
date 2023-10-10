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


def test_table_manager(caplog: Any) -> None:
    """Test functions from table manager.

    Args:
        caplog: captured log.
    """
    with caplog.at_level(logging.INFO):
        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/create/table/test_table.sql",
            f"{TEST_LAKEHOUSE_IN}/create/table/",
        )
        manage_table(f"file://{TEST_RESOURCES}/create/acon_create_table.json")
        assert "create_table successfully executed!" in caplog.text

        manage_table(f"file://{TEST_RESOURCES}/execute_sql/acon_execute_sql.json")
        assert "sql successfully executed!" in caplog.text

        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/create/view/test_view.sql",
            f"{TEST_LAKEHOUSE_IN}/create/view/",
        )
        manage_table(f"file://{TEST_RESOURCES}/create/acon_create_view.json")
        assert "create_view successfully executed!" in caplog.text

        manage_table(f"file://{TEST_RESOURCES}/describe/acon_describe.json")
        assert (
            "DataFrame[col_name: string, data_type: string, comment: string]"
            in caplog.text
        )

        manage_table(f"file://{TEST_RESOURCES}/vacuum/acon_vacuum_table.json")
        assert "Vacuuming table: test_db.DummyTableBronze" in caplog.text

        manage_table(f"file://{TEST_RESOURCES}/vacuum/acon_vacuum_location.json")
        assert (
            "Vacuuming location: file:///app/tests/lakehouse/out/"
            "feature/table_manager/dummy_table_bronze/data" in caplog.text
        )

        manage_table(f"file://{TEST_RESOURCES}/optimize/optimize_table.json")

        assert (
            "sql command: OPTIMIZE test_db.DummyTableBronze WHERE year >= 2021 and "
            "month >= 09 and day > 01 ZORDER BY (col1,col2)" in caplog.text
        )

        manage_table(f"file://{TEST_RESOURCES}/optimize/optimize_location.json")

        assert (
            f"sql command: OPTIMIZE delta.`file://{TEST_LAKEHOUSE_OUT}/"
            "dummy_table_bronze/data` WHERE year >= 2021 and "
            "month >= 09 and day > 01 ZORDER BY (col1,col2)" in caplog.text
        )

        with pytest.raises(
            AnalysisException, match=".*ANALYZE TABLE is not supported for v2 tables.*"
        ):
            # compute table stats is still not supported in current OS delta lake.
            manage_table(
                f"file://{TEST_RESOURCES}/compute_table_statistics/table_stats.json"
            )

        assert (
            "sql command: ANALYZE TABLE test_db.DummyTableBronze COMPUTE STATISTICS"
            in caplog.text
        )

        manage_table(
            f"file://{TEST_RESOURCES}/show_tbl_properties/show_tbl_properties.json"
        )
        assert "sql command: SHOW TBLPROPERTIES test_db.DummyTableBronze" in caplog.text

        manage_table(f"file://{TEST_RESOURCES}/get_tbl_pk/get_tbl_pk.json")
        assert "['id', 'col1']" in caplog.text

        manage_table(f"file://{TEST_RESOURCES}/drop/acon_drop_view.json")
        assert "View successfully dropped!" in caplog.text

        manage_table(f"file://{TEST_RESOURCES}/delete/acon_delete_where_table.json")
        assert (
            "sql command: DELETE FROM test_db.DummyTableBronze WHERE year=2021"
            in caplog.text
        )

        manage_table(f"file://{TEST_RESOURCES}/drop/acon_drop_table.json")
        assert "Table successfully dropped!" in caplog.text
