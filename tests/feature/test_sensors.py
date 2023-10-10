"""Module with integration tests for sensors feature."""
import json
import os
from datetime import datetime

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from lakehouse_engine.algorithms.exceptions import (
    NoNewDataException,
    SensorAlreadyExistsException,
)
from lakehouse_engine.core.definitions import SensorSpec, SensorStatus
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.core.sensor_manager import SensorControlTableManager
from lakehouse_engine.engine import (
    execute_sensor,
    generate_sensor_query,
    update_sensor_status,
)
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_NAME = "sensors"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_NAME}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_NAME}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_NAME}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_NAME}"

_TEST_SENSOR_DELTA_TABLE_BASE_SCHEMA = {
    "sensor_id": "string",
    "assets": "array<string>",
    "status": "string",
    "status_change_timestamp": "timestamp",
    "checkpoint_location": "string",
}

_TEST_SENSOR_DELTA_TABLE_SCHEMA = {
    **_TEST_SENSOR_DELTA_TABLE_BASE_SCHEMA,
    **{
        "upstream_key": "string",
        "upstream_value": "string",
    },
}


@pytest.mark.parametrize(
    "scenario",
    [
        "1st_run",
        "has_new_data",
        "has_data_from_previous_execution",
        "upstream_acquired_new_data_but_not_processed",
        "no_new_data",
    ],
)
def test_table_sensor(scenario: list) -> None:
    """Test the feature of using a sensor to read from a delta table.

    This specific test focuses on a delta table that is in itself the delta
    table where sensor information is stored. This is useful for data products
    consuming other data products sensor information to trigger their pipelines.

    Scenarios:
        1st_run: initial setup.
        has_new_data: the first time the sensor detects new data from the
            upstream.
        has_data_from_previous_execution: the sensor does not detect new data
            from the upstream, but it had data detected from a previous
            execution of the pipeline for which the completion of the processing
            of all the data was not acknowledged (e.g., the pipeline failed
            before completing all the tasks).
        upstream_acquired_new_data_but_not_processed: tests the scenario where
            the upstream sensor has acquired new data, but because it's still
            not in processed state, the downstream sensoring this table cannot
            consider there's new data available from the upstream (e.g.,
            a data product pipeline has identified new data from the source,
            but the pipeline failed, so the downstream data product pipeline's
            sensor cannot consider there's new data from the upstream).
        no_new_data: there's no new data from the upstream.
    """
    upstream_table = "test_table_sensor_upstream"
    sensor_id = "sensor_id_1"
    control_db_table_name = "test_db.test_table_sensor"
    checkpoint_location = f"{TEST_LAKEHOUSE_IN}/test_table_sensor/"

    if scenario == "1st_run":
        DataframeHelpers.create_delta_table(
            _TEST_SENSOR_DELTA_TABLE_SCHEMA,
            table="test_table_sensor",
        )
        DataframeHelpers.create_delta_table(
            _TEST_SENSOR_DELTA_TABLE_SCHEMA,
            table=upstream_table,
            enable_cdf=True,
        )

    if scenario == "has_new_data":
        _insert_data_into_upstream_table(upstream_table)
    elif scenario == "upstream_acquired_new_data_but_not_processed":
        _insert_data_into_upstream_table(
            upstream_table,
            values=(
                f"('sensor_id_upstream_1', array('dummy_upstream_asset_1'), "
                f"'{SensorStatus.ACQUIRED_NEW_DATA.value}', "
                f"'2023-05-30 23:29:49.079522', null, null, null)"
            ),
        )

    acon = {
        "sensor_id": sensor_id,
        "assets": ["dummy_asset_1"],
        "control_db_table_name": control_db_table_name,
        "input_spec": {
            "spec_id": "sensor_upstream",
            "read_type": "streaming",
            "data_format": "delta",
            "db_table": f"test_db.{upstream_table}",
            "options": {
                "readChangeFeed": "true",
            },
        },
        "preprocess_query": generate_sensor_query("sensor_id_upstream_1"),
        "base_checkpoint_location": checkpoint_location,
        "fail_on_empty_result": True,
    }

    if scenario in ["has_new_data", "has_data_from_previous_execution"]:
        has_new_data = execute_sensor(acon=acon)
        sensor_table_data = SensorControlTableManager.read_sensor_table_data(
            sensor_id=sensor_id, control_db_table_name=control_db_table_name
        )
        assert sensor_table_data.status == SensorStatus.ACQUIRED_NEW_DATA.value
        assert has_new_data

        if scenario == "has_data_from_previous_execution":
            # this is the final scenario where we should have data from upstream.
            # therefore, we checkpoint to indicate that sensor has processed
            # all the new data.
            update_sensor_status(
                sensor_id,
                control_db_table_name,
            )

            sensor_table_data = SensorControlTableManager.read_sensor_table_data(
                sensor_id=sensor_id, control_db_table_name=control_db_table_name
            )

            assert sensor_table_data.status == SensorStatus.PROCESSED_NEW_DATA.value
    else:
        with pytest.raises(NoNewDataException) as exception:
            execute_sensor(acon=acon)

        assert f"No data was acquired by {sensor_id} sensor." == str(exception.value)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "raise_exception_as_sensor_already_exists_by_sensor_id",
            "sensor_id": "sensor_id_2",
            "assets": ["dummy_asset_1"],
        },
        {
            "scenario_name": "raise_exception_as_sensor_already_exists_by_assets",
            "sensor_id": "sensor_id_1",
            "assets": ["dummy_asset_2"],
        },
    ],
)
def test_if_sensor_already_exists(scenario: dict) -> None:
    """Test if the sensor already exists.

    This specific test focuses on the ways to identify if a sensor
    already exists.

    Scenarios:
        raise_exception_as_sensor_already_exists_by_sensor_id: raises
            exception if you try to create a sensor with a
            different sensor id but same asset.
        raise_exception_as_sensor_already_exists_by_assets: raises
            exception if you try to create a sensor with
            different assets but same sensor_id.
    """
    sensor_id = "sensor_id_1"
    assets = ["dummy_asset_1"]

    control_db_table_name = "test_db.test_table_sensor"
    upstream_table = "test_table_sensor_upstream"
    checkpoint_location = f"{TEST_LAKEHOUSE_IN}/test_table_sensor/"

    LocalStorage.clean_folder(checkpoint_location)
    ExecEnv.SESSION.sql(f"DROP TABLE IF EXISTS {control_db_table_name}")
    ExecEnv.SESSION.sql(f"DROP TABLE IF EXISTS test_db.{upstream_table}")

    DataframeHelpers.create_delta_table(
        _TEST_SENSOR_DELTA_TABLE_SCHEMA,
        table="test_table_sensor",
    )
    DataframeHelpers.create_delta_table(
        _TEST_SENSOR_DELTA_TABLE_SCHEMA,
        table=upstream_table,
        enable_cdf=True,
    )

    _insert_data_into_upstream_table(upstream_table)

    acon = {
        "sensor_id": sensor_id,
        "assets": assets,
        "control_db_table_name": control_db_table_name,
        "input_spec": {
            "spec_id": "sensor_upstream",
            "read_type": "streaming",
            "data_format": "delta",
            "db_table": f"test_db.{upstream_table}",
            "options": {
                "readChangeFeed": "true",
            },
        },
        "preprocess_query": generate_sensor_query("sensor_id_upstream_1"),
        "base_checkpoint_location": checkpoint_location,
        "fail_on_empty_result": True,
    }

    execute_sensor(acon=acon)

    with pytest.raises(SensorAlreadyExistsException) as exception:
        acon["sensor_id"] = scenario["sensor_id"]
        acon["assets"] = scenario["assets"]
        execute_sensor(acon=acon)

    assert "There's already a sensor registered with same id or assets!" == str(
        exception.value
    )


@pytest.mark.parametrize(
    "scenario",
    [
        "1st_run",
        "2nd_run_with_new_data",
        "3rd_run_without_new_data",
        "4th_run_with_new_data",
    ],
)
def test_jdbc_sensor(scenario: str) -> None:
    """Test the feature of sensoring new data from a jdbc upstream.

    Scenario:
        1st_run - initial setup.
        2nd_run_with_new_data - jdbc upstream has new data.
        3rd_run_without_new_data - jdbc upstream does not have new data.
        4th_run_with_new_data - jdbc upstream has new data again.
    """
    upstream_jdbc_table = "test_jdbc_sensor_upstream"
    sensor_id = "sensor_id_1"
    sensor_table = "test_jdbc_sensor"
    control_db_table_name = f"test_db.{sensor_table}"
    os.makedirs(f"{TEST_LAKEHOUSE_IN}/{upstream_jdbc_table}", exist_ok=True)

    if scenario == "1st_run":
        DataframeHelpers.create_delta_table(
            _TEST_SENSOR_DELTA_TABLE_SCHEMA,
            table=sensor_table,
        )
        _insert_into_jdbc_table(init=True)
    elif scenario == "2nd_run_with_new_data":
        _insert_into_jdbc_table(time=datetime.now())
    elif scenario == "4th_run_with_new_data":
        _insert_into_jdbc_table(time=datetime.now())

    acon = {
        "sensor_id": sensor_id,
        "assets": ["dummy_asset_1"],
        "control_db_table_name": control_db_table_name,
        "input_spec": {
            "spec_id": "sensor_upstream",
            "read_type": "batch",
            "data_format": "jdbc",
            "jdbc_args": {
                "url": f"jdbc:sqlite:{TEST_LAKEHOUSE_IN}/"
                f"{upstream_jdbc_table}/tests.db",
                "table": upstream_jdbc_table,
                "properties": {"driver": "org.sqlite.JDBC"},
            },
        },
        "preprocess_query": generate_sensor_query(
            sensor_id=sensor_id,
            filter_exp="?upstream_key > '?upstream_value'",
            control_db_table_name=control_db_table_name,
            upstream_key="dummy_time",
        ),
        "fail_on_empty_result": True,
    }

    if scenario in ["2nd_run_with_new_data", "4th_run_with_new_data"]:
        has_new_data = execute_sensor(acon=acon)
        sensor_table_data = SensorControlTableManager.read_sensor_table_data(
            sensor_id=sensor_id, control_db_table_name=control_db_table_name
        )

        assert sensor_table_data.status == SensorStatus.ACQUIRED_NEW_DATA.value

        update_sensor_status(
            sensor_id,
            control_db_table_name,
        )

        sensor_table_data = SensorControlTableManager.read_sensor_table_data(
            sensor_id=sensor_id, control_db_table_name=control_db_table_name
        )

        assert sensor_table_data.status == SensorStatus.PROCESSED_NEW_DATA.value
        assert has_new_data
    else:
        with pytest.raises(NoNewDataException) as exception:
            execute_sensor(acon=acon)

        assert f"No data was acquired by {sensor_id} sensor." == str(exception.value)


def test_files_sensor() -> None:
    """Test the feature of sensoring a filesystem location (e.g., s3)."""
    sensor_id = "sensor_id_1"
    sensor_table = "test_files_sensor"
    control_db_table_name = f"test_db.{sensor_table}"
    checkpoint_location = f"{TEST_LAKEHOUSE_IN}/test_files_sensor/"
    files_location = f"{TEST_LAKEHOUSE_IN}/test_files_sensor/files/"

    DataframeHelpers.create_delta_table(
        _TEST_SENSOR_DELTA_TABLE_SCHEMA,
        table=sensor_table,
    )

    schema = _insert_files_sensor_test_data(files_location)

    acon = {
        "sensor_id": sensor_id,
        "assets": ["dummy_asset_1"],
        "control_db_table_name": control_db_table_name,
        "input_spec": {
            "spec_id": "sensor_upstream",
            "read_type": "streaming",
            "data_format": "csv",
            "location": files_location,
            "schema": json.loads(schema.json()),
        },
        "base_checkpoint_location": checkpoint_location,
        "fail_on_empty_result": False,
    }

    has_new_data = execute_sensor(acon=acon)

    assert has_new_data


def test_update_sensor_status() -> None:
    """Test sensor update status logic."""
    sensor_id = "sensor_id_1"
    sensor_table = "test_checkpoint_sensor"
    control_db_table_name = f"test_db.{sensor_table}"
    status = SensorStatus.ACQUIRED_NEW_DATA.value
    checkpoint_location = "s3://dummy-bucket/sensors/sensor_id_1"

    DataframeHelpers.create_delta_table(
        _TEST_SENSOR_DELTA_TABLE_BASE_SCHEMA,
        table="test_checkpoint_sensor",
    )

    SensorControlTableManager.update_sensor_status(
        sensor_spec=SensorSpec(
            sensor_id=sensor_id,
            assets=["asset_1"],
            control_db_table_name=control_db_table_name,
            checkpoint_location=checkpoint_location,
            preprocess_query=None,
            input_spec=None,
        ),
        status=status,
    )

    row = SensorControlTableManager.read_sensor_table_data(
        sensor_id=sensor_id, control_db_table_name=control_db_table_name
    )

    assert (
        row.sensor_id == sensor_id
        and row.status == SensorStatus.ACQUIRED_NEW_DATA.value
        and row.checkpoint_location == "s3://dummy-bucket/sensors/sensor_id_1"
    )


def _insert_data_into_upstream_table(
    table: str,
    db: str = "test_db",
    values: str = None,
) -> None:
    """Insert data into upstream table for testing sensoring based on tables.

    Args:
        table: table name.
        db: database name.
        values: string with the values operator for inserting data through SQL
            DML statement.
    """
    if not values:
        values = (
            f"('sensor_id_upstream_1', array('dummy_upstream_asset_1'), "
            f"'{SensorStatus.PROCESSED_NEW_DATA.value}', "
            f"'2023-05-30 23:28:49.079522', null, null, null),"
            f"('sensor_id_upstream_2', array('dummy_upstream_asset_2'), "
            f"'{SensorStatus.PROCESSED_NEW_DATA.value}', "
            f"'2023-05-30 23:28:49.089522', null, null, null)"
        )

    ExecEnv.SESSION.sql(f"INSERT INTO {db}.{table} VALUES {values}")  # nosec: B608


def _insert_files_sensor_test_data(files_location: str) -> StructType:
    """Insert test data for files sensor test.

    Args:
        files_location: location to insert the data.

    Returns:
        A dummy struct type.
    """
    schema = StructType([StructField("dummy_field", StringType(), True)])

    df = ExecEnv.SESSION.createDataFrame(
        [
            ["a"],
            ["b"],
        ],
        schema,
    )

    df.write.format("csv").save(files_location)

    return schema


def _insert_into_jdbc_table(
    init: bool = False,
    time: datetime = None,
) -> None:
    """Insert data into the jdbc table for tests.

    Args:
        init: if to init the table or not with empty data.
        time: value to use for the dummy_time field, so that time-based filters
            can be applied to the table so that we know that new data is
            available from upstream.
    """
    schema = StructType(
        [
            StructField("dummy_field", StringType(), True),
            StructField("dummy_time", StringType(), True),
        ]
    )

    if init:
        df = ExecEnv.SESSION.createDataFrame(
            [],
            schema,
        )
    else:
        df = ExecEnv.SESSION.createDataFrame(
            [
                ["a", str(time)],
                ["b", str(time)],
            ],
            schema,
        )

    DataframeHelpers.write_into_jdbc_table(
        df,
        f"jdbc:sqlite:{TEST_LAKEHOUSE_IN}/test_jdbc_sensor_upstream/tests.db",
        "test_jdbc_sensor_upstream",
    )
