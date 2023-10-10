"""Module with unit tests for Sensor class."""
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import Row, StructType

from lakehouse_engine.algorithms.exceptions import (
    NoNewDataException,
    SensorAlreadyExistsException,
)
from lakehouse_engine.algorithms.sensor import Sensor
from lakehouse_engine.core.definitions import (
    InputFormat,
    InputSpec,
    ReadType,
    SensorSpec,
    SensorStatus,
)
from lakehouse_engine.core.sensor_manager import (
    SensorControlTableManager,
    SensorUpstreamManager,
)
from tests.utils.dataframe_helpers import DataframeHelpers


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "create_sensor",
            "sensor_data": {
                "sensor_id": "sensor_id_1",
                "assets": ["asset_1"],
                "control_db_table_name": "control_sensor_table_name",
                "input_spec": {
                    "spec_id": "input_spec",
                    "read_type": ReadType.STREAMING.value,
                    "data_format": InputFormat.CSV.value,
                },
                "fail_on_empty_result": False,
                "base_checkpoint_location": "s3://dummy-bucket",
            },
            "sensor_already_exists": False,
            "expected_result": SensorSpec(
                sensor_id="sensor_id_1",
                assets=["asset_1"],
                control_db_table_name="control_sensor_table_name",
                input_spec=InputSpec(
                    spec_id="input_spec",
                    read_type=ReadType.STREAMING.value,
                    data_format=InputFormat.CSV.value,
                ),
                preprocess_query=None,
                checkpoint_location="s3://dummy-bucket"
                "/lakehouse_engine/sensors/sensor_id_1",
                fail_on_empty_result=False,
            ),
        },
        {
            "scenario_name": "raise_exception_sensor_already_exists",
            "sensor_data": {
                "sensor_id": "sensor_id_1",
                "assets": ["asset_1"],
                "control_db_table_name": "control_sensor_table_name",
                "input_spec": {
                    "spec_id": "input_spec",
                    "read_type": ReadType.STREAMING.value,
                    "data_format": InputFormat.CSV.value,
                },
                "fail_on_empty_result": False,
                "base_checkpoint_location": "s3://dummy-bucket",
            },
            "sensor_already_exists": True,
            "expected_result": "There's already a sensor registered "
            "with same id or assets!",
        },
    ],
)
def test_create_sensor(scenario: dict, capsys: Any) -> None:
    """Test Sensor creation.

    We will raise an exception if we try to create a Sensor
    that already exists, otherwise we will create successfully.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """
    with patch.object(
        Sensor,
        "_check_if_sensor_already_exists",
        new=MagicMock(return_value=scenario["sensor_already_exists"]),
    ) as sensor_already_exists_mock:
        sensor_already_exists_mock.start()
        if scenario["scenario_name"] == "raise_exception_sensor_already_exists":
            with pytest.raises(SensorAlreadyExistsException) as exception:
                Sensor(scenario["sensor_data"])

            assert scenario["expected_result"] == str(exception.value)
        else:
            subject = Sensor(scenario["sensor_data"])

            assert subject.spec == scenario["expected_result"]
        sensor_already_exists_mock.stop()


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "create_non_existing_sensor_with_sensor_id",
            "sensor_data": {
                "sensor_id": "sensor_id_1",
                "assets": None,
                "control_db_table_name": "control_sensor_table_name",
                "input_spec": {
                    "spec_id": "input_spec",
                    "read_type": ReadType.STREAMING.value,
                    "data_format": InputFormat.CSV.value,
                },
                "fail_on_empty_result": False,
                "base_checkpoint_location": "s3://dummy-bucket",
            },
            "control_db_sensor_data": Row(
                sensor_id="sensor_id_1",
                assets=None,
                status=SensorStatus.ACQUIRED_NEW_DATA.value,
                status_change_timestamp=datetime(2023, 5, 26, 14, 38, 16, 676508),
                checkpoint_location="s3://dummy-bucket/sensors/sensor_id_1",
            ),
            "expected_result": False,
        },
        {
            "scenario_name": "create_non_existing_sensor_with_assets",
            "sensor_data": {
                "sensor_id": None,
                "assets": ["asset_1"],
                "control_db_table_name": "control_sensor_table_name",
                "input_spec": {
                    "spec_id": "input_spec",
                    "read_type": ReadType.STREAMING.value,
                    "data_format": InputFormat.CSV.value,
                },
                "fail_on_empty_result": False,
                "base_checkpoint_location": "s3://dummy-bucket",
            },
            "control_db_sensor_data": Row(
                sensor_id=None,
                assets=["asset_1"],
                status=SensorStatus.ACQUIRED_NEW_DATA.value,
                status_change_timestamp=datetime(2023, 5, 26, 14, 38, 16, 676508),
                checkpoint_location="s3://dummy-bucket/sensors/sensor_id_1",
            ),
            "expected_result": False,
        },
        {
            "scenario_name": "create_non_existing_sensor_with_sensor_id_and_assets",
            "sensor_data": {
                "sensor_id": "sensor_id_1",
                "assets": ["asset_1"],
                "control_db_table_name": "control_sensor_table_name",
                "input_spec": {
                    "spec_id": "input_spec",
                    "read_type": ReadType.STREAMING.value,
                    "data_format": InputFormat.CSV.value,
                },
                "fail_on_empty_result": False,
                "base_checkpoint_location": "s3://dummy-bucket",
            },
            "control_db_sensor_data": Row(
                sensor_id="sensor_id_1",
                assets=["asset_1"],
                status=SensorStatus.ACQUIRED_NEW_DATA.value,
                status_change_timestamp=datetime(2023, 5, 26, 14, 38, 16, 676508),
                checkpoint_location="s3://dummy-bucket/sensors/sensor_id_1",
            ),
            "expected_result": False,
        },
        {
            "scenario_name": "raise_exception_as_sensor_"
            "already_exist_with_same_id_and_different_asset",
            "sensor_data": {
                "sensor_id": "sensor_id_1",
                "assets": ["asset_1"],
                "control_db_table_name": "control_sensor_table_name",
                "input_spec": {
                    "spec_id": "input_spec",
                    "read_type": ReadType.STREAMING.value,
                    "data_format": InputFormat.CSV.value,
                },
                "fail_on_empty_result": False,
                "base_checkpoint_location": "s3://dummy-bucket",
            },
            "control_db_sensor_data": Row(
                sensor_id="sensor_id_1",
                assets=["asset_2"],
                status=SensorStatus.ACQUIRED_NEW_DATA.value,
                status_change_timestamp=datetime(2023, 5, 26, 14, 38, 16, 676508),
                checkpoint_location="s3://dummy-bucket/sensors/sensor_id_1",
            ),
            "expected_result": "There's already a sensor "
            "registered with same id or assets!",
        },
        {
            "scenario_name": "raise_exception_as_sensor_"
            "already_exist_with_same_asset_and_different_id",
            "sensor_data": {
                "sensor_id": "sensor_id_1",
                "assets": ["asset_1"],
                "control_db_table_name": "control_sensor_table_name",
                "input_spec": {
                    "spec_id": "input_spec",
                    "read_type": ReadType.STREAMING.value,
                    "data_format": InputFormat.CSV.value,
                },
                "fail_on_empty_result": False,
                "base_checkpoint_location": "s3://dummy-bucket",
            },
            "control_db_sensor_data": Row(
                sensor_id="sensor_id_2",
                assets=["asset_1"],
                status=SensorStatus.ACQUIRED_NEW_DATA.value,
                status_change_timestamp=datetime(2023, 5, 26, 14, 38, 16, 676508),
                checkpoint_location="s3://dummy-bucket/sensors/sensor_id_1",
            ),
            "expected_result": "There's already a sensor "
            "registered with same id or assets!",
        },
    ],
)
def test_sensor_already_exists(scenario: dict, capsys: Any) -> None:
    """Test if Sensor already exists.

    We will raise an exception if the Sensor already exists by sensor_id or
    by assets.
    If the sensor doesn't exist we will create a new Sensor.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """
    with patch.object(
        SensorControlTableManager,
        "read_sensor_table_data",
        new=MagicMock(return_value=scenario["control_db_sensor_data"]),
    ) as sensor_already_exists_mock:
        sensor_already_exists_mock.start()
        if "raise_exception" in scenario["scenario_name"]:
            with pytest.raises(SensorAlreadyExistsException) as exception:
                Sensor(scenario["sensor_data"])

            assert scenario["expected_result"] == str(exception.value)
        else:
            subject = Sensor(scenario["sensor_data"])._check_if_sensor_already_exists()

            assert subject == scenario["expected_result"]
        sensor_already_exists_mock.stop()


class TestExecuteSensor:
    """Test suite containing tests for the Sensor execute method."""

    _sensor_already_exists_mock = patch.object(
        Sensor,
        "_check_if_sensor_already_exists",
        new=MagicMock(return_value=False),
    )

    @classmethod
    def setup_class(cls) -> None:
        """Start mock for all test methods in this suite."""
        cls._sensor_already_exists_mock.start()

    @classmethod
    def teardown_class(cls) -> None:
        """Clean mock after all test methods in this suite."""
        cls._sensor_already_exists_mock.stop()

    @pytest.mark.parametrize(
        "scenario",
        [
            {
                "scenario_name": "execute_stream_sensor",
                "sensor_data": {
                    "sensor_id": "sensor_id_1",
                    "assets": ["asset_1"],
                    "control_db_table_name": "control_sensor_table_name",
                    "input_spec": {
                        "spec_id": "input_spec",
                        "read_type": ReadType.STREAMING.value,
                        "data_format": InputFormat.CSV.value,
                    },
                    "fail_on_empty_result": False,
                    "base_checkpoint_location": "s3://dummy-bucket",
                },
                "expected_result": True,
            }
        ],
    )
    def test_execute_stream_sensor(self, scenario: dict, capsys: Any) -> None:
        """Test streaming Sensor execution.

        Args:
            scenario: scenario to test.
            capsys: capture stdout and stderr.
        """
        with patch.object(
            SensorControlTableManager,
            "check_if_sensor_has_acquired_data",
            new=MagicMock(return_value=scenario["expected_result"]),
        ) as check_if_sensor_acquired_data_mock:
            check_if_sensor_acquired_data_mock.start()
            with patch.object(
                SensorUpstreamManager,
                "read_new_data",
                new=MagicMock(
                    return_value=DataframeHelpers.create_empty_dataframe(StructType([]))
                ),
            ) as sensor_new_data_mock:
                with patch.object(
                    Sensor,
                    "_run_streaming_sensor",
                    new=MagicMock(return_value=scenario["expected_result"]),
                ) as run_stream_sensor_mock:
                    run_stream_sensor_mock.start()
                    subject = Sensor(scenario["sensor_data"]).execute()

                    assert subject == scenario["expected_result"]
                    run_stream_sensor_mock.stop()
                sensor_new_data_mock.stop()
            check_if_sensor_acquired_data_mock.stop()

    @pytest.mark.parametrize(
        "scenario",
        [
            {
                "scenario_name": "execute_batch_sensor",
                "sensor_data": {
                    "sensor_id": "sensor_id_1",
                    "assets": ["asset_1"],
                    "control_db_table_name": "control_sensor_table_name",
                    "input_spec": {
                        "spec_id": "input_spec",
                        "read_type": ReadType.BATCH.value,
                        "data_format": InputFormat.JDBC.value,
                    },
                    "base_checkpoint_location": "s3://dummy-bucket",
                },
                "expected_result": True,
            },
        ],
    )
    def test_execute_batch_sensor(self, scenario: dict, capsys: Any) -> None:
        """Test batch Sensor execution.

        Args:
            scenario: scenario to test.
            capsys: capture stdout and stderr.
        """
        with patch.object(
            SensorControlTableManager,
            "check_if_sensor_has_acquired_data",
            new=MagicMock(return_value=scenario["expected_result"]),
        ) as check_if_sensor_acquired_data_mock:
            check_if_sensor_acquired_data_mock.start()
            with patch.object(
                SensorUpstreamManager,
                "read_new_data",
                new=MagicMock(
                    return_value=DataframeHelpers.create_empty_dataframe(StructType([]))
                ),
            ) as sensor_new_data_mock:
                sensor_new_data_mock.start()
                with patch.object(
                    Sensor,
                    "_run_batch_sensor",
                    new=MagicMock(return_value=scenario["expected_result"]),
                ) as run_batch_sensor_mock:
                    run_batch_sensor_mock.start()
                    subject = Sensor(scenario["sensor_data"]).execute()

                    assert subject == scenario["expected_result"]
                    run_batch_sensor_mock.stop()
                sensor_new_data_mock.stop()
            check_if_sensor_acquired_data_mock.stop()

    @pytest.mark.parametrize(
        "scenario",
        [
            {
                "scenario_name": "raise_exception_sensor_"
                "input_spec_format_not_implemented",
                "sensor_data": {
                    "sensor_id": "sensor_id_1",
                    "assets": ["asset_1"],
                    "control_db_table_name": "control_sensor_table_name",
                    "input_spec": {
                        "spec_id": "input_spec",
                        "read_type": ReadType.BATCH.value,
                        "data_format": InputFormat.DATAFRAME.value,
                    },
                    "base_checkpoint_location": "s3://dummy-bucket",
                },
                "expected_result": "A sensor has not been "
                "implemented yet for this data format.",
            },
            {
                "scenario_name": "raise_exception_sensor_"
                "input_spec_format_doesnt_exists",
                "sensor_data": {
                    "sensor_id": "sensor_id_1",
                    "assets": ["asset_1"],
                    "control_db_table_name": "control_sensor_table_name",
                    "input_spec": {
                        "spec_id": "input_spec",
                        "db_table": "test_db.test_table",
                        "read_type": ReadType.BATCH.value,
                        "data_format": "databricks",
                    },
                    "base_checkpoint_location": "s3://dummy-bucket",
                },
                "expected_result": "Data format databricks isn't implemented yet.",
            },
        ],
    )
    def test_execute_sensor_raise_no_input_spec_format_implemented(
        self, scenario: dict, capsys: Any
    ) -> None:
        """Expect to raise exception for input spec format not implemented.

        Args:
            scenario: scenario to test.
            capsys: capture stdout and stderr.
        """
        with pytest.raises(NotImplementedError) as exception:
            Sensor(scenario["sensor_data"]).execute()

        assert scenario["expected_result"] == str(exception.value)

    @pytest.mark.parametrize(
        "scenario",
        [
            {
                "scenario_name": "raise_no_new_data_exception",
                "sensor_data": {
                    "sensor_id": "sensor_id_1",
                    "assets": ["asset_1"],
                    "control_db_table_name": "control_sensor_table_name",
                    "control_db_table_name": "control_sensor_table_name",
                    "input_spec": {
                        "spec_id": "input_spec",
                        "read_type": ReadType.STREAMING.value,
                        "data_format": InputFormat.KAFKA.value,
                    },
                    "base_checkpoint_location": "s3://dummy-bucket",
                    "fail_on_empty_result": True,
                },
                "expected_result": "No data was acquired by sensor_id_1 sensor.",
            },
        ],
    )
    def test_execute_sensor_raise_no_new_data_exception(
        self, scenario: dict, capsys: Any
    ) -> None:
        """Expect to raise exception for empty data.

        When we pass the flag `fail_on_empty_result` equals to `True`.

        Args:
            scenario: scenario to test.
            capsys: capture stdout and stderr.
        """
        with patch.object(
            SensorControlTableManager,
            "check_if_sensor_has_acquired_data",
            new=MagicMock(return_value=False),
        ) as check_if_sensor_acquired_data_mock:
            check_if_sensor_acquired_data_mock.start()
            with patch.object(
                SensorUpstreamManager,
                "read_new_data",
                new=MagicMock(
                    return_value=DataframeHelpers.create_empty_dataframe(StructType([]))
                ),
            ) as sensor_new_data_mock:
                with patch.object(
                    Sensor, "_run_streaming_sensor", new=MagicMock(return_value=False)
                ) as run_stream_sensor_mock:
                    run_stream_sensor_mock.start()
                    with pytest.raises(NoNewDataException) as exception:
                        Sensor(scenario["sensor_data"]).execute()

                    assert scenario["expected_result"] == str(exception.value)
                    run_stream_sensor_mock.stop()
                sensor_new_data_mock.stop()
            check_if_sensor_acquired_data_mock.stop()
