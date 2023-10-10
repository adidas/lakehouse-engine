"""Defines terminator behaviour."""
from typing import List

from lakehouse_engine.core.definitions import SensorSpec, SensorStatus
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.core.sensor_manager import SensorControlTableManager
from lakehouse_engine.utils.logging_handler import LoggingHandler


class SensorTerminator(object):
    """Sensor Terminator class."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def update_sensor_status(
        cls,
        sensor_id: str,
        control_db_table_name: str,
        status: str = SensorStatus.PROCESSED_NEW_DATA.value,
        assets: List[str] = None,
    ) -> None:
        """Update internal sensor status.

        Update the sensor status in the control table, it should be used to tell the
        system that the sensor has processed all new data that was previously
        identified, hence updating the shifted sensor status.
        Usually used to move from `SensorStatus.ACQUIRED_NEW_DATA` to
        `SensorStatus.PROCESSED_NEW_DATA`, but there might be scenarios - still
        to identify - where we can update the sensor status from/to different statuses.

        Args:
            sensor_id: sensor id.
            control_db_table_name: db.table to store sensor checkpoints.
            status: status of the sensor.
            assets: a list of assets that are considered as available to
                consume downstream after this sensor has status
                PROCESSED_NEW_DATA.
        """
        if status not in [s.value for s in SensorStatus]:
            raise NotImplementedError(f"Status {status} not accepted in sensor.")

        ExecEnv.get_or_create(app_name="update_sensor_status")
        SensorControlTableManager.update_sensor_status(
            sensor_spec=SensorSpec(
                sensor_id=sensor_id,
                control_db_table_name=control_db_table_name,
                assets=assets,
                input_spec=None,
                preprocess_query=None,
                checkpoint_location=None,
            ),
            status=status,
        )
