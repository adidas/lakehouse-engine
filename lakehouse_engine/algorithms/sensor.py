"""Module to define Sensor algorithm behavior."""
from pyspark.sql import DataFrame

from lakehouse_engine.algorithms.algorithm import Algorithm
from lakehouse_engine.algorithms.exceptions import (
    NoNewDataException,
    SensorAlreadyExistsException,
)
from lakehouse_engine.core.definitions import (
    FILE_INPUT_FORMATS,
    InputFormat,
    SensorSpec,
    SensorStatus,
)
from lakehouse_engine.core.sensor_manager import (
    SensorControlTableManager,
    SensorUpstreamManager,
)
from lakehouse_engine.utils.logging_handler import LoggingHandler


class Sensor(Algorithm):
    """Class representing a sensor to check if the upstream has new data."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    def __init__(self, acon: dict):
        """Construct Sensor instances.

        Args:
            acon: algorithm configuration.
        """
        self.spec: SensorSpec = SensorSpec.create_from_acon(acon=acon)

        if self._check_if_sensor_already_exists():
            raise SensorAlreadyExistsException(
                "There's already a sensor registered with same id or assets!"
            )

    def execute(self) -> bool:
        """Execute the sensor."""
        self._LOGGER.info(f"Starting {self.spec.input_spec.data_format} sensor...")

        if InputFormat.exists(self.spec.input_spec.data_format):
            new_data_df = SensorUpstreamManager.read_new_data(sensor_spec=self.spec)
            if (
                self.spec.input_spec.data_format == InputFormat.KAFKA.value
                or self.spec.input_spec.data_format in FILE_INPUT_FORMATS
                or (
                    self.spec.input_spec.db_table is not None
                    and self.spec.input_spec.data_format != InputFormat.JDBC.value
                )
            ):
                Sensor._run_streaming_sensor(
                    sensor_spec=self.spec, new_data_df=new_data_df
                )
            elif self.spec.input_spec.data_format == InputFormat.JDBC.value:
                Sensor._run_batch_sensor(
                    sensor_spec=self.spec,
                    new_data_df=new_data_df,
                )
            else:
                raise NotImplementedError(
                    "A sensor has not been implemented yet for this data format."
                )
        else:
            raise NotImplementedError(
                f"Data format {self.spec.input_spec.data_format} isn't implemented yet."
            )

        has_new_data = SensorControlTableManager.check_if_sensor_has_acquired_data(
            self.spec.sensor_id,
            self.spec.control_db_table_name,
        )

        self._LOGGER.info(
            f"Sensor {self.spec.sensor_id} has previously "
            f"acquired data? {has_new_data}"
        )

        if self.spec.fail_on_empty_result and not has_new_data:
            raise NoNewDataException(
                f"No data was acquired by {self.spec.sensor_id} sensor."
            )

        return has_new_data

    def _check_if_sensor_already_exists(self) -> bool:
        """Check if sensor already exists in the table to avoid duplicates."""
        row = SensorControlTableManager.read_sensor_table_data(
            sensor_id=self.spec.sensor_id,
            control_db_table_name=self.spec.control_db_table_name,
        )

        if row and row.assets != self.spec.assets:
            return True
        else:
            row = SensorControlTableManager.read_sensor_table_data(
                assets=self.spec.assets,
                control_db_table_name=self.spec.control_db_table_name,
            )
            return row is not None and row.sensor_id != self.spec.sensor_id

    @classmethod
    def _run_streaming_sensor(
        cls, sensor_spec: SensorSpec, new_data_df: DataFrame
    ) -> None:
        """Run sensor in streaming mode (internally runs in batch mode)."""
        if not new_data_df.isStreaming:
            raise RuntimeError(
                f"The sensor {sensor_spec.sensor_id} for the requested data "
                "format needs to be executed "
                "in streaming mode! Please change the read type of the input "
                "spec."
            )

        def foreach_batch_check_new_data(df: DataFrame, batch_id: int) -> None:
            Sensor._run_batch_sensor(
                sensor_spec=sensor_spec,
                new_data_df=df,
            )

        new_data_df.writeStream.trigger(availableNow=True).option(
            "checkpointLocation", sensor_spec.checkpoint_location
        ).foreachBatch(foreach_batch_check_new_data).start().awaitTermination()

    @classmethod
    def _run_batch_sensor(
        cls,
        sensor_spec: SensorSpec,
        new_data_df: DataFrame,
    ) -> None:
        """Run sensor in batch mode.

        Args:
            sensor_spec: sensor spec containing all sensor information.
            new_data_df: DataFrame possibly containing new data.
        """
        new_data_first_row = SensorUpstreamManager.get_new_data(new_data_df)

        cls._LOGGER.info(
            f"Sensor {sensor_spec.sensor_id} has new data from upstream? "
            f"{new_data_first_row is not None}"
        )

        if new_data_first_row:
            SensorControlTableManager.update_sensor_status(
                sensor_spec=sensor_spec,
                status=SensorStatus.ACQUIRED_NEW_DATA.value,
                upstream_key=(
                    new_data_first_row.UPSTREAM_KEY
                    if "UPSTREAM_KEY" in new_data_df.columns
                    else None
                ),
                upstream_value=(
                    new_data_first_row.UPSTREAM_VALUE
                    if "UPSTREAM_VALUE" in new_data_df.columns
                    else None
                ),
            )
            cls._LOGGER.info(
                f"Successfully updated sensor status for sensor "
                f"{sensor_spec.sensor_id}..."
            )
