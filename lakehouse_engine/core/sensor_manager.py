"""Module to define Sensor Manager classes."""

from datetime import datetime
from typing import List, Optional, Union

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import array, lit

from lakehouse_engine.core.definitions import (
    SENSOR_SCHEMA,
    SENSOR_UPDATE_SET,
    SAPLogchain,
    SensorSpec,
    SensorStatus,
)
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.reader_factory import ReaderFactory
from lakehouse_engine.utils.logging_handler import LoggingHandler


class SensorControlTableManager(object):
    """Class to control the Sensor execution."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    @classmethod
    def check_if_sensor_has_acquired_data(
        cls,
        sensor_id: str,
        control_db_table_name: str,
    ) -> bool:
        """Check if sensor has acquired new data.

        Args:
            sensor_id: sendor id.
            control_db_table_name: db.table to control sensor runs.

        Returns:
            True if acquired new data, otherwise False
        """
        sensor_table_data = cls.read_sensor_table_data(
            sensor_id=sensor_id, control_db_table_name=control_db_table_name
        )
        cls._LOGGER.info(f"sensor_table_data = {sensor_table_data}")

        return (
            sensor_table_data is not None
            and sensor_table_data.status == SensorStatus.ACQUIRED_NEW_DATA.value
        )

    @classmethod
    def update_sensor_status(
        cls,
        sensor_spec: SensorSpec,
        status: str,
        upstream_key: str = None,
        upstream_value: str = None,
    ) -> None:
        """Control sensor execution storing the execution data in a delta table.

        Args:
            sensor_spec: sensor spec containing all sensor
                information we need to update the control status.
            status: status of the sensor.
            upstream_key: upstream key (e.g., used to store an attribute
                name from the upstream so that new data can be detected
                automatically).
            upstream_value: upstream value (e.g., used to store the max
                attribute value from the upstream so that new data can be
                detected automatically).
        """
        cls._LOGGER.info(
            f"Updating sensor status for sensor {sensor_spec.sensor_id}..."
        )

        data = cls._convert_sensor_to_data(
            spec=sensor_spec,
            status=status,
            upstream_key=upstream_key,
            upstream_value=upstream_value,
        )

        sensor_update_set = cls._get_sensor_update_set(
            assets=sensor_spec.assets,
            checkpoint_location=sensor_spec.checkpoint_location,
            upstream_key=upstream_key,
            upstream_value=upstream_value,
        )

        cls._update_sensor_control(
            data=data,
            sensor_update_set=sensor_update_set,
            sensor_control_table=sensor_spec.control_db_table_name,
            sensor_id=sensor_spec.sensor_id,
        )

    @classmethod
    def _update_sensor_control(
        cls,
        data: List[dict],
        sensor_update_set: dict,
        sensor_control_table: str,
        sensor_id: str,
    ) -> None:
        """Update sensor control delta table.

        Args:
            data: to be updated.
            sensor_update_set: columns which we had update.
            sensor_control_table: control table name.
            sensor_id: sensor_id to be updated.
        """
        sensors_delta_table = DeltaTable.forName(
            ExecEnv.SESSION,
            sensor_control_table,
        )
        sensors_updates = ExecEnv.SESSION.createDataFrame(data, SENSOR_SCHEMA)
        sensors_delta_table.alias("sensors").merge(
            sensors_updates.alias("updates"),
            f"sensors.sensor_id = '{sensor_id}' AND "
            "sensors.sensor_id = updates.sensor_id",
        ).whenMatchedUpdate(set=sensor_update_set).whenNotMatchedInsertAll().execute()

    @classmethod
    def _convert_sensor_to_data(
        cls,
        spec: SensorSpec,
        status: str,
        upstream_key: str,
        upstream_value: str,
        status_change_timestamp: Optional[datetime] = None,
    ) -> List[dict]:
        """Convert sensor data to dataframe input data.

        Args:
            spec: sensor spec containing sensor identifier data.
            status: new sensor data status.
            upstream_key: key used to acquired data from the upstream.
            upstream_value: max value from the upstream_key
                acquired from the upstream.
            status_change_timestamp: timestamp we commit
                this change in the sensor control table.

        Return:
            Sensor data as list[dict], used to create a
                dataframe to store the data into the sensor_control_table.
        """
        status_change_timestamp = (
            datetime.now()
            if status_change_timestamp is None
            else status_change_timestamp
        )
        return [
            {
                "sensor_id": spec.sensor_id,
                "assets": spec.assets,
                "status": status,
                "status_change_timestamp": status_change_timestamp,
                "checkpoint_location": spec.checkpoint_location,
                "upstream_key": upstream_key,
                "upstream_value": upstream_value,
            }
        ]

    @classmethod
    def _get_sensor_update_set(cls, **kwargs: Union[Optional[str], List[str]]) -> dict:
        """Get the sensor update set.

        Args:
            kwargs: Containing the following keys:
            - assets
            - checkpoint_location
            - upstream_key
            - upstream_value

        Return:
            A set containing the fields to update in the control_table.
        """
        sensor_update_set = dict(SENSOR_UPDATE_SET)
        for key, value in kwargs.items():
            if value:
                sensor_update_set[f"sensors.{key}"] = f"updates.{key}"

        return sensor_update_set

    @classmethod
    def read_sensor_table_data(
        cls,
        control_db_table_name: str,
        sensor_id: str = None,
        assets: list = None,
    ) -> Optional[Row]:
        """Read data from delta table containing sensor status info.

        Args:
            sensor_id: sensor id. If this parameter is defined search occurs
                only considering this parameter. Otherwise it considers sensor
                assets and checkpoint location.
            control_db_table_name: db.table to control sensor runs.
            assets: list of assets that are fueled by the pipeline
                where this sensor is.

        Return:
            Row containing the data for the provided sensor_id.
        """
        df = DeltaTable.forName(
            ExecEnv.SESSION,
            control_db_table_name,
        ).toDF()

        if sensor_id:
            df = df.where(df.sensor_id == sensor_id)
        elif assets:
            df = df.where(df.assets == array(*[lit(asset) for asset in assets]))
        else:
            raise ValueError(
                "Either sensor_id or assets need to be provided as arguments."
            )

        return df.first()


class SensorUpstreamManager(object):
    """Class to deal with Sensor Upstream data."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    @classmethod
    def generate_filter_exp_query(
        cls,
        sensor_id: str,
        filter_exp: str,
        control_db_table_name: str = None,
        upstream_key: str = None,
        upstream_value: str = None,
        upstream_table_name: str = None,
    ) -> str:
        """Generates a sensor preprocess query based on timestamp logic.

        Args:
            sensor_id: sensor id.
            filter_exp: expression to filter incoming new data.
                You can use the placeholder `?upstream_value` so that
                it can be replaced by the upstream_value in the
                control_db_table_name for this specific sensor_id.
            control_db_table_name: db.table to retrieve the last status change
                timestamp. This is only relevant for the jdbc sensor.
            upstream_key: the key of custom sensor information
                to control how to identify new data from the
                upstream (e.g., a time column in the upstream).
            upstream_value: value for custom sensor
                to identify new data from the upstream
                (e.g., the value of a time present in the upstream)
                If none we will set the default value.
                Note: This parameter is used just to override the
                default value `-2147483647`.
            upstream_table_name: value for custom sensor
                to query new data from the upstream.
                If none we will set the default value,
                our `sensor_new_data` view.

        Return:
            The query string.
        """
        source_table = upstream_table_name if upstream_table_name else "sensor_new_data"
        select_exp = "SELECT COUNT(1) as count"
        if control_db_table_name:
            if not upstream_key:
                raise ValueError(
                    "If control_db_table_name is defined, upstream_key should "
                    "also be defined!"
                )

            default_upstream_value: str = "-2147483647"
            trigger_name = upstream_key
            trigger_value = (
                default_upstream_value if upstream_value is None else upstream_value
            )
            sensor_table_data = SensorControlTableManager.read_sensor_table_data(
                sensor_id=sensor_id, control_db_table_name=control_db_table_name
            )

            if sensor_table_data and sensor_table_data.upstream_value:
                trigger_value = sensor_table_data.upstream_value

            filter_exp = filter_exp.replace("?upstream_key", trigger_name).replace(
                "?upstream_value", trigger_value
            )
            select_exp = (
                f"SELECT COUNT(1) as count, '{trigger_name}' as UPSTREAM_KEY, "
                f"max({trigger_name}) as UPSTREAM_VALUE"
            )

        query = (
            f"{select_exp} "
            f"FROM {source_table} "
            f"WHERE {filter_exp} "
            f"HAVING COUNT(1) > 0"
        )

        return query

    @classmethod
    def generate_sensor_table_preprocess_query(
        cls,
        sensor_id: str,
    ) -> str:
        """Generates a query to be used for a sensor having other sensor as upstream.

        Args:
            sensor_id: sensor id.

        Return:
            The query string.
        """
        query = (
            f"SELECT * "  # nosec
            f"FROM sensor_new_data "
            f"WHERE"
            f" _change_type in ('insert', 'update_postimage')"
            f" and sensor_id = '{sensor_id}'"
            f" and status = '{SensorStatus.PROCESSED_NEW_DATA.value}'"
        )

        return query

    @classmethod
    def read_new_data(cls, sensor_spec: SensorSpec) -> DataFrame:
        """Read new data from the upstream into the sensor 'new_data_df'.

        Args:
            sensor_spec: sensor spec containing all sensor information.

        Return:
            An empty dataframe if doesn't have new data otherwise the new data
        """
        new_data_df = ReaderFactory.get_data(sensor_spec.input_spec)

        if sensor_spec.preprocess_query:
            new_data_df.createOrReplaceTempView("sensor_new_data")
            new_data_df = ExecEnv.SESSION.sql(sensor_spec.preprocess_query)

        return new_data_df

    @classmethod
    def get_new_data(
        cls,
        new_data_df: DataFrame,
    ) -> Optional[Row]:
        """Get new data from upstream df if it's present.

        Args:
            new_data_df: DataFrame possibly containing new data.

        Return:
            Optional row, present if there is new data in the upstream,
            absent otherwise.
        """
        return new_data_df.first()

    @classmethod
    def generate_sensor_sap_logchain_query(
        cls,
        chain_id: str,
        dbtable: str = SAPLogchain.DBTABLE.value,
        status: str = SAPLogchain.GREEN_STATUS.value,
        engine_table_name: str = SAPLogchain.ENGINE_TABLE.value,
    ) -> str:
        """Generates a sensor query based in the SAP Logchain table.

        Args:
            chain_id: chain id to query the status on SAP.
            dbtable: db.table to retrieve the data to
                check if the sap chain is already finished.
            status: db.table to retrieve the last status change
                timestamp.
            engine_table_name: table name exposed with the SAP LOGCHAIN data.
                This table will be used in the jdbc query.

        Return:
            The query string.
        """
        if not chain_id:
            raise ValueError(
                "To query on log chain SAP table the chain id should be defined!"
            )

        select_exp = (
            "SELECT CHAIN_ID, CONCAT(DATUM, ZEIT) AS LOAD_DATE, ANALYZED_STATUS"
        )
        filter_exp = (
            f"UPPER(CHAIN_ID) = UPPER('{chain_id}') "
            f"AND UPPER(ANALYZED_STATUS) = UPPER('{status}')"
        )

        query = (
            f"WITH {engine_table_name} AS ("
            f"{select_exp} "
            f"FROM {dbtable} "
            f"WHERE {filter_exp}"
            ")"
        )

        return query
