"""Contract of the lakehouse engine with all the available functions to be executed."""
from typing import List, Optional, OrderedDict

from lakehouse_engine.algorithms.data_loader import DataLoader
from lakehouse_engine.algorithms.dq_validator import DQValidator
from lakehouse_engine.algorithms.reconciliator import Reconciliator
from lakehouse_engine.algorithms.sensor import Sensor, SensorStatus
from lakehouse_engine.core.definitions import SAPLogchain, TerminatorSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.core.file_manager import FileManager
from lakehouse_engine.core.sensor_manager import SensorUpstreamManager
from lakehouse_engine.core.table_manager import TableManager
from lakehouse_engine.terminators.notifier_factory import NotifierFactory
from lakehouse_engine.terminators.sensor_terminator import SensorTerminator
from lakehouse_engine.utils.configs.config_utils import ConfigUtils


def load_data(
    acon_path: Optional[str] = None, acon: Optional[dict] = None
) -> Optional[OrderedDict]:
    """Load data using the DataLoader algorithm.

    Args:
        acon_path: path of the acon (algorithm configuration) file.
        acon: acon provided directly through python code (e.g., notebooks or other
            apps).
    """
    acon = ConfigUtils.get_acon(acon_path, acon)
    ExecEnv.get_or_create(app_name="data_loader", config=acon.get("exec_env", None))
    return DataLoader(acon).execute()


def execute_reconciliation(
    acon_path: Optional[str] = None, acon: Optional[dict] = None
) -> None:
    """Execute the Reconciliator algorithm.

    Args:
        acon_path: path of the acon (algorithm configuration) file.
        acon: acon provided directly through python code (e.g., notebooks or other
            apps).
    """
    acon = ConfigUtils.get_acon(acon_path, acon)
    ExecEnv.get_or_create(app_name="reconciliator", config=acon.get("exec_env", None))
    Reconciliator(acon).execute()


def execute_dq_validation(
    acon_path: Optional[str] = None, acon: Optional[dict] = None
) -> None:
    """Execute the DQValidator algorithm.

    Args:
        acon_path: path of the acon (algorithm configuration) file.
        acon: acon provided directly through python code (e.g., notebooks or other
            apps).
    """
    acon = ConfigUtils.get_acon(acon_path, acon)
    ExecEnv.get_or_create(app_name="dq_validator", config=acon.get("exec_env", None))
    DQValidator(acon).execute()


def manage_table(acon_path: Optional[str] = None, acon: Optional[dict] = None) -> None:
    """Manipulate tables/views using Table Manager algorithm.

    Args:
        acon_path: path of the acon (algorithm configuration) file.
        acon: acon provided directly through python code (e.g., notebooks
            or other apps).
    """
    acon = ConfigUtils.get_acon(acon_path, acon)
    ExecEnv.get_or_create(app_name="manage_table", config=acon.get("exec_env", None))
    TableManager(acon).get_function()


def manage_files(acon_path: Optional[str] = None, acon: Optional[dict] = None) -> None:
    """Manipulate s3 files using File Manager algorithm.

    Args:
        acon_path: path of the acon (algorithm configuration) file.
        acon: acon provided directly through python code (e.g., notebooks
            or other apps).
    """
    acon = ConfigUtils.get_acon(acon_path, acon)
    ExecEnv.get_or_create(app_name="manage_files", config=acon.get("exec_env", None))
    FileManager(acon).get_function()


def execute_sensor(
    acon_path: Optional[str] = None, acon: Optional[dict] = None
) -> bool:
    """Execute a sensor based on a Sensor Algorithm Configuration.

    A sensor is useful to check if an upstream system has new data.

    Args:
        acon_path: path of the acon (algorithm configuration) file.
        acon: acon provided directly through python code (e.g., notebooks
            or other apps).
    """
    acon = ConfigUtils.get_acon(acon_path, acon)
    ExecEnv.get_or_create(app_name="execute_sensor", config=acon.get("exec_env", None))
    return Sensor(acon).execute()


def update_sensor_status(
    sensor_id: str,
    control_db_table_name: str,
    status: str = SensorStatus.PROCESSED_NEW_DATA.value,
    assets: List[str] = None,
) -> None:
    """Update internal sensor status.

    Update the sensor status in the control table,
    it should be used to tell the system
    that the sensor has processed all new data that was previously identified,
    hence updating the shifted sensor status.
    Usually used to move from `SensorStatus.ACQUIRED_NEW_DATA` to
    `SensorStatus.PROCESSED_NEW_DATA`,
    but there might be scenarios - still to identify -
    where we can update the sensor status from/to different statuses.

    Args:
        sensor_id: sensor id.
        control_db_table_name: db.table to store sensor checkpoints.
        status: status of the sensor.
        assets: a list of assets that are considered as available to
            consume downstream after this sensor has status
            PROCESSED_NEW_DATA.
    """
    ExecEnv.get_or_create(app_name="update_sensor_status")
    SensorTerminator.update_sensor_status(
        sensor_id=sensor_id,
        control_db_table_name=control_db_table_name,
        status=status,
        assets=assets,
    )


def generate_sensor_query(
    sensor_id: str,
    filter_exp: str = None,
    control_db_table_name: str = None,
    upstream_key: str = None,
    upstream_value: str = None,
    upstream_table_name: str = None,
) -> str:
    """Generates a preprocess query to be used in a sensor configuration.

    Args:
        sensor_id: sensor id.
        filter_exp: expression to filter incoming new data.
            You can use the placeholder ?default_upstream_key and
            ?default_upstream_value, so that it can be replaced by the
            respective values in the control_db_table_name for this specific
            sensor_id.
        control_db_table_name: db.table to retrieve the last status change
            timestamp. This is only relevant for the jdbc sensor.
        upstream_key: the key of custom sensor information to control how to
            identify new data from the upstream (e.g., a time column in the
            upstream).
        upstream_value: the upstream value
            to identify new data from the upstream (e.g., the value of a time
            present in the upstream).
        upstream_table_name: value for custom sensor
                to query new data from the upstream
                If none we will set the default value,
                our `sensor_new_data` view.

    Return:
        The query string.
    """
    ExecEnv.get_or_create(app_name="generate_sensor_preprocess_query")
    if filter_exp:
        return SensorUpstreamManager.generate_filter_exp_query(
            sensor_id=sensor_id,
            filter_exp=filter_exp,
            control_db_table_name=control_db_table_name,
            upstream_key=upstream_key,
            upstream_value=upstream_value,
            upstream_table_name=upstream_table_name,
        )
    else:
        return SensorUpstreamManager.generate_sensor_table_preprocess_query(
            sensor_id=sensor_id
        )


def generate_sensor_sap_logchain_query(
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
    ExecEnv.get_or_create(app_name="generate_sensor_sap_logchain_query")
    return SensorUpstreamManager.generate_sensor_sap_logchain_query(
        chain_id=chain_id,
        dbtable=dbtable,
        status=status,
        engine_table_name=engine_table_name,
    )


def send_notification(args: dict) -> None:
    """Send a notification using a notifier.

    Args:
        args: arguments for the notifier.
    """
    notifier = NotifierFactory.get_notifier(
        spec=TerminatorSpec(function="notify", args=args)
    )

    notifier.create_notification()
    notifier.send_notification()
