"""Contract of the lakehouse engine with all the available functions to be executed."""

from typing import List, Optional, OrderedDict

from lakehouse_engine.algorithms.data_loader import DataLoader
from lakehouse_engine.algorithms.gab import GAB
from lakehouse_engine.algorithms.reconciliator import Reconciliator
from lakehouse_engine.algorithms.sensor import Sensor, SensorStatus
from lakehouse_engine.core.definitions import (
    CollectEngineUsage,
    DQDefaults,
    SAPLogchain,
    TerminatorSpec,
)
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.core.file_manager import FileManagerFactory
from lakehouse_engine.core.sensor_manager import SensorUpstreamManager
from lakehouse_engine.core.table_manager import TableManager
from lakehouse_engine.terminators.notifier_factory import NotifierFactory
from lakehouse_engine.terminators.sensor_terminator import SensorTerminator
from lakehouse_engine.utils.acon_utils import validate_and_resolve_acon
from lakehouse_engine.utils.configs.config_utils import ConfigUtils
from lakehouse_engine.utils.engine_usage_stats import EngineUsageStats


def load_data(
    acon_path: Optional[str] = None,
    acon: Optional[dict] = None,
    collect_engine_usage: str = CollectEngineUsage.PROD_ONLY.value,
    spark_confs: dict = None,
) -> Optional[OrderedDict]:
    """Load data using the DataLoader algorithm.

    Args:
        acon_path: path of the acon (algorithm configuration) file.
        acon: acon provided directly through python code (e.g., notebooks or other
            apps).
        collect_engine_usage: Lakehouse usage statistics collection strategy.
        spark_confs: optional dictionary with the spark confs to be used when collecting
            the engine usage.
    """
    try:
        acon = ConfigUtils.get_acon(acon_path, acon)
        ExecEnv.get_or_create(app_name="data_loader", config=acon.get("exec_env", None))
        acon = validate_and_resolve_acon(acon, "in_motion")
    finally:
        EngineUsageStats.store_engine_usage(
            acon, load_data.__name__, collect_engine_usage, spark_confs
        )
    return DataLoader(acon).execute()


def execute_reconciliation(
    acon_path: Optional[str] = None,
    acon: Optional[dict] = None,
    collect_engine_usage: str = CollectEngineUsage.PROD_ONLY.value,
    spark_confs: dict = None,
) -> None:
    """Execute the Reconciliator algorithm.

    Args:
        acon_path: path of the acon (algorithm configuration) file.
        acon: acon provided directly through python code (e.g., notebooks or other
            apps).
        collect_engine_usage: Lakehouse usage statistics collection strategy.
        spark_confs: optional dictionary with the spark confs to be used when collecting
            the engine usage.
    """
    try:
        acon = ConfigUtils.get_acon(acon_path, acon)
        ExecEnv.get_or_create(
            app_name="reconciliator", config=acon.get("exec_env", None)
        )
        acon = validate_and_resolve_acon(acon)
    finally:
        EngineUsageStats.store_engine_usage(
            acon, execute_reconciliation.__name__, collect_engine_usage, spark_confs
        )
    Reconciliator(acon).execute()


def execute_dq_validation(
    acon_path: Optional[str] = None,
    acon: Optional[dict] = None,
    collect_engine_usage: str = CollectEngineUsage.PROD_ONLY.value,
    spark_confs: dict = None,
) -> None:
    """Execute the DQValidator algorithm.

    Args:
        acon_path: path of the acon (algorithm configuration) file.
        acon: acon provided directly through python code (e.g., notebooks or other
            apps).
        collect_engine_usage: Lakehouse usage statistics collection strategy.
        spark_confs: optional dictionary with the spark confs to be used when collecting
            the engine usage.
    """
    from lakehouse_engine.algorithms.dq_validator import DQValidator

    try:
        acon = ConfigUtils.get_acon(acon_path, acon)
        ExecEnv.get_or_create(
            app_name="dq_validator", config=acon.get("exec_env", None)
        )
        acon = validate_and_resolve_acon(acon, "at_rest")
    finally:
        EngineUsageStats.store_engine_usage(
            acon, execute_dq_validation.__name__, collect_engine_usage, spark_confs
        )
    DQValidator(acon).execute()


def manage_table(
    acon_path: Optional[str] = None,
    acon: Optional[dict] = None,
    collect_engine_usage: str = CollectEngineUsage.PROD_ONLY.value,
    spark_confs: dict = None,
) -> None:
    """Manipulate tables/views using Table Manager algorithm.

    Args:
        acon_path: path of the acon (algorithm configuration) file.
        acon: acon provided directly through python code (e.g., notebooks
            or other apps).
        collect_engine_usage: Lakehouse usage statistics collection strategy.
        spark_confs: optional dictionary with the spark confs to be used when collecting
            the engine usage.
    """
    acon = ConfigUtils.get_acon(acon_path, acon)
    ExecEnv.get_or_create(app_name="manage_table", config=acon.get("exec_env", None))
    EngineUsageStats.store_engine_usage(
        acon, manage_table.__name__, collect_engine_usage, spark_confs
    )
    TableManager(acon).get_function()


def manage_files(
    acon_path: Optional[str] = None,
    acon: Optional[dict] = None,
    collect_engine_usage: str = CollectEngineUsage.PROD_ONLY.value,
    spark_confs: dict = None,
) -> None:
    """Manipulate s3 files using File Manager algorithm.

    Args:
        acon_path: path of the acon (algorithm configuration) file.
        acon: acon provided directly through python code (e.g., notebooks
            or other apps).
        collect_engine_usage: Lakehouse usage statistics collection strategy.
        spark_confs: optional dictionary with the spark confs to be used when collecting
            the engine usage.
    """
    acon = ConfigUtils.get_acon(acon_path, acon)
    ExecEnv.get_or_create(app_name="manage_files", config=acon.get("exec_env", None))
    EngineUsageStats.store_engine_usage(
        acon, manage_files.__name__, collect_engine_usage, spark_confs
    )
    FileManagerFactory.execute_function(configs=acon)


def execute_sensor(
    acon_path: Optional[str] = None,
    acon: Optional[dict] = None,
    collect_engine_usage: str = CollectEngineUsage.PROD_ONLY.value,
    spark_confs: dict = None,
) -> bool:
    """Execute a sensor based on a Sensor Algorithm Configuration.

    A sensor is useful to check if an upstream system has new data.

    Args:
        acon_path: path of the acon (algorithm configuration) file.
        acon: acon provided directly through python code (e.g., notebooks
            or other apps).
        collect_engine_usage: Lakehouse usage statistics collection strategy.
        spark_confs: optional dictionary with the spark confs to be used when collecting
            the engine usage.
    """
    acon = ConfigUtils.get_acon(acon_path, acon)
    ExecEnv.get_or_create(app_name="execute_sensor", config=acon.get("exec_env", None))
    EngineUsageStats.store_engine_usage(
        acon, execute_sensor.__name__, collect_engine_usage, spark_confs
    )
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
        control_db_table_name: `db.table` to store sensor checkpoints.
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
        control_db_table_name: `db.table` to retrieve the last status change
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

    Returns:
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
        dbtable: `db.table` to retrieve the data to
            check if the sap chain is already finished.
        status: `db.table` to retrieve the last status change
            timestamp.
        engine_table_name: table name exposed with the SAP LOGCHAIN data.
            This table will be used in the jdbc query.

    Returns:
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


def build_data_docs(
    store_backend: str = DQDefaults.STORE_BACKEND.value,
    local_fs_root_dir: str = None,
    data_docs_local_fs: str = None,
    data_docs_prefix: str = DQDefaults.DATA_DOCS_PREFIX.value,
    bucket: str = None,
    data_docs_bucket: str = None,
    expectations_store_prefix: str = DQDefaults.EXPECTATIONS_STORE_PREFIX.value,
    validations_store_prefix: str = DQDefaults.VALIDATIONS_STORE_PREFIX.value,
    checkpoint_store_prefix: str = DQDefaults.CHECKPOINT_STORE_PREFIX.value,
) -> None:
    """Build Data Docs for the project.

    This function does a full build of data docs based on all the great expectations
    checkpoints in the specified location, getting all history of run/validations
    executed and results.

    Args:
        store_backend: which store_backend to use (e.g. s3 or file_system).
        local_fs_root_dir: path of the root directory. Note: only applicable
            for store_backend file_system
        data_docs_local_fs: path of the root directory. Note: only applicable
            for store_backend file_system.
        data_docs_prefix: prefix where to store data_docs' data.
        bucket: the bucket name to consider for the store_backend
            (store DQ artefacts). Note: only applicable for store_backend s3.
        data_docs_bucket: the bucket name for data docs only. When defined,
            it will supersede bucket parameter.
            Note: only applicable for store_backend s3.
        expectations_store_prefix: prefix where to store expectations' data.
            Note: only applicable for store_backend s3.
        validations_store_prefix: prefix where to store validations' data.
            Note: only applicable for store_backend s3.
        checkpoint_store_prefix: prefix where to store checkpoints' data.
            Note: only applicable for store_backend s3.
    """
    from lakehouse_engine.dq_processors.dq_factory import DQFactory

    DQFactory.build_data_docs(
        store_backend=store_backend,
        local_fs_root_dir=local_fs_root_dir,
        data_docs_local_fs=data_docs_local_fs,
        data_docs_prefix=data_docs_prefix,
        bucket=bucket,
        data_docs_bucket=data_docs_bucket,
        expectations_store_prefix=expectations_store_prefix,
        validations_store_prefix=validations_store_prefix,
        checkpoint_store_prefix=checkpoint_store_prefix,
    )


def execute_gab(
    acon_path: Optional[str] = None,
    acon: Optional[dict] = None,
    collect_engine_usage: str = CollectEngineUsage.PROD_ONLY.value,
    spark_confs: dict = None,
) -> None:
    """Execute the gold asset builder based on a GAB Algorithm Configuration.

    GaB is useful to build your gold assets with predefined functions for recurrent
    periods.

    Args:
        acon_path: path of the acon (algorithm configuration) file.
        acon: acon provided directly through python code (e.g., notebooks
            or other apps).
        collect_engine_usage: Lakehouse usage statistics collection strategy.
        spark_confs: optional dictionary with the spark confs to be used when collecting
            the engine usage.
    """
    acon = ConfigUtils.get_acon(acon_path, acon)
    ExecEnv.get_or_create(app_name="execute_gab", config=acon.get("exec_env", None))
    EngineUsageStats.store_engine_usage(
        acon, execute_gab.__name__, collect_engine_usage, spark_confs
    )
    GAB(acon).execute()
