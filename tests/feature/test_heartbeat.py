"""Module with integration tests for heartbeat feature."""

import datetime
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType

from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.engine import (
    execute_heartbeat_sensor_data_feed,
    execute_sensor_heartbeat,
    trigger_heartbeat_sensor_jobs,
    update_heartbeat_sensor_status,
)
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.schema_utils import SchemaUtils
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_NAME = "heartbeat"
FEATURE_TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_NAME}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_NAME}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_NAME}"
_LOGGER = LoggingHandler(__name__).get_logger()


def _create_heartbeat_table(scenario_name: str, tables: dict) -> None:
    """Create the necessary tables required for using Heartbeat.

    Args:
        scenario_name (str): The name of the scenario.
        tables (dict): Table names.
    """
    for _, table_name in tables.items():
        DataframeHelpers.create_delta_table(
            cols=SchemaUtils.from_file_to_dict(
                f"file:///{FEATURE_TEST_RESOURCES}/setup/"
                f"{scenario_name}/column_list/{table_name}.json"
            ),
            table=table_name,
        )


def _test_heartbeat_sensor_data_feed(
    heartbeat_data_file_path: str,
    heartbeat_control_table_name: str,
    ctrl_heartbeat_df: DataFrame,
) -> None:
    """Test the function that populates the heartbeat control table.

    Args:
        heartbeat_data_file_path (str): Path to the CSV file used
            to populate the control table.
        heartbeat_control_table_name (str): Name of the target
            control table.
        ctrl_heartbeat_df (DataFrame): Reference DataFrame
            used to validate the table contents.
    """
    _LOGGER.info("Testing execute_heartbeat_sensor_data_feed function")

    execute_heartbeat_sensor_data_feed(
        heartbeat_data_file_path, heartbeat_control_table_name
    )
    heartbeat_df = ExecEnv.SESSION.table(f"{heartbeat_control_table_name}")

    assert not DataframeHelpers.has_diff(heartbeat_df, ctrl_heartbeat_df)


@patch(
    "lakehouse_engine.algorithms.sensors.heartbeat.Heartbeat._execute_batch_of_sensor",
    MagicMock(
        return_value={
            "sensor_id": "dummy_delta_table",
            "trigger_job_id": "1927384615203749",
        }
    ),
)
@patch("lakehouse_engine.algorithms.sensors.heartbeat.current_timestamp")
def _test_execute_sensor_heartbeat(
    mocked_timestamp: MagicMock,
    acon: dict,
    heartbeat_control_table_name: str,
    ctrl_heartbeat_df: DataFrame,
    results: dict,
) -> None:
    """Test the execution of the sensor heartbeat process.

    This test mocks the internal `_execute_batch_of_sensor` method
    to simulate the heartbeat execution, then validates the
    resulting state in the heartbeat control table after
    the execution of the execute_sensor_heartbeat function.

    Args:
        mocked_timestamp (MagicMock): A static timestamp for testing.
        acon (dict): Acon used to trigger the heartbeat execution.
        heartbeat_control_table_name (str): Name of the control table to validate.
        ctrl_heartbeat_df (DataFrame): Reference DataFrame
            for asserting table contents.
        results (dict): Reference values to compare.
    """
    mocked_timestamp.return_value = lit(
        datetime.datetime.strptime("2025/08/14 23:00", "%Y/%m/%d %H:%M")
    ).cast(TimestampType())

    execute_sensor_heartbeat(acon=acon)
    heartbeat_result = ExecEnv.SESSION.table(f"{heartbeat_control_table_name}")

    assert (
        heartbeat_result.filter("status = 'NEW_EVENT_AVAILABLE'").count()
        == results["new_events_available_count"]
    )
    assert not DataframeHelpers.has_diff(ctrl_heartbeat_df, heartbeat_result)


@patch("lakehouse_engine.algorithms.sensors.heartbeat.current_timestamp")
@patch(
    "lakehouse_engine.core.sensor_manager.datetime",
)
def _test_update_heartbeat_sensor_status(
    mocked_timestamp_sensor: MagicMock,
    mocked_timestamp_heartbeat: MagicMock,
    heartbeat_control_table_name: str,
    sensor_table_name: str,
    job_id: str,
    ctrl_heartbeat_df: DataFrame,
    ctrl_sensor_df: DataFrame,
) -> None:
    """Test the update of sensor and heartbeat control table statuses.

    This test validates that the `update_heartbeat_sensor_status`
    function correctly updates timestamps and status fields in
    both the sensor and heartbeat control tables. It also
    compares the updated tables against expected control DataFrames.

    Args:
        mocked_timestamp_sensor (MagicMock): A static timestamp for testing
            sensor table.
        mocked_timestamp_heartbeat (MagicMock): A static timestamp for testing
            heartbeat table.
        heartbeat_control_table_name (str): Name of the heartbeat control
            table to validate.
        sensor_table_name (str): Name of the sensor table to validate.
        job_id (str): Job identifier used in the update process.
        ctrl_heartbeat_df (DataFrame): Expected state
            of the updated heartbeat control table.
        ctrl_sensor_df (DataFrame): Expected state
            of the updated sensor table.
    """
    mocked_timestamp_sensor.now.return_value = datetime.datetime(
        2025, 8, 14, 23, 00, 00, 00000
    )
    mocked_timestamp_heartbeat.return_value = lit(
        datetime.datetime.strptime("2025/08/14 23:00", "%Y/%m/%d %H:%M")
    ).cast(TimestampType())

    update_heartbeat_sensor_status(
        heartbeat_control_table_name, sensor_table_name, job_id
    )

    heartbeat_data = ExecEnv.SESSION.table(f"{heartbeat_control_table_name}")
    sensor_data = ExecEnv.SESSION.table(f"{sensor_table_name}")

    _LOGGER.info("Comparing heartbeat and sensor tables with control tables")
    assert not DataframeHelpers.has_diff(ctrl_sensor_df, sensor_data)

    assert not DataframeHelpers.has_diff(ctrl_heartbeat_df, heartbeat_data)


@patch(
    "lakehouse_engine.core.sensor_manager.SensorJobRunManager.run_job",
    MagicMock(return_value=("run_id", None)),
)
@patch("lakehouse_engine.algorithms.sensors.heartbeat.current_timestamp")
def _trigger_heartbeat_sensor_jobs(
    mocked_timestamp_heartbeat: MagicMock,
    acon: dict,
    heartbeat_control_table_name: str,
    heartbeat_control_table_updated: DataFrame,
) -> None:
    """Test the triggering of sensor heartbeat jobs.

    This test mocks the `run_job` method to simulate job execution,
    triggers the heartbeat sensor jobs, and verifies that the
    heartbeat control table reflects the expected changes.

    Args:
        mocked_timestamp_heartbeat (MagicMock): A static timestamp for testing
            heartbeat table.
        acon (dict): Acon used to trigger the sensor jobs.
        heartbeat_control_table_name (str): Name of the heartbeat control
            table to validate.
        heartbeat_control_table_updated (DataFrame): Expected state
            of the control table after job execution.
    """
    mocked_timestamp_heartbeat.return_value = lit(
        datetime.datetime.strptime("2025/08/14 23:00", "%Y/%m/%d %H:%M")
    ).cast(TimestampType())

    trigger_heartbeat_sensor_jobs(acon)

    heartbeat_table_job_run = ExecEnv.SESSION.table(f"{heartbeat_control_table_name}")
    assert not DataframeHelpers.has_diff(
        heartbeat_table_job_run, heartbeat_control_table_updated
    )


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "use_case_name": "default",
            "control_files": {
                "ctrl_heart_tbl_heartb_feed_fname": "ctr_heart_tbl_heartb_feed.csv",
                "ctrl_heart_tbl_exe_sns_hb_fname": "ctrl_heart_tbl_exec_sensor.csv",
                "ctrl_heart_tbl_updated_fname": "ctrl_heart_tbl_updated.csv",
                "ctrl_heart_tbl_trigger_job_fname": "ctrl_heart_tbl_trigger_job.csv",
                "ctrl_sensor_tbl_upd_status_fname": "ctrl_sensor_tbl_upd_status.json",
                "ctrl_heart_tbl_schema_fname": "ctrl_heart_tbl_schema.json",
            },
            "tables": {
                "heartbeat_sensor_control_table": "heartbeat_sensor_control_table",
                "sensor_table": "sensor_table",
            },
            "setup": {
                "setup_heartbeat_data": "setup_heartbeat_data.csv",
                "setup_sensor_data": "setup_sensor_data.json",
                "schema_sensor_df": "schema_sensor_df.json",
            },
            "execute_sensor_heartbeat_results": {"new_events_available_count": 1},
            "job_id": "1927384615203749",
            "trigger_heartbeat_sensor_jobs_records": {
                "heartbeat": """
                    ("delta_table","dummy_order","batch",
                    "dummy_heartbeat_asset",NULL,NULL,NULL,
                    "1015557820139870","data-product_job_name_orders","NEW_EVENT_AVAILABLE",
                    NULL,NULL,NULL,"UNPAUSED","true")""",
                "sensors": """
                    ("dummy_order",
                    array("dummy_heartbeat_asset"),"ACQUIRED_NEW_DATA",
                    NULL,NULL,"LOAD_DATE","10155578201985")""",
            },
        },
        {
            "use_case_name": "heartbeat_paused_sensor_new_record",
            "control_files": {
                "ctrl_heart_tbl_heartb_feed_fname": "ctr_heart_tbl_heartb_feed.csv",
                "ctrl_heart_tbl_exe_sns_hb_fname": "ctrl_heart_tbl_exec_sensor.csv",
                "ctrl_heart_tbl_updated_fname": "ctrl_heart_tbl_updated.csv",
                "ctrl_heart_tbl_trigger_job_fname": "ctrl_heart_tbl_trigger_job.csv",
                "ctrl_sensor_tbl_upd_status_fname": "ctrl_sensor_tbl_upd_status.json",
                "ctrl_heart_tbl_schema_fname": "ctrl_heart_tbl_schema.json",
            },
            "tables": {
                "heartbeat_sensor_control_table": "heartbeat_sensor_control_table",
                "sensor_table": "sensor_table",
            },
            "setup": {
                "setup_heartbeat_data": "setup_heartbeat_data.csv",
                "setup_sensor_data": "setup_sensor_data.json",
                "schema_sensor_df": "schema_sensor_df.json",
            },
            "execute_sensor_heartbeat_results": {"new_events_available_count": 0},
            "job_id": "2604918372561094",
            "trigger_heartbeat_sensor_jobs_records": {
                "heartbeat": """
                    ("delta_table","dummy_order","batch",
                    "dummy_heartbeat_asset",NULL,NULL,NULL,
                    "1015557820139870","data-product_job_name_orders","IN PROGRESS",
                    NULL,NULL,NULL,"UNPAUSED","true")""",
                "sensors": """
                    ("dummy_order",
                    array("dummy_heartbeat_asset"),"ACQUIRED_NEW_DATA",
                    NULL,NULL,"LOAD_DATE","10155578201985")""",
            },
        },
    ],
)
def test_heartbeat(scenario: dict) -> None:
    """Test the heartbeat feature.

    Tests the heartbeat feature by validating the four core
    functions invoked by the heartbeat algorithm.

    Args:
        scenario: The test scenario to execute.

    Scenarios:
        Default: A basic scenario that tests the four main steps of
        the Heartbeat algorithm:
            1. `execute_heartbeat_sensor_data_feed`: Loads a CSV file
                into an empty Heartbeat control table.
            2. `execute_sensor_heartbeat`: Simulates a Databricks job run.
                The return value is patched to avoid actual API calls.
            3. `update_heartbeat_sensor_status`: Updates values in the Heartbeat
                and Sensor tables.
            4. `trigger_heartbeat_sensor_jobs`: Triggers Databricks jobs.
            This function is also patched to prevent real job execution.
        Heartbeat_paused_sensor_new_record: Different state records that will
        have different behaviour.
            1. A record wih job_state = 'PAUSED' and  sensor_source = 'delta_table'
                is inserted into the `heartbeat` table.
                - Expected Behavior: No updates or changes throughout the test.
            2. A record wih job_state = 'Null' and  sensor_source = 'sap_bw' is
                inserted into heartbeat control table and sensor table.
               - Expected Behavior: Record is updated during the process to
                    reflect activity.
            3. A record wih job_state = 'COMPLETED' and  sensor_source = 'kafka'
                is inserted into heartbeat control table.
               - Expected Behavior:
                 - The record is updated during the process.
                 - A corresponding entry is created in the `sensor` table.
    """
    scenario_name = scenario["use_case_name"]
    _LOGGER.info(f"Setting up Test - {scenario_name}.")

    tables = scenario["tables"]
    control_files = scenario["control_files"]

    heartbeat_control_table_name = f"test_db.{tables['heartbeat_sensor_control_table']}"
    sensor_table_name = f"test_db.{tables['sensor_table']}"

    acon = {
        "heartbeat_sensor_db_table": heartbeat_control_table_name,
        "lakehouse_engine_sensor_db_table": sensor_table_name,
        "data_format": "delta",
        "sensor_source": "delta_table",
        "token": "my-token",
        "domain": "my-adidas-domain.cloud.databricks.com",
    }

    _create_heartbeat_table(scenario_name, tables)

    LocalStorage.copy_dir(
        f"{FEATURE_TEST_RESOURCES}/setup/{scenario_name}/data/",
        f"{TEST_LAKEHOUSE_IN}/{scenario_name}/data/",
    )

    LocalStorage.copy_dir(
        f"{FEATURE_TEST_RESOURCES}/control/{scenario_name}/data/",
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario_name}/data/",
    )

    setup_heartbeat_data_file_path = (
        f"{TEST_LAKEHOUSE_IN}/{scenario_name}/data/"
        f"{scenario['setup']['setup_heartbeat_data']}"
    )

    ctrl_heart_tbl_heartb_feed_fname = control_files["ctrl_heart_tbl_heartb_feed_fname"]
    ctrl_heart_tbl_heartb_feed_file_path = (
        f"{TEST_LAKEHOUSE_CONTROL}/"
        f"{scenario_name}/data/{ctrl_heart_tbl_heartb_feed_fname}"
    )

    ctrl_heart_tbl_schema_file_name = control_files["ctrl_heart_tbl_schema_fname"]
    ctrl_heart_tbl_schema_file_path = (
        f"file:///{FEATURE_TEST_RESOURCES}/control/"
        f"{scenario_name}/schema/{ctrl_heart_tbl_schema_file_name}"
    )

    ctrl_heartbeat_df = DataframeHelpers.read_from_file(
        ctrl_heart_tbl_heartb_feed_file_path,
        schema=SchemaUtils.from_file_to_dict(ctrl_heart_tbl_schema_file_path),
    )

    _test_heartbeat_sensor_data_feed(
        setup_heartbeat_data_file_path, heartbeat_control_table_name, ctrl_heartbeat_df
    )

    _LOGGER.info("Testing execute_sensor_heartbeat function")

    ctrl_heart_tbl_exe_sns_file_name = control_files["ctrl_heart_tbl_exe_sns_hb_fname"]
    ctrl_heart_tbl_exe_sns_file_path = (
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario_name}/"
        f"data/{ctrl_heart_tbl_exe_sns_file_name}"
    )
    ctrl_heart_tbl_exe_sns_df = DataframeHelpers.read_from_file(
        ctrl_heart_tbl_exe_sns_file_path,
        schema=SchemaUtils.from_file_to_dict(ctrl_heart_tbl_schema_file_path),
    )

    execute_sensor_results = scenario["execute_sensor_heartbeat_results"]

    _test_execute_sensor_heartbeat(
        acon=acon,
        heartbeat_control_table_name=heartbeat_control_table_name,
        ctrl_heartbeat_df=ctrl_heart_tbl_exe_sns_df,
        results=execute_sensor_results,
    )

    _LOGGER.info("Testing update_heartbeat_sensor_status function")

    sensor_df_schema = (
        f"file:///{FEATURE_TEST_RESOURCES}/setup/"
        f"{scenario_name}/schema/{scenario['setup']['schema_sensor_df']}"
    )

    ctrl_heart_table_upd = (
        f"{FEATURE_TEST_RESOURCES}/control/{scenario_name}/"
        f"data/{scenario['control_files']['ctrl_heart_tbl_updated_fname']}"
    )

    setup_sensor_file_name = scenario["setup"]["setup_sensor_data"]
    sensor_table_data_path = (
        f"{TEST_LAKEHOUSE_IN}/{scenario_name}/data/{setup_sensor_file_name}"
    )

    ctrl_sensor_tbl_upd_status_fname = control_files["ctrl_sensor_tbl_upd_status_fname"]
    ctrl_sensor_upd_path = (
        f"{TEST_LAKEHOUSE_CONTROL}/{scenario_name}/"
        f"data/{ctrl_sensor_tbl_upd_status_fname}"
    )

    sensors_data = DataframeHelpers.read_from_file(
        sensor_table_data_path,
        file_format="json",
        schema=SchemaUtils.from_file_to_dict(sensor_df_schema),
    )

    ctrl_sensor_upd_sensor_status_df = DataframeHelpers.read_from_file(
        ctrl_sensor_upd_path,
        file_format="json",
        schema=SchemaUtils.from_file_to_dict(sensor_df_schema),
    )

    ctrl_heart_tbl_df_upd_sns_status = DataframeHelpers.read_from_file(
        ctrl_heart_table_upd,
        schema=SchemaUtils.from_file_to_dict(ctrl_heart_tbl_schema_file_path),
    )

    sensors_data.write.format("delta").mode("overwrite").saveAsTable(sensor_table_name)

    job_id = scenario["job_id"]

    _test_update_heartbeat_sensor_status(
        heartbeat_control_table_name=heartbeat_control_table_name,
        sensor_table_name=sensor_table_name,
        job_id=job_id,
        ctrl_heartbeat_df=ctrl_heart_tbl_df_upd_sns_status,
        ctrl_sensor_df=ctrl_sensor_upd_sensor_status_df,
    )

    _LOGGER.info("Testing trigger_heartbeat_sensor_jobs function")

    _LOGGER.info(f"acon: {acon}")

    _LOGGER.info("Preparing heartbeat and sensor table")
    records_to_insert = scenario["trigger_heartbeat_sensor_jobs_records"]

    ExecEnv.SESSION.sql(
        f"""INSERT INTO {heartbeat_control_table_name}
            VALUES {records_to_insert["heartbeat"]}"""  # nosec
    )
    ExecEnv.SESSION.sql(
        f"""INSERT INTO {sensor_table_name}
        VALUES {records_to_insert["sensors"]}"""  # nosec
    )

    ctrl_heart_tbl_trig_job_fname = control_files["ctrl_heart_tbl_trigger_job_fname"]
    ctrl_heart_tbl_trig_job_path = (
        f"file:///{FEATURE_TEST_RESOURCES}/control/"
        f"{scenario_name}/data/{ctrl_heart_tbl_trig_job_fname}"
    )

    ctrl_heartbeat_update_df = DataframeHelpers.read_from_file(
        ctrl_heart_tbl_trig_job_path,
        schema=SchemaUtils.from_file_to_dict(ctrl_heart_tbl_schema_file_path),
    )

    _trigger_heartbeat_sensor_jobs(
        acon=acon,
        heartbeat_control_table_name=heartbeat_control_table_name,
        heartbeat_control_table_updated=ctrl_heartbeat_update_df,
    )

    for _, table_name in tables.items():
        LocalStorage.clean_folder(f"{LAKEHOUSE}{table_name}")
        ExecEnv.SESSION.sql(f"""DROP TABLE IF EXISTS test_db.{table_name}""")  # nosec
