"""Module that tests the anchor job function from the heartbeat module."""

from unittest.mock import Mock, patch

import pytest

from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.engine import trigger_heartbeat_sensor_jobs
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.schema_utils import SchemaUtils
from tests.conftest import LAKEHOUSE, UNIT_RESOURCES
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_NAME = "heartbeat_anchor_job"
FEATURE_TEST_RESOURCES = f"{UNIT_RESOURCES}/heartbeat/{TEST_NAME}"
_LOGGER = LoggingHandler(__name__).get_logger()

_SETUP_DELTA_TABLES = ["heartbeat_sensor_control_table"]


def _create_heartbeat_table() -> None:
    """Create the necessary tables required for using Heartbeat."""
    _LOGGER.info("Creating tables")
    for table in _SETUP_DELTA_TABLES:
        DataframeHelpers.create_delta_table(
            cols=SchemaUtils.from_file_to_dict(
                f"file:///{FEATURE_TEST_RESOURCES}/setup/column_list/{table}.json"
            ),
            table=table,
        )


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "use_case_name": "delta_table_trigger_2_jobs",
            "sensor_source": "delta_table",
            "trigger_jobs_records": {
                "heartbeat": """
                    ("delta_table","dummy_orders","batch",
                    "delta_table_order_events",NULL,NULL,NULL,
                    "3849201756384721","events_orders","NEW_EVENT_AVAILABLE",
                    NULL,NULL,NULL,"UNPAUSED","TRUE"),
                    ("delta_table","dummy_sales","batch",
                    "delta_table_order_events",NULL,NULL,NULL,
                    "3849201756384721","events_orders","NEW_EVENT_AVAILABLE",
                    NULL,NULL,NULL,"UNPAUSED","TRUE"),
                    ("delta_table","dummy_test","batch",
                    "delta_table_order_events",NULL,NULL,NULL,
                    "7601938475620193","events_orders","NEW_EVENT_AVAILABLE",
                    NULL,NULL,NULL,"UNPAUSED","TRUE"),
                    ("delta_table","dummy_test2","batch",
                    "delta_table_order_events",NULL,NULL,NULL,
                    "7601938475620193","events_orders","NEW_EVENT_AVAILABLE",
                    NULL,NULL,NULL,"UNPAUSED","TRUE")
                    """,
            },
            "jobs_triggered_count": 2,
            "job_id": ["3849201756384721", "7601938475620193"],
        },
        {
            "use_case_name": "kafka_trigger_1_job",
            "sensor_source": "kafka",
            "trigger_jobs_records": {
                "heartbeat": """
                    ("kafka","dummy_test3","batch",
                    "delta_table_order_events",NULL,NULL,NULL,
                    "5918374620193847","events_orders","COMPLETE",
                    NULL,NULL,NULL,"UNPAUSED","FALSE"),
                    ("kafka","dummy_test4","batch",
                    "delta_table_order_events",NULL,NULL,NULL,
                    "5918374620193847","events_orders","NEW_EVENT_AVAILABLE",
                    NULL,NULL,NULL,"UNPAUSED","TRUE")
                    """,
            },
            "jobs_triggered_count": 1,
            "job_id": ["5918374620193847"],
        },
        {
            "use_case_name": "sap_b4_no_trigger",
            "sensor_source": "sap_b4",
            "trigger_jobs_records": {
                "heartbeat": """
                    ("sap_b4","dummy_test3","batch",
                    "delta_table_order_events",NULL,NULL,NULL,
                    "8203746159283746","events_orders","NEW_EVENT_AVAILABLE",
                    NULL,NULL,NULL,"PAUSED","FALSE"),
                    ("sap_b4","dummy_test4","batch",
                    "delta_table_order_events",NULL,NULL,NULL,
                    "8203746159283746","events_orders","COMPLETE",
                    NULL,NULL,NULL,"UNPAUSED","TRUE")
                    """
            },
            "jobs_triggered_count": 0,
        },
    ],
)
@patch(
    "lakehouse_engine.core.sensor_manager.SensorJobRunManager.run_job",
    return_value=("run_id", None),
)
def test_anchor_job(mock_run_job: Mock, scenario: dict) -> None:
    """Test the number of jobs triggered.

    Args:
        mock_run_job (Mock): The mocked object.
        scenario: The test scenario to execute.

    Scenarios:
        1- 2 different jobs id's each one with two hard dependencies.
            From the 4 records in the table, only two should trigger a job.
        2- 1 job id with two records that can trigger the job.
            Only 1 comply with the specifications to trigger a job.
        3- 1 job id with two records that can trigger the job.
            None comply with the specifications to trigger a job.
    """
    scenario_name = scenario["use_case_name"]
    sensor_source = scenario["sensor_source"]
    records = scenario["trigger_jobs_records"].get("heartbeat")
    jobs_triggered_count = scenario["jobs_triggered_count"]

    heartbeat_table = "test_db.heartbeat_sensor_control_table"
    sensor_table = "test_db.sensor_table"

    acon = {
        "heartbeat_sensor_db_table": heartbeat_table,
        "lakehouse_engine_sensor_db_table": sensor_table,
        "data_format": "delta",
        "sensor_source": sensor_source,
        "token": "my-token",
        "domain": "adidas-domain.cloud.databricks.com",
    }

    _LOGGER.info(f"Scenario: {scenario_name}")

    _create_heartbeat_table()

    ExecEnv.SESSION.sql(
        f"""INSERT INTO {heartbeat_table}
            VALUES {records}"""  # nosec
    )

    trigger_heartbeat_sensor_jobs(acon=acon)
    assert mock_run_job.call_count == jobs_triggered_count

    if jobs_triggered_count > 0:
        triggered_job_id = scenario["job_id"]
        for call_args in mock_run_job.call_args_list:
            assert call_args[0][0] in triggered_job_id

    for table in _SETUP_DELTA_TABLES:
        LocalStorage.clean_folder(f"{LAKEHOUSE}{table}")
        ExecEnv.SESSION.sql(f"""DROP TABLE IF EXISTS test_db.{table}""")  # nosec
