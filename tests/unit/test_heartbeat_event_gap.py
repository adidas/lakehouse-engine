"""Test the heartbeat max_event_gap_hours option."""

from unittest.mock import Mock, patch

import pytest
from delta import DeltaTable
from pyspark.sql.functions import current_timestamp

from lakehouse_engine.algorithms.sensors.heartbeat import Heartbeat
from lakehouse_engine.core.definitions import HeartbeatConfigSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.engine import (
    trigger_heartbeat_sensor_jobs,
    update_heartbeat_sensor_status,
)
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.schema_utils import SchemaUtils
from tests.conftest import LAKEHOUSE, UNIT_RESOURCES
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_RESOURCES = f"{UNIT_RESOURCES}/heartbeat/heartbeat_anchor_job"
_LOGGER = LoggingHandler(__name__).get_logger()

HEARTBEAT_TABLE = "test_db.heartbeat_sensor_control_table"
SENSOR_TABLE = "test_db.sensor_table"
_SETUP_DELTA_TABLES = ["heartbeat_sensor_control_table", "sensor_table"]
JOB_ID = "151920838540073"


def _create_tables() -> None:
    """Create the heartbeat control table and the sensor table."""
    for table in _SETUP_DELTA_TABLES:
        DataframeHelpers.create_delta_table(
            cols=SchemaUtils.from_file_to_dict(
                f"file:///{TEST_RESOURCES}/setup/column_list/{table}.json"
            ),
            table=table,
        )


def _drop_tables() -> None:
    """Drop the tables created for the test."""
    for table in _SETUP_DELTA_TABLES:
        LocalStorage.clean_folder(f"{LAKEHOUSE}{table}")
        ExecEnv.SESSION.sql(f"DROP TABLE IF EXISTS test_db.{table}")  # nosec


def _heartbeat_record(
    sensor_id: str,
    status: str = "NEW_EVENT_AVAILABLE",
    job_start_timestamp: str = "NULL",
) -> str:
    """Build a control table record for the CADP job."""
    return (
        f'("trigger_file","{sensor_id}","streaming","{sensor_id}",NULL,NULL,NULL,'
        f'"{JOB_ID}","product_crp_dtc","{status}",NULL,{job_start_timestamp},NULL,'
        '"UNPAUSED","TRUE")'
    )


def _sensor_record(sensor_id: str, event_time: str) -> str:
    """Build a sensor table record that acquired new data at event_time."""
    sensor_table_id = f"{sensor_id}_{JOB_ID}"
    return (
        f'("{sensor_table_id}",array("{sensor_table_id}"),"ACQUIRED_NEW_DATA",'
        f"TIMESTAMP'{event_time}',NULL,NULL,NULL)"
    )


def _string_sensor_record(sensor_id: str, event_time: str) -> str:
    """Build a sensor table record with the event time as a raw string."""
    sensor_table_id = f"{sensor_id}_{JOB_ID}"
    return (
        f'("{sensor_table_id}",array("{sensor_table_id}"),"ACQUIRED_NEW_DATA",'
        f'"{event_time}",NULL,NULL,NULL)'
    )


def _get_acon(max_event_gap_hours: int | None) -> dict:
    """Get the heartbeat acon for the trigger_file sensor source."""
    return {
        "heartbeat_sensor_db_table": HEARTBEAT_TABLE,
        "lakehouse_engine_sensor_db_table": SENSOR_TABLE,
        "data_format": "cloudfiles",
        "sensor_source": "trigger_file",
        "token": "my-token",
        "domain": "adidas-domain.cloud.databricks.com",
        "max_event_gap_hours": max_event_gap_hours,
    }


def _get_sensor_statuses() -> dict:
    """Get the status of each sensor in the sensor table."""
    return {
        row["sensor_id"]: row["status"]
        for row in ExecEnv.SESSION.table(SENSOR_TABLE).collect()
    }


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "use_case_name": "stale_event_not_triggered",
            "events": {
                "pgr_a971_data": "2026-08-21 05:30:00",
                "pgr_a972_data": "2026-08-21 04:30:00",
                "s4_pricing": "2026-08-21 05:30:00",
                "als_pricing_data": "2026-08-20 19:00:00",
            },
            "max_event_gap_hours": 6,
            "jobs_triggered_count": 0,
        },
        {
            "use_case_name": "same_cycle_events_triggered",
            "events": {
                "pgr_a971_data": "2026-08-21 05:30:00",
                "pgr_a972_data": "2026-08-21 04:30:00",
                "s4_pricing": "2026-08-21 05:30:00",
                "als_pricing_data": "2026-08-21 06:00:00",
            },
            "max_event_gap_hours": 6,
            "jobs_triggered_count": 1,
        },
        {
            "use_case_name": "gap_not_configured_triggered",
            "events": {
                "pgr_a971_data": "2026-08-21 05:30:00",
                "pgr_a972_data": "2026-08-21 04:30:00",
                "s4_pricing": "2026-08-21 05:30:00",
                "als_pricing_data": "2026-08-20 19:00:00",
            },
            "max_event_gap_hours": None,
            "jobs_triggered_count": 1,
        },
        {
            "use_case_name": "single_dependency_triggered",
            "events": {"als_pricing_data": "2026-08-20 19:00:00"},
            "max_event_gap_hours": 6,
            "jobs_triggered_count": 1,
        },
        {
            "use_case_name": "delayed_upstream_not_triggered",
            "events": {
                "pgr_a971_data": "2026-07-31 12:11:00",
                "pgr_a972_data": "2026-07-31 06:40:00",
                "s4_pricing": "2026-07-31 05:17:00",
                "als_pricing_data": "2026-07-31 05:25:00",
            },
            "max_event_gap_hours": 6,
            "jobs_triggered_count": 0,
        },
        {
            "use_case_name": "delayed_upstream_triggered_with_bigger_gap",
            "events": {
                "pgr_a971_data": "2026-07-31 12:11:00",
                "pgr_a972_data": "2026-07-31 06:40:00",
                "s4_pricing": "2026-07-31 05:17:00",
                "als_pricing_data": "2026-07-31 05:25:00",
            },
            "max_event_gap_hours": 8,
            "jobs_triggered_count": 1,
        },
    ],
)
@patch(
    "lakehouse_engine.core.sensor_manager.SensorJobRunManager.run_job",
    return_value=("run_id", None),
)
def test_event_gap(mock_run_job: Mock, scenario: dict) -> None:
    """Test if jobs are only triggered when events belong to the same cycle.

    Args:
        mock_run_job (Mock): The mocked Databricks job trigger.
        scenario: The test scenario to execute.

    Scenarios:
        1- Stale als event (10h30 gap) with max_event_gap_hours = 6: not triggered.
        2- New als event (1h30 gap) with max_event_gap_hours = 6: triggered.
        3- Stale als event without max_event_gap_hours: triggered (default).
        4- Single dependency with max_event_gap_hours = 6: triggered.
        5- Delayed upstream (6h54 gap) with max_event_gap_hours = 6: not triggered.
        6- Delayed upstream (6h54 gap) with max_event_gap_hours = 8: triggered.
    """
    _LOGGER.info(f"Scenario: {scenario['use_case_name']}")
    _create_tables()

    events = scenario["events"]
    heartbeat_records = ",".join(_heartbeat_record(s) for s in events)
    sensor_records = ",".join(_sensor_record(s, t) for s, t in events.items())
    ExecEnv.SESSION.sql(
        f"INSERT INTO {HEARTBEAT_TABLE} VALUES {heartbeat_records}"  # noqa: S608
    )
    ExecEnv.SESSION.sql(
        f"INSERT INTO {SENSOR_TABLE} VALUES {sensor_records}"  # noqa: S608
    )

    trigger_heartbeat_sensor_jobs(acon=_get_acon(scenario["max_event_gap_hours"]))

    assert mock_run_job.call_count == scenario["jobs_triggered_count"]

    _drop_tables()


@pytest.mark.parametrize(
    "max_event_gap_hours, expected_records",
    [(None, 0), (6, 4)],
)
def test_active_jobs_include_new_events(
    max_event_gap_hours: int | None, expected_records: int
) -> None:
    """Test if NEW_EVENT_AVAILABLE records are checked only with max_event_gap_hours.

    Args:
        max_event_gap_hours: max hours between events of the same cycle.
        expected_records: number of records the heartbeat should check.
    """
    _create_tables()

    sensor_ids = ["pgr_a971_data", "pgr_a972_data", "s4_pricing", "als_pricing_data"]
    heartbeat_records = ",".join(_heartbeat_record(s) for s in sensor_ids)
    ExecEnv.SESSION.sql(
        f"INSERT INTO {HEARTBEAT_TABLE} VALUES {heartbeat_records}"  # noqa: S608
    )

    active_jobs_df = Heartbeat._get_active_heartbeat_jobs(
        DeltaTable.forName(ExecEnv.SESSION, HEARTBEAT_TABLE),
        "trigger_file",
        max_event_gap_hours,
    )

    assert active_jobs_df.count() == expected_records

    _drop_tables()


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "use_case_name": "late_event_kept",
            "heartbeat_status": "IN_PROGRESS",
            "events": {
                "pgr_a971_data": "2026-08-21 09:10:00",
                "als_pricing_data": "2026-08-21 09:21:00",
            },
            "expected_sensor_status": {
                "pgr_a971_data": "PROCESSED_NEW_DATA",
                "als_pricing_data": "ACQUIRED_NEW_DATA",
            },
        },
        {
            "use_case_name": "manual_run_all_processed",
            "heartbeat_status": "NEW_EVENT_AVAILABLE",
            "events": {
                "pgr_a971_data": "2026-08-21 09:10:00",
                "als_pricing_data": "2026-08-21 09:21:00",
            },
            "expected_sensor_status": {
                "pgr_a971_data": "PROCESSED_NEW_DATA",
                "als_pricing_data": "PROCESSED_NEW_DATA",
            },
        },
    ],
)
def test_completion_keeps_late_events(scenario: dict) -> None:
    """Test if events acquired while the job was running are kept.

    Args:
        scenario: The test scenario to execute.

    Scenarios:
        1- Job triggered by the heartbeat (IN_PROGRESS) started at 09:20: the event
            acquired at 09:21 keeps ACQUIRED_NEW_DATA, the one at 09:10 is processed.
        2- Manual run (records not IN_PROGRESS): all events are processed (default).
    """
    _LOGGER.info(f"Scenario: {scenario['use_case_name']}")
    _create_tables()

    events = scenario["events"]
    heartbeat_records = ",".join(
        _heartbeat_record(
            s, scenario["heartbeat_status"], "TIMESTAMP'2026-08-21 09:20:00'"
        )
        for s in events
    )
    sensor_records = ",".join(_sensor_record(s, t) for s, t in events.items())
    ExecEnv.SESSION.sql(
        f"INSERT INTO {HEARTBEAT_TABLE} VALUES {heartbeat_records}"  # noqa: S608
    )
    ExecEnv.SESSION.sql(
        f"INSERT INTO {SENSOR_TABLE} VALUES {sensor_records}"  # noqa: S608
    )

    update_heartbeat_sensor_status(HEARTBEAT_TABLE, SENSOR_TABLE, JOB_ID)

    sensor_statuses = _get_sensor_statuses()
    for sensor_id, expected_status in scenario["expected_sensor_status"].items():
        assert sensor_statuses[f"{sensor_id}_{JOB_ID}"] == expected_status

    heartbeat_statuses = {
        row["status"] for row in ExecEnv.SESSION.table(HEARTBEAT_TABLE).collect()
    }
    assert heartbeat_statuses == {"COMPLETED"}

    _drop_tables()


def test_new_event_does_not_overwrite_in_progress() -> None:
    """Test if a new event does not overwrite a record whose job is running."""
    _create_tables()

    in_progress_record = _heartbeat_record("als_pricing_data", "IN_PROGRESS")
    ExecEnv.SESSION.sql(
        f"INSERT INTO {HEARTBEAT_TABLE} VALUES {in_progress_record}"  # noqa: S608
    )

    Heartbeat._update_heartbeat_status_with_sensor_info(
        ExecEnv.SESSION.table(HEARTBEAT_TABLE),
        DeltaTable.forName(ExecEnv.SESSION, HEARTBEAT_TABLE),
        f"(sensor_id = 'als_pricing_data' AND trigger_job_id = '{JOB_ID}')",
        current_timestamp(),
        "trigger_file",
    )

    records = ExecEnv.SESSION.table(HEARTBEAT_TABLE).collect()
    assert len(records) == 1
    assert records[0]["status"] == "IN_PROGRESS"

    _drop_tables()


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "use_case_name": "string_stale_event_not_triggered",
            "als_event_time": "2026-08-20 19:00:00.123456",
            "jobs_triggered_count": 0,
        },
        {
            "use_case_name": "string_same_cycle_events_triggered",
            "als_event_time": "2026-08-21 06:00:00.123456",
            "jobs_triggered_count": 1,
        },
    ],
)
@patch(
    "lakehouse_engine.core.sensor_manager.SensorJobRunManager.run_job",
    return_value=("run_id", None),
)
def test_event_gap_with_string_timestamps(mock_run_job: Mock, scenario: dict) -> None:
    """Test the event gap when the sensor table stores timestamps as STRING.

    The sensors table DDL of the data product templates defines the
    status_change_timestamp column as STRING, and the engine writes it with
    microseconds.

    Args:
        mock_run_job (Mock): The mocked Databricks job trigger.
        scenario: The test scenario to execute.

    Scenarios:
        1- Stale als event (10h30 gap) with max_event_gap_hours = 6: not triggered.
        2- New als event (1h30 gap) with max_event_gap_hours = 6: triggered.
    """
    _LOGGER.info(f"Scenario: {scenario['use_case_name']}")

    DataframeHelpers.create_delta_table(
        cols=SchemaUtils.from_file_to_dict(
            f"file:///{TEST_RESOURCES}/setup/column_list/"
            "heartbeat_sensor_control_table.json"
        ),
        table="heartbeat_sensor_control_table",
    )
    sensor_table_cols = SchemaUtils.from_file_to_dict(
        f"file:///{TEST_RESOURCES}/setup/column_list/sensor_table.json"
    )
    sensor_table_cols["status_change_timestamp"] = "string"
    DataframeHelpers.create_delta_table(cols=sensor_table_cols, table="sensor_table")

    events = {
        "pgr_a971_data": "2026-08-21 05:30:00.123456",
        "pgr_a972_data": "2026-08-21 04:30:00.123456",
        "s4_pricing": "2026-08-21 05:30:00.123456",
        "als_pricing_data": scenario["als_event_time"],
    }
    heartbeat_records = ",".join(_heartbeat_record(s) for s in events)
    sensor_records = ",".join(_sensor_record(s, t) for s, t in events.items())
    ExecEnv.SESSION.sql(
        f"INSERT INTO {HEARTBEAT_TABLE} VALUES {heartbeat_records}"  # noqa: S608
    )
    ExecEnv.SESSION.sql(
        f"INSERT INTO {SENSOR_TABLE} VALUES {sensor_records}"  # noqa: S608
    )

    trigger_heartbeat_sensor_jobs(acon=_get_acon(6))

    assert mock_run_job.call_count == scenario["jobs_triggered_count"]

    _drop_tables()


@pytest.mark.parametrize("max_event_gap_hours", ["6", -1, 0, 1.5, True])
def test_invalid_max_event_gap_hours(max_event_gap_hours: object) -> None:
    """Test if an invalid max_event_gap_hours is rejected.

    Args:
        max_event_gap_hours: invalid value for max_event_gap_hours.
    """
    acon = {**_get_acon(None), "max_event_gap_hours": max_event_gap_hours}

    with pytest.raises(ValueError, match="max_event_gap_hours"):
        HeartbeatConfigSpec.create_from_acon(acon)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "use_case_name": "missing_sensor_record_not_triggered",
            "als_event_time": None,
        },
        {
            "use_case_name": "invalid_event_time_not_triggered",
            "als_event_time": "not a timestamp",
        },
    ],
)
@patch(
    "lakehouse_engine.core.sensor_manager.SensorJobRunManager.run_job",
)
def test_event_gap_without_valid_event_time(mock_run_job: Mock, scenario: dict) -> None:
    """Test if the job is not triggered when a hard dependency has no valid event time.

    Args:
        mock_run_job (Mock): The mocked Databricks job trigger.
        scenario: The test scenario to execute.

    Scenarios:
        1- als has no record in the sensors table: not triggered.
        2- als has a status_change_timestamp that can't be parsed: not triggered.
    """
    _LOGGER.info(f"Scenario: {scenario['use_case_name']}")

    DataframeHelpers.create_delta_table(
        cols=SchemaUtils.from_file_to_dict(
            f"file:///{TEST_RESOURCES}/setup/column_list/"
            "heartbeat_sensor_control_table.json"
        ),
        table="heartbeat_sensor_control_table",
    )
    sensor_table_cols = SchemaUtils.from_file_to_dict(
        f"file:///{TEST_RESOURCES}/setup/column_list/sensor_table.json"
    )
    sensor_table_cols["status_change_timestamp"] = "string"
    DataframeHelpers.create_delta_table(cols=sensor_table_cols, table="sensor_table")

    events = {
        "pgr_a971_data": "2026-08-21 05:30:00.123456",
        "als_pricing_data": scenario["als_event_time"],
    }
    heartbeat_records = ",".join(_heartbeat_record(s) for s in events)
    sensor_records = ",".join(
        _string_sensor_record(s, t) for s, t in events.items() if t is not None
    )
    ExecEnv.SESSION.sql(
        f"INSERT INTO {HEARTBEAT_TABLE} VALUES {heartbeat_records}"  # noqa: S608
    )
    ExecEnv.SESSION.sql(
        f"INSERT INTO {SENSOR_TABLE} VALUES {sensor_records}"  # noqa: S608
    )

    trigger_heartbeat_sensor_jobs(acon=_get_acon(6))

    assert mock_run_job.call_count == 0

    _drop_tables()
