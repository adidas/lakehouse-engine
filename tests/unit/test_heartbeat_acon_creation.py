"""Module that tests the Acon creation function from the heartbeat module."""

from unittest.mock import Mock, patch

import pytest
from pyspark.sql import DataFrame

from lakehouse_engine.algorithms.sensors.heartbeat import Heartbeat
from lakehouse_engine.core.definitions import HeartbeatConfigSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.schema_utils import SchemaUtils
from tests.conftest import LAKEHOUSE, UNIT_RESOURCES
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_NAME = "heartbeat_acon_creation"
FEATURE_TEST_RESOURCES = f"{UNIT_RESOURCES}/heartbeat/{TEST_NAME}"
_LOGGER = LoggingHandler(__name__).get_logger()

_SETUP_DELTA_TABLES = ["heartbeat_sensor_control_table", "sensor_table"]


def _create_heartbeat_table() -> None:
    """Create the necessary tables required for using Heartbeat."""
    _LOGGER.info("Creating heartbeat tables")
    for table in _SETUP_DELTA_TABLES:
        DataframeHelpers.create_delta_table(
            cols=SchemaUtils.from_file_to_dict(
                f"file:///{FEATURE_TEST_RESOURCES}/setup/column_list/{table}.json"
            ),
            table=table,
        )


def _select_all(table: str) -> DataFrame:
    """Select all records from the specified table.

    Args:
        table (str): The name of the table.
    """
    return ExecEnv.SESSION.sql(f"SELECT * FROM  {table} ORDER BY sensor_id")  # nosec


def _check_acon(heartbeat_table: str, acon: dict, acon_result_list: dict) -> None:
    """Validates the generated ACON.

    Args:
        heartbeat_table (str): The name of the heartbeat control table.
        acon (dict): The initial ACON that feeds the heartbeat algorithm.
        acon_result_list (dict): The expected ACON configuration.
    """
    _LOGGER.info("Checking acon creation.")
    for control_table_row in _select_all(heartbeat_table).collect():
        result = Heartbeat._get_sensor_acon_from_heartbeat(
            HeartbeatConfigSpec.create_from_acon(acon), control_table_row
        )
        print(result)

        assert result == acon_result_list[control_table_row["sensor_id"]]


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "use_case_name": "delta_table",
            "rows_to_add": {
                "heartbeat": """
                    ("delta_table","dummy_order","batch",
                    "delta_table_order_events",NULL,NULL,NULL,
                    "9274610384726150","dummy_order_events","COMPLETED",
                    NULL,NULL,NULL,"UNPAUSED","TRUE")
                    """,
            },
            "results": {
                "dummy_order": {
                    "sensor_id": "dummy_order_9274610384726150",
                    "assets": ["delta_table_order_events_9274610384726150"],
                    "control_db_table_name": "test_db.sensor_table",
                    "input_spec": {
                        "spec_id": "sensor_upstream",
                        "read_type": "batch",
                        "data_format": "delta",
                        "db_table": "dummy_order",
                        "options": None,
                        "location": None,
                        "schema": None,
                    },
                    "preprocess_query": None,
                    "base_checkpoint_location": None,
                    "fail_on_empty_result": False,
                },
            },
        },
        {
            "use_case_name": "kafka",
            "rows_to_add": {
                "heartbeat": """
                    ("kafka",
                    "sales: sales.dummy_deliveries",
                    "batch","delta_table_order_events",NULL,NULL,NULL,
                    "1847362093847561","dummy_order_events","COMPLETED",
                    NULL,NULL,NULL,"UNPAUSED","TRUE")
                    """,
            },
            "results": {
                "sales: sales.dummy_deliveries": {
                    "sensor_id": "sales__sales_dummy_deliveries_1847362093847561",
                    "assets": ["delta_table_order_events_1847362093847561"],
                    "control_db_table_name": "test_db.sensor_table",
                    "input_spec": {
                        "spec_id": "sensor_upstream",
                        "read_type": "batch",
                        "data_format": "kafka",
                        "db_table": None,
                        "options": {
                            "kafka.bootstrap.servers": ["server1", "server2"],
                            "subscribe": "sales.dummy_deliveries",
                            "startingOffsets": "earliest",
                            "kafka.security.protocol": "SSL",
                            "kafka.ssl.truststore.location": "trust_store_location",
                            "kafka.ssl.truststore.password": "key",
                            "kafka.ssl.keystore.location": "keystore_location",
                            "kafka.ssl.keystore.password": "key",
                        },
                        "location": None,
                        "schema": None,
                    },
                    "preprocess_query": None,
                    "base_checkpoint_location": None,
                    "fail_on_empty_result": False,
                }
            },
        },
        {
            "use_case_name": "sap_b4",
            "rows_to_add": {
                "heartbeat": """
                    ("sap_b4","SAP_DUMMY_ID","batch",
                    "dummy_tables","LOAD_DATE",NULL,NULL,
                    "6039184726153847","dummy_order_events","COMPLETED",
                    NULL,NULL,NULL,"UNPAUSED","FALSE"),
                    ("sap_b4","SAP_DUMMY_ID2","batch",
                    "dummy_tables","LOAD_DATE",NULL,NULL,
                    "7482910364728193","dummy_order_events","COMPLETED",
                    NULL,NULL,NULL,"UNPAUSED","FALSE")
                    """,
            },
            "results": {
                "SAP_DUMMY_ID": {
                    "sensor_id": "SAP_DUMMY_ID_6039184726153847",
                    "assets": ["dummy_tables_6039184726153847"],
                    "control_db_table_name": "test_db.sensor_table",
                    "input_spec": {
                        "spec_id": "sensor_upstream",
                        "read_type": "batch",
                        "data_format": "sap_b4",
                        "db_table": None,
                        "options": {
                            "prepareQuery": (
                                "WITH sensor_new_data AS (SELECT CHAIN_ID, "
                                "CONCAT(DATUM, ZEIT) AS LOAD_DATE, ANALYZED_STATUS "
                                "FROM sap_table "
                                "WHERE UPPER(CHAIN_ID) = UPPER('SAP_DUMMY_ID') "
                                "AND UPPER(ANALYZED_STATUS) = UPPER('G'))"
                            ),
                            "query": (
                                "SELECT COUNT(1) as count, "
                                "'LOAD_DATE' as UPSTREAM_KEY, "
                                "max(LOAD_DATE) as UPSTREAM_VALUE FROM sensor_new_data "
                                "WHERE LOAD_DATE > '19000101000000' HAVING COUNT(1) > 0"
                            ),
                        },
                        "location": None,
                        "schema": None,
                    },
                    "preprocess_query": None,
                    "base_checkpoint_location": None,
                    "fail_on_empty_result": False,
                },
                "SAP_DUMMY_ID2": {
                    "sensor_id": "SAP_DUMMY_ID2_7482910364728193",
                    "assets": ["dummy_tables_7482910364728193"],
                    "control_db_table_name": "test_db.sensor_table",
                    "input_spec": {
                        "spec_id": "sensor_upstream",
                        "read_type": "batch",
                        "data_format": "sap_b4",
                        "db_table": None,
                        "options": {
                            "prepareQuery": (
                                "WITH sensor_new_data AS (SELECT CHAIN_ID, "
                                "CONCAT(DATUM, ZEIT) AS LOAD_DATE, ANALYZED_STATUS "
                                "FROM sap_table "
                                "WHERE "
                                "UPPER(CHAIN_ID) = UPPER('SAP_DUMMY_ID2') "
                                "AND UPPER(ANALYZED_STATUS) = UPPER('G'))"
                            ),
                            "query": (
                                "SELECT COUNT(1) as count, "
                                "'LOAD_DATE' as UPSTREAM_KEY, "
                                "max(LOAD_DATE) as UPSTREAM_VALUE FROM sensor_new_data "
                                "WHERE LOAD_DATE > '19000101000000' HAVING COUNT(1) > 0"
                            ),
                        },
                        "location": None,
                        "schema": None,
                    },
                    "preprocess_query": None,
                    "base_checkpoint_location": None,
                    "fail_on_empty_result": False,
                },
            },
        },
    ],
)
@patch("lakehouse_engine.utils.databricks_utils.DatabricksUtils.get_db_utils")
def test_get_sensor_acon(mock_get_db_utils: Mock, scenario: dict) -> None:
    """Test the acon creation.

    Args:
        mock_get_db_utils (Mock): The mocked object.
        scenario (dict): The test scenario to execute.

    Scenarios:
        1- For delta tables source.
        2- For kafka topics source.
        3- For SAP sources. In this scenario we have two records
            that will yield two different acons.
    """
    scenario_name = scenario["use_case_name"]
    records = scenario["rows_to_add"].get("heartbeat")
    acon_result_list = scenario["results"]

    heartbeat_table = "test_db.heartbeat_sensor_control_table"
    sensor_table = "test_db.sensor_table"

    acon = {
        "sensor_source": scenario_name,
        "data_format": "delta",
        "heartbeat_sensor_db_table": heartbeat_table,
        "lakehouse_engine_sensor_db_table": sensor_table,
        "token": "my-token",
        "domain": "adidas-domain.cloud.databricks.com",
    }

    _LOGGER.info(f"Scenario: {scenario_name}")

    _create_heartbeat_table()

    _LOGGER.info("Inserting records in heartbeat table.")
    ExecEnv.SESSION.sql(
        f"""INSERT INTO {heartbeat_table}
            VALUES {records}"""  # nosec
    )

    if scenario_name == "sap_b4":
        _LOGGER.info("Inserting records in sensors table.")
        acon.update(
            {
                "data_format": "sap_b4",
                "jdbc_db_table": "sap_table",
                "options": {
                    "prepareQuery": "",
                    "query": "",
                },
            }
        )

    if scenario_name == "kafka":
        acon.update(
            {
                "data_format": "kafka",
                "kafka_configs": {
                    "sales": {
                        "kafka_bootstrap_servers_list": ["server1", "server2"],
                        "kafka_ssl_truststore_location": "trust_store_location",
                        "kafka_ssl_keystore_location": "keystore_location",
                        "truststore_pwd_secret_key": "trust_store_key",
                        "keystore_pwd_secret_key": "keystore_pwd_secret_key",
                    }
                },
            }
        )

        mock_db_utils = Mock()
        mock_secrets = Mock()
        mock_secrets.get.return_value = "key"
        mock_db_utils.secrets = mock_secrets
        mock_get_db_utils.return_value = mock_db_utils

        _check_acon(heartbeat_table, acon, acon_result_list)
    else:
        _check_acon(heartbeat_table, acon, acon_result_list)

    for table in _SETUP_DELTA_TABLES:
        LocalStorage.clean_folder(f"{LAKEHOUSE}{table}")
        ExecEnv.SESSION.sql(f"""DROP TABLE IF EXISTS test_db.{table}""")  # nosec
