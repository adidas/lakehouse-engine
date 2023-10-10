"""Module with unit tests for Sensor Manager module."""

from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    Row,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse_engine.algorithms.sensor import SensorStatus
from lakehouse_engine.core.definitions import SensorSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.core.sensor_manager import (
    SensorControlTableManager,
    SensorUpstreamManager,
)
from lakehouse_engine.io.reader_factory import ReaderFactory

TEST_DEFAULT_DATETIME = datetime(2023, 5, 26, 14, 38, 16, 676508)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "should_return_default_update_set_when_empty_fields",
            "updated_set_to_add": {},
        },
        {
            "scenario_name": "should_add_just_one_field_to_update_set",
            "assets": ["asset_1"],
            "updated_set_to_add": {"sensors.assets": "updates.assets"},
        },
        {
            "scenario_name": "should_add_multiple_fields_to_update_set",
            "assets": ["asset_1"],
            "checkpoint_location": "s3://dummy-bucket/sensors/sensor_id_1",
            "upstream_key": "dummy_column",
            "upstream_value": "dummy_value",
            "updated_set_to_add": {
                "sensors.assets": "updates.assets",
                "sensors.checkpoint_location": "updates.checkpoint_location",
                "sensors.upstream_key": "updates.upstream_key",
                "sensors.upstream_value": "updates.upstream_value",
            },
        },
    ],
)
def test_sensor_update_set(scenario: dict, capsys: Any) -> None:
    """Test sensor update set adding multiple fields based in the items to add.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """
    expected_default_update_set = {
        "sensors.sensor_id": "updates.sensor_id",
        "sensors.status": "updates.status",
        "sensors.status_change_timestamp": "updates.status_change_timestamp",
    }

    subject = SensorControlTableManager._get_sensor_update_set(
        assets=scenario.get("assets"),
        checkpoint_location=scenario.get("checkpoint_location"),
        upstream_key=scenario.get("upstream_key"),
        upstream_value=scenario.get("upstream_value"),
    )

    assert subject == {**expected_default_update_set, **scenario["updated_set_to_add"]}


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "true_when_table_data_and_status_acquired_new_data",
            "sensor_id": "sensor_id_1",
            "assets": ["asset_1"],
            "status": SensorStatus.ACQUIRED_NEW_DATA.value,
            "status_change_timestamp": datetime.now(),
            "checkpoint_location": "s3://dummy-bucket/sensors/sensor_id_1",
            "upstream_key": "dummy_column",
            "upstream_value": "dummy_value",
        },
    ],
)
def test_sensor_data(scenario: dict, capsys: Any) -> None:
    """Test Sensor data construction.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """
    subject = SensorControlTableManager._convert_sensor_to_data(
        spec=SensorSpec(
            sensor_id=scenario["sensor_id"],
            assets=scenario["assets"],
            control_db_table_name=None,
            checkpoint_location=scenario["checkpoint_location"],
            preprocess_query=None,
            input_spec=None,
        ),
        status=scenario["status"],
        upstream_key=scenario["upstream_key"],
        upstream_value=scenario["upstream_value"],
        status_change_timestamp=scenario["status_change_timestamp"],
    )

    assert subject == [
        {
            "sensor_id": scenario["sensor_id"],
            "assets": scenario["assets"],
            "status": scenario["status"],
            "status_change_timestamp": scenario["status_change_timestamp"],
            "checkpoint_location": scenario["checkpoint_location"],
            "upstream_key": scenario["upstream_key"],
            "upstream_value": scenario["upstream_value"],
        }
    ]


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "true_when_table_data_and_status_acquired_new_data",
            "sensor_id": "sensor_id_1",
            "control_db_table_name": "sensor_control_db_table",
            "sensor_data": Row(
                sensor_id="sensor_id_1",
                assets=["asset_1"],
                status=SensorStatus.ACQUIRED_NEW_DATA.value,
                status_change_timestamp=TEST_DEFAULT_DATETIME,
                checkpoint_location="s3://dummy-bucket/sensors/sensor_id_1",
            ),
            "expected_result": True,
        },
        {
            "scenario_name": "false_when_table_data_is_absent",
            "sensor_id": "sensor_id_1",
            "control_db_table_name": "sensor_control_db_table",
            "sensor_data": None,
            "expected_result": False,
        },
        {
            "scenario_name": "false_when_table_data_is_present_and_"
            "status_different_than_acquired_new_data",
            "sensor_id": "sensor_id_1",
            "control_db_table_name": "sensor_control_db_table",
            "sensor_data": Row(
                sensor_id="sensor_id_1",
                assets=["asset_1"],
                status=SensorStatus.PROCESSED_NEW_DATA.value,
                status_change_timestamp=TEST_DEFAULT_DATETIME,
                checkpoint_location="s3://dummy-bucket/sensors/sensor_id_1",
            ),
            "expected_result": False,
        },
    ],
)
def test_check_if_sensor_has_acquired_data(scenario: dict, capsys: Any) -> None:
    """Test if Sensor has acquired data.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """
    with patch.object(
        SensorControlTableManager,
        "read_sensor_table_data",
        new=MagicMock(return_value=scenario["sensor_data"]),
    ) as sensor_table_data_mock:
        sensor_table_data_mock.start()
        subject = SensorControlTableManager.check_if_sensor_has_acquired_data(
            sensor_id=scenario["sensor_id"],
            control_db_table_name=scenario["control_db_table_name"],
        )

        assert subject == scenario["expected_result"]
        sensor_table_data_mock.stop()


@pytest.fixture
def control_table_fixture() -> DataFrame:
    """Return a dummy dataframe in the Sensor control table schema."""
    schema = StructType(
        [
            StructField("sensor_id", StringType(), False),
            StructField("assets", ArrayType(StringType(), False), True),
            StructField("status", StringType(), False),
            StructField("status_change_timestamp", TimestampType(), False),
            StructField("checkpoint_location", StringType(), True),
        ]
    )
    return ExecEnv.SESSION.createDataFrame(
        [
            [
                "sensor_id_1",
                [],
                SensorStatus.ACQUIRED_NEW_DATA.value,
                TEST_DEFAULT_DATETIME,
                "s3://dummy-bucket/sensors/sensor_id_1",
            ],
            [
                "sensor_id_2",
                ["asset_2"],
                SensorStatus.PROCESSED_NEW_DATA.value,
                TEST_DEFAULT_DATETIME,
                "s3://dummy-bucket/sensors/sensor_id_2",
            ],
            [
                "sensor_id_3",
                ["asset_3"],
                SensorStatus.ACQUIRED_NEW_DATA.value,
                TEST_DEFAULT_DATETIME,
                "s3://dummy-bucket/sensors/sensor_id_3",
            ],
        ],
        schema,
    )


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "sensor_id_is_present",
            "sensor_id": "sensor_id_1",
            "control_db_table_name": "sensor_control_db_table",
            "assets": None,
            "expected_result": {
                "sensor_id": "sensor_id_1",
                "assets": [],
                "status": SensorStatus.ACQUIRED_NEW_DATA.value,
                "status_change_timestamp": TEST_DEFAULT_DATETIME,
                "checkpoint_location": "s3://dummy-bucket/sensors/sensor_id_1",
            },
        },
        {
            "scenario_name": "sensor_id_is_absent_and_assets_is_present",
            "sensor_id": None,
            "control_db_table_name": "sensor_control_db_table",
            "assets": ["asset_2"],
            "expected_result": {
                "sensor_id": "sensor_id_2",
                "assets": ["asset_2"],
                "status": SensorStatus.PROCESSED_NEW_DATA.value,
                "status_change_timestamp": TEST_DEFAULT_DATETIME,
                "checkpoint_location": "s3://dummy-bucket/sensors/sensor_id_2",
            },
        },
        {
            "scenario_name": "sensor_id_and_sensor_asset_are_absent",
            "sensor_id": None,
            "control_db_table_name": "sensor_control_db_table",
            "assets": None,
            "expected_result": "Either sensor_id or assets "
            "need to be provided as arguments.",
        },
    ],
)
def test_read_sensor_table_data(
    scenario: dict, capsys: Any, control_table_fixture: DataFrame
) -> None:
    """Test read data from Sensor control table.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
        control_table_fixture: fixture representing the
            control table as DataFrame.
    """
    expected_result = scenario["expected_result"]

    with patch.object(DeltaTable, "forName", MagicMock()) as delta_table_for_name_mock:
        delta_table_for_name_mock.start()
        with patch.object(
            delta_table_for_name_mock.return_value,
            "toDF",
            MagicMock(return_value=control_table_fixture),
        ) as delta_table_for_to_df_mock:
            delta_table_for_to_df_mock.start()

            if scenario["scenario_name"] == "sensor_id_and_sensor_asset_are_absent":
                with pytest.raises(ValueError) as exception:
                    SensorControlTableManager.read_sensor_table_data(
                        sensor_id=scenario["sensor_id"],
                        control_db_table_name=scenario["control_db_table_name"],
                        assets=scenario["assets"],
                    )

                assert expected_result in str(exception.value)
            else:
                subject = SensorControlTableManager.read_sensor_table_data(
                    sensor_id=scenario["sensor_id"],
                    control_db_table_name=scenario["control_db_table_name"],
                    assets=scenario["assets"],
                )

                assert subject.asDict() == expected_result

            delta_table_for_to_df_mock.stop()
    delta_table_for_name_mock.stop()


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "test_if_has_new_data",
            "empty_df": False,
            "expected_result": True,
        },
        {
            "scenario_name": "test_if_has_not_new_data",
            "empty_df": True,
            "expected_result": False,
        },
    ],
)
def test_has_new_data(scenario: dict, capsys: Any) -> None:
    """Test if checking for new data works correctly where there is new data.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """
    new_data_df = _prepare_new_data_tests(return_empty_df=scenario["empty_df"])

    has_new_data = SensorUpstreamManager.get_new_data(new_data_df) is not None

    assert has_new_data == scenario["expected_result"]


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "sensor_db_table_and_default_dummy_value",
            "sensor": {
                "sensor_id": "sensor_id_1",
                "filter_exp": "?upstream_key > '?upstream_value'",
                "control_db_table_name": "test_jdbc_sensor_default_dummy_value",
                "upstream_key": "dummy_time",
                "upstream_value": None,
            },
            "sensor_data": Row(
                sensor_id="sensor_id_1",
                assets=["asset_1"],
                status=SensorStatus.ACQUIRED_NEW_DATA.value,
                status_change_timestamp=TEST_DEFAULT_DATETIME,
                checkpoint_location="s3://dummy-bucket/sensors/sensor_id_1",
                upstream_key="dummy_time",
                upstream_value=None,
            ),
            "expected_result": "SELECT COUNT(1) as count, "
            "'dummy_time' as UPSTREAM_KEY, "
            "max(dummy_time) as UPSTREAM_VALUE "
            "FROM sensor_new_data "
            "WHERE dummy_time > '-2147483647' "
            "HAVING COUNT(1) > 0",
        },
        {
            "scenario_name": "sensor_db_table_with_custom_value",
            "sensor": {
                "sensor_id": "sensor_id_1",
                "filter_exp": "?upstream_key > '?upstream_value'",
                "control_db_table_name": "test_jdbc_sensor_custom_value",
                "upstream_key": "dummy_time",
                "upstream_value": "3333333333",
            },
            "sensor_data": Row(
                sensor_id="sensor_id_1",
                assets=["asset_1"],
                status=SensorStatus.ACQUIRED_NEW_DATA.value,
                status_change_timestamp=TEST_DEFAULT_DATETIME,
                checkpoint_location="s3://dummy-bucket/sensors/sensor_id_1",
                upstream_key="dummy_time",
                upstream_value="3333333333",
            ),
            "expected_result": "SELECT COUNT(1) as count, "
            "'dummy_time' as UPSTREAM_KEY, "
            "max(dummy_time) as UPSTREAM_VALUE "
            "FROM sensor_new_data "
            "WHERE dummy_time > '3333333333' "
            "HAVING COUNT(1) > 0",
        },
        {
            "scenario_name": "filter_exp_preprocess_query",
            "sensor": {
                "sensor_id": "sensor_id_1",
                "filter_exp": "my_column > 'my_value'",
                "control_db_table_name": None,
                "upstream_key": None,
                "upstream_value": None,
            },
            "sensor_data": Row(
                sensor_id="sensor_id_1",
                assets=["asset_1"],
                status=SensorStatus.ACQUIRED_NEW_DATA.value,
                status_change_timestamp=TEST_DEFAULT_DATETIME,
                checkpoint_location="s3://dummy-bucket/sensors/sensor_id_1",
                upstream_key=None,
                upstream_value=None,
            ),
            "expected_result": "SELECT COUNT(1) as count "
            "FROM sensor_new_data "
            "WHERE my_column > 'my_value' "
            "HAVING COUNT(1) > 0",
        },
        {
            "scenario_name": "filter_exp_preprocess_query_from_upstream_table_name",
            "sensor": {
                "sensor_id": "sensor_id_1",
                "filter_exp": "?upstream_key > '?upstream_value'",
                "control_db_table_name": "test_jdbc_sensor_default_dummy_value",
                "upstream_key": "dummy_time",
                "upstream_value": "3333333333",
                "upstream_table_name": "test_db.dummy_table",
            },
            "sensor_data": Row(
                sensor_id="sensor_id_1",
                assets=["asset_1"],
                status=SensorStatus.ACQUIRED_NEW_DATA.value,
                status_change_timestamp=TEST_DEFAULT_DATETIME,
                checkpoint_location="s3://dummy-bucket/sensors/sensor_id_1",
                upstream_key="dummy_time",
                upstream_value="3333333333",
            ),
            "expected_result": "SELECT COUNT(1) as count, "
            "'dummy_time' as UPSTREAM_KEY, "
            "max(dummy_time) as UPSTREAM_VALUE "
            "FROM test_db.dummy_table "
            "WHERE dummy_time > '3333333333' "
            "HAVING COUNT(1) > 0",
        },
        {
            "scenario_name": "raise_exception_db_name_is_defined_and_upstream_key_not",
            "sensor": {
                "sensor_id": "sensor_id_1",
                "filter_exp": "my_column > 'my_value'",
                "control_db_table_name": "test_jdbc_sensor_raise_exception",
                "upstream_key": None,
                "upstream_value": None,
            },
            "expected_result": "If control_db_table_name is defined, "
            "upstream_key should "
            "also be defined!",
        },
    ],
)
def test_if_generate_filter_exp_preprocess_query(scenario: dict, capsys: Any) -> None:
    """Test filter expression for preprocess query gen.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """
    sensor_data = scenario["sensor"]
    expected_result = scenario["expected_result"]
    db_table = sensor_data.get("control_db_table_name")

    if (
        scenario["scenario_name"]
        == "raise_exception_db_name_is_defined_and_upstream_key_not"
    ):
        with pytest.raises(ValueError) as exception:
            SensorUpstreamManager.generate_filter_exp_query(
                sensor_data.get("sensor_id"),
                sensor_data.get("filter_exp"),
                f"test_db.{db_table}" if db_table else None,
                sensor_data.get("upstream_key"),
                sensor_data.get("upstream_value"),
            )

        assert expected_result in str(exception.value)
    else:
        with patch.object(
            SensorControlTableManager,
            "read_sensor_table_data",
            new=MagicMock(return_value=scenario["sensor_data"]),
        ) as sensor_table_data_mock:
            sensor_table_data_mock.start()
            subject = SensorUpstreamManager.generate_filter_exp_query(
                sensor_data.get("sensor_id"),
                sensor_data.get("filter_exp"),
                f"test_db.{db_table}" if db_table else None,
                sensor_data.get("upstream_key"),
                sensor_data.get("upstream_value"),
                sensor_data.get("upstream_table_name"),
            )

            assert subject == expected_result
            sensor_table_data_mock.stop()


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "generate_sensor_table_preprocess_query",
            "sensor_id": "sensor_id_1",
            "expected_result": "SELECT * "  # nosec
            "FROM sensor_new_data "
            "WHERE"
            " _change_type in ('insert', 'update_postimage')"
            " and sensor_id = 'sensor_id_1'"
            f" and status = '{SensorStatus.PROCESSED_NEW_DATA.value}'",
        }
    ],
)
def test_generate_sensor_table_preprocess_query(scenario: dict, capsys: Any) -> None:
    """Test if we are generating correctly the preprocess query.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """
    subject = SensorUpstreamManager.generate_sensor_table_preprocess_query(
        scenario["sensor_id"]
    )

    assert subject == scenario["expected_result"]


@pytest.fixture
def dataframe_fixture() -> DataFrame:
    """Return a dummy dataframe to be used in our tests."""
    schema = StructType([StructField("dummy_field", StringType(), True)])
    return ExecEnv.SESSION.createDataFrame(
        [["a"], ["b"], ["c"]],
        schema,
    )


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "read_new_data",
            "preprocess_query": None,
            "expected_result": 3,
        },
        {
            "scenario_name": "read_new_data_with_preprocess_query",
            "preprocess_query": "SELECT *"
            "FROM sensor_new_data "
            "WHERE dummy_field = 'b' ",
            "expected_result": 1,
        },
    ],
)
def test_read_new_data(
    scenario: dict, capsys: Any, dataframe_fixture: DataFrame
) -> None:
    """Test if we execute the preprocess query when reading new data.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
        dataframe_fixture: fixture representing a dummy dataframe to be
            used as mock return.
    """
    with patch.object(
        ReaderFactory, "get_data", MagicMock(return_value=dataframe_fixture)
    ) as reader_factory_mock:
        reader_factory_mock.start()

        new_data = SensorUpstreamManager.read_new_data(
            sensor_spec=SensorSpec(
                sensor_id="sensor_id_1",
                assets=["asset_1"],
                control_db_table_name="test_db.sensor_control_table",
                input_spec=None,
                preprocess_query=scenario["preprocess_query"],
                checkpoint_location="s3://dummy-bucket/sensors/sensor_id_1",
            )
        )

        assert new_data.count() == scenario["expected_result"]
        reader_factory_mock.stop()


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "generate_sap_logchain_query",
            "chain_id": "MY_SAP_CHAIN_ID",
            "expected_result": "WITH sensor_new_data AS ("
            "SELECT "
            "CHAIN_ID, "
            "CONCAT(DATUM, ZEIT) AS LOAD_DATE, "
            "ANALYZED_STATUS "
            "FROM SAPPHA.RSPCLOGCHAIN "
            "WHERE "
            "UPPER(CHAIN_ID) = UPPER('MY_SAP_CHAIN_ID') "
            "AND UPPER(ANALYZED_STATUS) = UPPER('G')"
            ")",  # nosec
        },
        {
            "scenario_name": "generate_sap_logchain_query_dbtable",
            "chain_id": "MY_SAP_CHAIN_ID",
            "dbtable": "test_db.test_table",
            "expected_result": "WITH sensor_new_data AS ("
            "SELECT "
            "CHAIN_ID, "
            "CONCAT(DATUM, ZEIT) AS LOAD_DATE, "
            "ANALYZED_STATUS "
            "FROM test_db.test_table "
            "WHERE "
            "UPPER(CHAIN_ID) = UPPER('MY_SAP_CHAIN_ID') "
            "AND UPPER(ANALYZED_STATUS) = UPPER('G')"
            ")",  # nosec
        },
        {
            "scenario_name": "generate_sap_logchain_query_status",
            "chain_id": "MY_SAP_CHAIN_ID",
            "status": "A",
            "expected_result": "WITH sensor_new_data AS ("
            "SELECT "
            "CHAIN_ID, "
            "CONCAT(DATUM, ZEIT) AS LOAD_DATE, "
            "ANALYZED_STATUS "
            "FROM SAPPHA.RSPCLOGCHAIN "
            "WHERE "
            "UPPER(CHAIN_ID) = UPPER('MY_SAP_CHAIN_ID') "
            "AND UPPER(ANALYZED_STATUS) = UPPER('A')"
            ")",  # nosec
        },
        {
            "scenario_name": "generate_sap_logchain_query_engine_table",
            "chain_id": "MY_SAP_CHAIN_ID",
            "engine_table_name": "test_SAPTABLE",
            "expected_result": "WITH test_SAPTABLE AS ("
            "SELECT "
            "CHAIN_ID, "
            "CONCAT(DATUM, ZEIT) AS LOAD_DATE, "
            "ANALYZED_STATUS "
            "FROM SAPPHA.RSPCLOGCHAIN "
            "WHERE "
            "UPPER(CHAIN_ID) = UPPER('MY_SAP_CHAIN_ID') "
            "AND UPPER(ANALYZED_STATUS) = UPPER('G')"
            ")",  # nosec
        },
        {
            "scenario_name": "generate_sap_logchain_query_full_custom",
            "chain_id": "MY_SAP_CHAIN_ID",
            "dbtable": "test_db.test_table",
            "status": "A",
            "engine_table_name": "test_SAPTABLE",
            "expected_result": "WITH test_SAPTABLE AS ("
            "SELECT "
            "CHAIN_ID, "
            "CONCAT(DATUM, ZEIT) AS LOAD_DATE, "
            "ANALYZED_STATUS "
            "FROM test_db.test_table "
            "WHERE "
            "UPPER(CHAIN_ID) = UPPER('MY_SAP_CHAIN_ID') "
            "AND UPPER(ANALYZED_STATUS) = UPPER('A')"
            ")",  # nosec
        },
        {
            "scenario_name": "raise_exception_chain_id_is_not_defined",
            "chain_id": None,
            "expected_result": "To query on log chain SAP table the chain id "
            "should be defined!",
        },
    ],
)
def test_generate_sensor_sap_logchain_query(scenario: dict, capsys: Any) -> None:
    """Test if we are generating correctly the sap logchain query.

    Args:
        scenario: scenario to test.
        capsys: capture stdout and stderr.
    """
    if scenario["scenario_name"] == "raise_exception_chain_id_is_not_defined":
        with pytest.raises(ValueError) as exception:
            SensorUpstreamManager.generate_sensor_sap_logchain_query(
                scenario["chain_id"],
                scenario.get("dbtable"),
                scenario.get("status"),
                scenario.get("engine_table_name"),
            )

        assert scenario["expected_result"] in str(exception.value)
    else:
        if scenario["scenario_name"] == "generate_sap_logchain_query":
            subject = SensorUpstreamManager.generate_sensor_sap_logchain_query(
                scenario.get("chain_id"),
            )
        elif scenario["scenario_name"] == "generate_sap_logchain_query_dbtable":
            subject = SensorUpstreamManager.generate_sensor_sap_logchain_query(
                scenario.get("chain_id"),
                dbtable=scenario.get("dbtable"),
            )
        elif scenario["scenario_name"] == "generate_sap_logchain_query_status":
            subject = SensorUpstreamManager.generate_sensor_sap_logchain_query(
                scenario.get("chain_id"),
                status=scenario.get("status"),
            )
        elif scenario["scenario_name"] == "generate_sap_logchain_query_engine_table":
            subject = SensorUpstreamManager.generate_sensor_sap_logchain_query(
                scenario.get("chain_id"),
                engine_table_name=scenario.get("engine_table_name"),
            )
        else:
            subject = SensorUpstreamManager.generate_sensor_sap_logchain_query(
                scenario.get("chain_id"),
                scenario.get("dbtable"),
                scenario.get("status"),
                scenario.get("engine_table_name"),
            )

        assert subject == scenario["expected_result"]


def _prepare_new_data_tests(return_empty_df: bool = False) -> DataFrame:
    schema = StructType([StructField("dummy_field", StringType(), True)])

    if return_empty_df:
        return ExecEnv.SESSION.createDataFrame(
            [],
            schema,
        )
    else:
        return ExecEnv.SESSION.createDataFrame(
            [["a"], ["b"], ["c"]],
            schema,
        )
