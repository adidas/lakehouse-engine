"""Module with integration tests for gab feature."""

from typing import Any, Optional

import pendulum
import pytest
from _pytest.fixtures import SubRequest
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import Row

from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.engine import execute_gab, load_data
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.schema_utils import SchemaUtils
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_NAME = "gab"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_NAME}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_NAME}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_NAME}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_NAME}"
_LOGGER = LoggingHandler(__name__).get_logger()
_CALENDAR_MIN_DATE = pendulum.from_format("2016-01-01", "YYYY-MM-DD")
_CALENDAR_MAX_DATE = pendulum.from_format("2023-01-01", "YYYY-MM-DD")
_SETUP_DELTA_TABLES = {
    "dim_calendar": "calendar",
    "lkp_query_builder": "lkp_query_builder",
    "gab_use_case_results": "gab_use_case_results",
    "gab_log_events": "gab_log_events",
}
_USE_CASE_TABLES = ["order_events", "dummy_sales_kpi"]


def _create_gab_tables() -> None:
    """Create necessary tables to use GAB."""
    for table_name, table_column_file in _SETUP_DELTA_TABLES.items():
        DataframeHelpers.create_delta_table(
            cols=SchemaUtils.from_file_to_dict(
                f"file:///{TEST_RESOURCES}/setup/column_list/{table_column_file}.json"
            ),
            table=table_name,
        )


def _generate_calendar_test_dates() -> list:
    """Generate calendar date between the test period."""
    calendar_dates: list[Row] = []
    calendar_date = _CALENDAR_MIN_DATE

    for _ in range(1, _CALENDAR_MIN_DATE.diff(_CALENDAR_MAX_DATE).in_days()):
        calendar_date = calendar_date.add(days=1)
        calendar_dates.append(Row(value=calendar_date.strftime("%Y-%m-%d")))

    return calendar_dates


def _transform_dates_list_to_dataframe(dates: list) -> DataFrame:
    """Create calendar dates DataFrame from a list of dates.

    Args:
        dates: list of dates to create the calendar DataFrame.
    """
    calendar_dates = ExecEnv.SESSION.createDataFrame(dates)
    calendar_dates = calendar_dates.withColumn(
        "calendar_date", to_date(col("value"), "yyyy-MM-dd")
    ).drop(calendar_dates.value)

    return calendar_dates


def _feed_dim_calendar(df: DataFrame) -> DataFrame:
    """Feed dim calendar table."""
    df.createOrReplaceTempView("dates_completed")

    df_cal = ExecEnv.SESSION.sql(
        """
        WITH monday_calendar AS (
            SELECT
                 calendar_date,
                WEEKOFYEAR(calendar_date) AS weeknum_mon,
                DATE_FORMAT(calendar_date, 'E') AS day_en,
                MIN(calendar_date) OVER (PARTITION BY CONCAT(DATE_PART(
                    'YEAROFWEEK', calendar_date
                ),
                WEEKOFYEAR(calendar_date)) ORDER BY calendar_date) AS weekstart_mon
            FROM dates_completed
            ORDER BY
                calendar_date
        ),
        monday_calendar_plus_week_num_sunday AS (
            SELECT
                monday_calendar.*,
                LEAD(weeknum_mon) OVER(ORDER BY calendar_date) AS weeknum_sun
            FROM monday_calendar
        ),
        calendar_complementary_values AS (
            SELECT
                calendar_date,
                weeknum_mon,
                day_en,
                weekstart_mon,
                weekstart_mon+6 AS weekend_mon,
                LEAD(weekstart_mon-1) OVER(ORDER BY calendar_date) AS weekstart_sun,
                DATE(DATE_TRUNC('MONTH', calendar_date)) AS month_start,
                DATE(DATE_TRUNC('QUARTER', calendar_date)) AS quarter_start,
                DATE(DATE_TRUNC('YEAR', calendar_date)) AS year_start
            FROM monday_calendar_plus_week_num_sunday
        )
        SELECT
            calendar_date,
            day_en,
            weeknum_mon,
            weekstart_mon,
            weekend_mon,
            weekstart_sun,
            weekstart_sun+6 AS weekend_sun,
            month_start,
            add_months(month_start, 1)-1 AS month_end,
            quarter_start,
            ADD_MONTHS(quarter_start, 3)-1 AS quarter_end,
            year_start,
            ADD_MONTHS(year_start, 12)-1 AS year_end
        FROM calendar_complementary_values
        """
    )

    return df_cal


def _feed_table_with_test_data(
    table_name: str,
    source_dataframe: Optional[DataFrame] = None,
    transformer_specs: list = None,
    input_id_to_write: str = "data_to_load",
) -> None:
    """Feed table with test data.

    Args:
        table_name: name of the table to feed.
        source_dataframe: dataframe to feed the table, present when load_type is
            dataframe.
        transformer_specs: acon transformations.
        input_id_to_write: input id used in the write step.
    """
    input_spec: dict[str, Any]
    if source_dataframe:
        input_spec = {
            "spec_id": "data_to_load",
            "read_type": "batch",
            "data_format": "dataframe",
            "df_name": source_dataframe,
        }
    else:
        input_spec = {
            "spec_id": "data_to_load",
            "read_type": "batch",
            "data_format": "csv",
            "schema_path": f"file:///{TEST_RESOURCES}/setup/schema/{table_name}.json",
            "options": {
                "header": True,
                "delimiter": "|",
                "mode": "FAILFAST",
                "nullValue": "null",
            },
            "location": f"file:///{TEST_RESOURCES}/setup/data/{table_name}.csv",
        }

    acon = {
        "input_specs": [input_spec],
        "transform_specs": transformer_specs if transformer_specs else [],
        "output_specs": [
            {
                "spec_id": "loaded_table",
                "input_id": input_id_to_write,
                "write_type": "overwrite",
                "data_format": "delta",
                "db_table": f"test_db.{table_name}",
            },
        ],
    }
    load_data(acon=acon)


def _create_and_load_source_data_for_use_case(source_table: str) -> None:
    """Create and load source for use case.

    Args:
        source_table: source table to create/feed the data.
    """
    DataframeHelpers.create_delta_table(
        cols=SchemaUtils.from_file_to_dict(
            f"file:///{TEST_RESOURCES}/setup/column_list/{source_table}.json"
        ),
        table=source_table,
    )

    _feed_table_with_test_data(table_name=source_table)


def _import_use_case_sql(use_case_name: str) -> None:
    """Import use case SQL stage files.

    Args:
        use_case_name: name of the use case.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/usecases/{use_case_name}/*.sql",
        f"{TEST_LAKEHOUSE_IN}/usecases_sql/{use_case_name}/",
    )


def _setup_use_case(use_case_name: str) -> None:
    """Set up the use case.

    Args:
        use_case_name: name of hte use case.
    """
    _create_and_load_source_data_for_use_case(use_case_name)
    _import_use_case_sql(use_case_name)


@pytest.fixture(scope="session", autouse=True)
def _gab_setup() -> None:
    """Execute the GAB setup.

    Create and load config gab tables.
    """
    _LOGGER.info("Creating gab config tables...")

    _create_gab_tables()
    _feed_table_with_test_data(table_name="lkp_query_builder")

    calendar_dates = _generate_calendar_test_dates()
    calendar_dates_df = _transform_dates_list_to_dataframe(calendar_dates)
    _feed_table_with_test_data(
        table_name="dim_calendar",
        source_dataframe=calendar_dates_df,
        input_id_to_write="transformed_data",
        transformer_specs=[
            {
                "spec_id": "transformed_data",
                "input_id": "data_to_load",
                "transformers": [
                    {
                        "function": "custom_transformation",
                        "args": {"custom_transformer": _feed_dim_calendar},
                    }
                ],
            }
        ],
    )

    _LOGGER.info("Created with success...")


@pytest.fixture(scope="session", autouse=True, params=[_USE_CASE_TABLES])
def _run_setup_use_case(request: SubRequest) -> None:
    """Create and load use case gab tables.

    Args:
        request: fixture request, giving access to the `params`.
    """
    _LOGGER.info("Creating use case config tables...")
    for use_case in request.param:
        _setup_use_case(use_case)

    _LOGGER.info("Created with success...")


@pytest.mark.usefixtures("_gab_setup", "_run_setup_use_case")
@pytest.mark.parametrize(
    "scenario",
    [
        {
            "use_case_name": "order_events",
            "gold_assets": ["vw_orders_all", "vw_orders_filtered"],
            "gold_asset_schema": "vw_orders",
            "use_case_stages": "order_events",
        },
        {
            "use_case_name": "order_events_snapshot",
            "gold_assets": ["vw_orders_all_snapshot", "vw_orders_filtered_snapshot"],
            "gold_asset_schema": "vw_orders",
            "use_case_stages": "order_events",
        },
        {
            "use_case_name": "order_events_nam",
            "gold_assets": [
                "vw_nam_orders_all_snapshot",
                "vw_nam_orders_filtered_snapshot",
            ],
            "gold_asset_schema": "vw_orders",
            "use_case_stages": "order_events",
        },
        {
            "use_case_name": "order_events_negative_timezone_offset",
            "gold_assets": [
                "vw_negative_offset_orders_all",
                "vw_negative_offset_orders_filtered",
            ],
            "gold_asset_schema": "vw_orders",
            "use_case_stages": "order_events",
        },
        {
            "use_case_name": "dummy_sales_kpi",
            "gold_assets": ["vw_dummy_sales_kpi"],
            "gold_asset_schema": "vw_dummy_sales_kpi",
            "use_case_stages": "dummy_sales_kpi",
        },
        {
            "use_case_name": "skip_use_case_by_empty_reconciliation",
            "query_label": "order_events_empty_reconciliation_window",
            "use_case_stages": "order_events",
        },
        {
            "use_case_name": "skip_use_case_by_empty_requested_cadence",
            "query_label": "order_events_negative_timezone_offset",
            "use_case_stages": "order_events",
        },
        {
            "use_case_name": "skip_use_case_by_not_configured_cadence",
            "query_label": "order_events_negative_timezone_offset",
            "use_case_stages": "order_events",
        },
        {
            "use_case_name": "skip_use_case_by_unexisting_cadence",
            "query_label": "order_events_unexisting_cadence",
            "use_case_stages": "order_events",
        },
    ],
)
def test_gold_asset_builder(scenario: dict, caplog: Any) -> None:
    """Test the feature of using gab to generate gold assets.

    Args:
        scenario: scenario to test.
        caplog: captured log.

    Scenarios:
        order_events: tests gab features:
            - Cadence
            - Recon Window
            - Metrics
            - Extended Window Calculator
            Also test the generation of two different views for the same asset.
        order_events_snapshot: tests gab features:
            - Cadence
            - Recon Window
            - Metrics
            - Extended Window Calculator
            - Snapshot
            Also test the generation of two different views for the same asset.
        order_events_nam: tests gab features:
            - Cadence
            - Recon Window
            - Metrics
            - Extended Window Calculator
            - Snapshot
            Also test the generation of two different views for the same asset and the
                use case `query_type` equals to `NAM`.
        order_events_negative_timezone_offset: tests gab features:
            - Cadence
            - Recon Window
            - Metrics
            - Extended Window Calculator
            - Offset
            - Snapshot
            Also test the generation of two different views for the same asset.
       dummy_sales_kpi: tests almost all gab features:
            - Cadence
            - Recon Window
            - Metrics
            - Extended Window Calculator
            Also test multiple stages for the asset creation.

    """
    use_case_name = scenario["use_case_name"]
    execute_gab(
        f"file://{TEST_RESOURCES}/usecases/{scenario['use_case_stages']}/scenario/"
        f"{use_case_name}.json"
    )

    if not use_case_name.startswith("skip"):
        for expected_gold_asset in scenario["gold_assets"]:
            result_df = ExecEnv.SESSION.sql(
                f"SELECT * FROM test_db.{expected_gold_asset}"  # nosec
            )
            control_df = DataframeHelpers.read_from_file(
                f"{TEST_RESOURCES}/control/data/{expected_gold_asset}.csv",
                schema=SchemaUtils.from_file_to_dict(
                    f"file:///{TEST_RESOURCES}/control/schema/"
                    f"{scenario['gold_asset_schema']}.json"
                ),
            )

            assert not DataframeHelpers.has_diff(result_df, control_df)
    else:
        assert (
            f"Skipping use case {scenario['query_label']}. No cadence processed "
            "for the use case." in caplog.text
        )
