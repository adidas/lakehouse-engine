"""Module to define GAB Utility classes."""

import ast
import calendar
import json
from datetime import datetime
from typing import Optional, Union

import pendulum
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, struct, to_json

from lakehouse_engine.core.definitions import GABCadence, GABDefaults
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.logging_handler import LoggingHandler


class GABUtils(object):
    """Class containing utility functions for GAB."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    def logger(
        self,
        run_start_time: datetime,
        run_end_time: datetime,
        start: str,
        end: str,
        query_id: str,
        query_label: str,
        cadence: str,
        stage_file_path: str,
        query: str,
        status: str,
        error_message: Union[Exception, str],
        target_database: str,
    ) -> None:
        """Store the execution of each stage in the log events table.

        Args:
            run_start_time: execution start time.
            run_end_time: execution end time.
            start: use case start date.
            end: use case end date.
            query_id: gab configuration table use case identifier.
            query_label: gab configuration table use case name.
            cadence: cadence to process.
            stage_file_path: stage file path.
            query: query to execute.
            status: status of the query execution.
            error_message: error message if present.
            target_database: target database to write.
        """
        ins = """
        INSERT INTO {database}.gab_log_events
        VALUES (
            '{run_start_time}',
            '{run_end_time}',
            '{start}',
            '{end}',
            {query_id},
            '{query_label}',
            '{cadence}',
            '{stage_file_path}',
            '{query}',
            '{status}',
            '{error_message}'
        )""".format(  # nosec: B608
            database=target_database,
            run_start_time=run_start_time,
            run_end_time=run_end_time,
            start=start,
            end=end,
            query_id=query_id,
            query_label=query_label,
            cadence=cadence,
            stage_file_path=stage_file_path,
            query=self._escape_quote(query),
            status=status,
            error_message=(
                self._escape_quote(str(error_message))
                if status == "Failed"
                else error_message
            ),
        )

        ExecEnv.SESSION.sql(ins)

    @classmethod
    def _escape_quote(cls, to_escape: str) -> str:
        """Escape quote on string.

        Args:
            to_escape: string to escape.
        """
        return to_escape.replace("'", r"\'").replace('"', r"\"")

    @classmethod
    def get_json_column_as_dict(
        cls, lookup_query_builder: DataFrame, query_id: str, query_column: str
    ) -> dict:  # type: ignore
        """Get JSON column as dictionary.

        Args:
            lookup_query_builder: gab configuration data.
            query_id: gab configuration table use case identifier.
            query_column: column to get as json.
        """
        column_df = lookup_query_builder.filter(
            col("query_id") == lit(query_id)
        ).select(col(query_column))

        column_df_json = column_df.select(
            to_json(struct([column_df[x] for x in column_df.columns]))
        ).collect()[0][0]

        json_column = json.loads(column_df_json)

        for mapping in json_column.values():
            column_as_json = ast.literal_eval(mapping)

        return column_as_json  # type: ignore

    @classmethod
    def extract_columns_from_mapping(
        cls,
        columns: dict,
        is_dimension: bool,
        extract_column_without_alias: bool = False,
        table_alias: Optional[str] = None,
        is_extracted_value_as_name: bool = True,
    ) -> Union[tuple[list[str], list[str]], list[str]]:
        """Extract and transform columns to SQL select statement.

        Args:
            columns: data to extract the columns.
            is_dimension: flag identifying if is a dimension or a metric.
            extract_column_without_alias: flag to inform if it's to extract columns
                without aliases.
            table_alias: name or alias from the source table.
            is_extracted_value_as_name: identify if the extracted value is the
                column name.
        """
        column_with_alias = (
            "".join([table_alias, ".", "{} as {}"]) if table_alias else "{} as {}"
        )
        column_without_alias = (
            "".join([table_alias, ".", "{}"]) if table_alias else "{}"
        )

        extracted_columns_with_alias = []
        extracted_columns_without_alias = []
        for column_name, column_value in columns.items():
            if extract_column_without_alias:
                extracted_column_without_alias = column_without_alias.format(
                    cls._get_column_format_without_alias(
                        is_dimension,
                        column_name,
                        column_value,
                        is_extracted_value_as_name,
                    )
                )
                extracted_columns_without_alias.append(extracted_column_without_alias)

            extracted_column_with_alias = column_with_alias.format(
                *cls._extract_column_with_alias(
                    is_dimension,
                    column_name,
                    column_value,
                    is_extracted_value_as_name,
                )
            )
            extracted_columns_with_alias.append(extracted_column_with_alias)

        return (
            (extracted_columns_with_alias, extracted_columns_without_alias)
            if extract_column_without_alias
            else extracted_columns_with_alias
        )

    @classmethod
    def _extract_column_with_alias(
        cls,
        is_dimension: bool,
        column_name: str,
        column_value: Union[str, dict],
        is_extracted_value_as_name: bool = True,
    ) -> tuple[str, str]:
        """Extract column name with alias.

        Args:
            is_dimension: flag indicating if the column is a dimension.
            column_name: name of the column.
            column_value: value of the column.
            is_extracted_value_as_name: flag indicating if the name of the column is the
                extracted value.
        """
        extracted_value = (
            column_value
            if is_dimension
            else (column_value["metric_name"])  # type: ignore
        )

        return (
            (extracted_value, column_name)  # type: ignore
            if is_extracted_value_as_name
            else (column_name, extracted_value)
        )

    @classmethod
    def _get_column_format_without_alias(
        cls,
        is_dimension: bool,
        column_name: str,
        column_value: Union[str, dict],
        is_extracted_value_as_name: bool = True,
    ) -> str:
        """Extract column name without alias.

        Args:
            is_dimension: flag indicating if the column is a dimension.
            column_name: name of the column.
            column_value: value of the column.
            is_extracted_value_as_name: flag indicating if the name of the column is the
                extracted value.
        """
        extracted_value: str = (
            column_value
            if is_dimension
            else (column_value["metric_name"])  # type: ignore
        )

        return extracted_value if is_extracted_value_as_name else column_name

    @classmethod
    def get_cadence_configuration_at_end_date(cls, end_date: datetime) -> dict:
        """A dictionary that corresponds to the conclusion of a cadence.

        Any end date inputted by the user we check this end date is actually end of
            a cadence (YEAR, QUARTER, MONTH, WEEK).
        If the user input is 2024-03-31 this is a month end and a quarter end that
            means any use cases configured as month or quarter need to be calculated.

        Args:
            end_date: base end date.
        """
        init_end_date_dict = {}

        expected_end_cadence_date = pendulum.datetime(
            int(end_date.strftime("%Y")),
            int(end_date.strftime("%m")),
            int(end_date.strftime("%d")),
        ).replace(tzinfo=None)

        # Validating YEAR cadence
        if end_date == expected_end_cadence_date.last_of("year"):
            init_end_date_dict["YEAR"] = "N"

        # Validating QUARTER cadence
        if end_date == expected_end_cadence_date.last_of("quarter"):
            init_end_date_dict["QUARTER"] = "N"

        # Validating MONTH cadence
        if end_date == datetime(
            int(end_date.strftime("%Y")),
            int(end_date.strftime("%m")),
            calendar.monthrange(
                int(end_date.strftime("%Y")), int(end_date.strftime("%m"))
            )[1],
        ):
            init_end_date_dict["MONTH"] = "N"

        # Validating WEEK cadence
        if end_date == expected_end_cadence_date.end_of("week").replace(
            hour=0, minute=0, second=0, microsecond=0
        ):
            init_end_date_dict["WEEK"] = "N"

        init_end_date_dict["DAY"] = "N"

        return init_end_date_dict

    def get_reconciliation_cadences(
        self,
        cadence: str,
        selected_reconciliation_window: dict,
        cadence_configuration_at_end_date: dict,
        rerun_flag: str,
    ) -> dict:
        """Get reconciliation cadences based on the use case configuration.

        Args:
            cadence: cadence to process.
            selected_reconciliation_window: configured use case reconciliation window.
            cadence_configuration_at_end_date: cadences to execute at the end date.
            rerun_flag: flag indicating if it's a rerun or a normal run.
        """
        configured_cadences = self._get_configured_cadences_by_snapshot(
            cadence, selected_reconciliation_window, cadence_configuration_at_end_date
        )

        return self._get_cadences_to_execute(
            configured_cadences, cadence, cadence_configuration_at_end_date, rerun_flag
        )

    @classmethod
    def _get_cadences_to_execute(
        cls,
        configured_cadences: dict,
        cadence: str,
        cadence_configuration_at_end_date: dict,
        rerun_flag: str,
    ) -> dict:
        """Get cadences to execute.

        Args:
            cadence: cadence to process.
            configured_cadences: configured use case reconciliation window.
            cadence_configuration_at_end_date: cadences to execute at the end date.
            rerun_flag: flag indicating if it's a rerun or a normal run.
        """
        cadences_to_execute = {}
        cad_order = GABCadence.get_ordered_cadences()

        for snapshot_cadence, snapshot_flag in configured_cadences.items():
            if (
                (cad_order[cadence] > cad_order[snapshot_cadence])
                and (rerun_flag == "Y")
            ) or snapshot_cadence in cadence_configuration_at_end_date:
                cadences_to_execute[snapshot_cadence] = snapshot_flag
            elif snapshot_cadence not in cadence_configuration_at_end_date:
                continue

        return cls._sort_cadences_to_execute(cadences_to_execute, cad_order)

    @classmethod
    def _sort_cadences_to_execute(
        cls, cadences_to_execute: dict, cad_order: dict
    ) -> dict:
        """Sort the cadences to execute.

        Args:
            cadences_to_execute: cadences to execute.
            cad_order: all cadences with order.
        """
        # ordering it because when grouping cadences with snapshot and without snapshot
        # can impact the cadence ordering.
        sorted_cadences_to_execute: dict = dict(
            sorted(
                cadences_to_execute.items(),
                key=lambda item: cad_order.get(item[0]),  # type: ignore
            )
        )
        # ordering cadences to execute it from bigger (YEAR) to smaller (DAY)
        cadences_to_execute_items = []

        for cadence_name, cadence_value in sorted_cadences_to_execute.items():
            cadences_to_execute_items.append((cadence_name, cadence_value))

        cadences_sorted_by_bigger_cadence_to_execute: dict = dict(
            reversed(cadences_to_execute_items)
        )

        return cadences_sorted_by_bigger_cadence_to_execute

    @classmethod
    def _get_configured_cadences_by_snapshot(
        cls,
        cadence: str,
        selected_reconciliation_window: dict,
        cadence_configuration_at_end_date: dict,
    ) -> dict:
        """Get configured cadences to execute.

        Args:
            cadence: selected cadence.
            selected_reconciliation_window: configured use case reconciliation window.
            cadence_configuration_at_end_date: cadences to execute at the end date.

        Returns:
            Each cadence with the corresponding information if it's to execute with
                snapshot or not.
        """
        cadences_by_snapshot = {}

        (
            no_snapshot_cadences,
            snapshot_cadences,
        ) = cls._generate_reconciliation_by_snapshot(
            cadence, selected_reconciliation_window
        )

        for snapshot_cadence, snapshot_flag in no_snapshot_cadences.items():
            if snapshot_cadence in cadence_configuration_at_end_date:
                cadences_by_snapshot[snapshot_cadence] = snapshot_flag

                cls._LOGGER.info(f"{snapshot_cadence} is present in {cadence} cadence")
                break

        cadences_by_snapshot.update(snapshot_cadences)

        if (not cadences_by_snapshot) and (
            cadence in cadence_configuration_at_end_date
        ):
            cadences_by_snapshot[cadence] = "N"

        return cadences_by_snapshot

    @classmethod
    def _generate_reconciliation_by_snapshot(
        cls, cadence: str, selected_reconciliation_window: dict
    ) -> tuple[dict, dict]:
        """Generate reconciliation by snapshot.

        Args:
            cadence: cadence to process.
            selected_reconciliation_window: configured use case reconciliation window.
        """
        cadence_snapshot_configuration = {cadence: "N"}
        for cadence in GABCadence.get_cadences():
            cls._add_cadence_snapshot_to_cadence_snapshot_config(
                cadence, selected_reconciliation_window, cadence_snapshot_configuration
            )
        cadence_snapshot_configuration = dict(
            sorted(
                cadence_snapshot_configuration.items(),
                key=(
                    lambda item: GABCadence.get_ordered_cadences().get(  # type: ignore
                        item[0]
                    )
                ),
            )
        )

        cadence_snapshot_configuration = dict(
            reversed(list(cadence_snapshot_configuration.items()))
        )

        cadences_without_snapshot = {
            key: value
            for key, value in cadence_snapshot_configuration.items()
            if value == "N"
        }

        cadences_with_snapshot = {
            key: value
            for key, value in cadence_snapshot_configuration.items()
            if value == "Y"
        }

        return cadences_with_snapshot, cadences_without_snapshot

    @classmethod
    def _add_cadence_snapshot_to_cadence_snapshot_config(
        cls,
        cadence: str,
        selected_reconciliation_window: dict,
        cadence_snapshot_configuration: dict,
    ) -> None:
        """Add the selected reconciliation to cadence snapshot configuration.

        Args:
            cadence: selected cadence.
            selected_reconciliation_window:  configured use case reconciliation window.
            cadence_snapshot_configuration: cadence snapshot configuration dictionary
                who will be updated with the new value.
        """
        if cadence in selected_reconciliation_window:
            cadence_snapshot_configuration[cadence] = selected_reconciliation_window[
                cadence
            ]["snapshot"]

    @classmethod
    def format_datetime_to_default(cls, date_to_format: datetime) -> str:
        """Format datetime to GAB default format.

        Args:
            date_to_format: date to format.
        """
        return datetime.date(date_to_format).strftime(GABDefaults.DATE_FORMAT.value)


class GABPartitionUtils(object):
    """Class to extract a partition based in a date period."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    @classmethod
    def get_years(cls, start_date: str, end_date: str) -> list[str]:
        """Return a list of distinct years from the input parameters.

        Args:
            start_date: start of the period.
            end_date: end of the period.
        """
        year = []
        if start_date > end_date:
            raise ValueError(
                "Input Error: Invalid start_date and end_date. "
                "Start_date is greater than end_date"
            )

        for i in range(int(start_date[0:4]), int(end_date[0:4]) + 1):
            year.append(str(i))

        return year

    @classmethod
    def get_partition_condition(cls, start_date: str, end_date: str) -> str:
        """Return year,month and day partition statement from the input parameters.

        Args:
            start_date: start of the period.
            end_date: end of the period.
        """
        years = cls.get_years(start_date, end_date)
        if len(years) > 1:
            partition_condition = cls._get_multiple_years_partition(
                start_date, end_date, years
            )
        else:
            partition_condition = cls._get_single_year_partition(start_date, end_date)
        return partition_condition

    @classmethod
    def _get_multiple_years_partition(
        cls, start_date: str, end_date: str, years: list[str]
    ) -> str:
        """Return partition when executing multiple years (>1).

        Args:
            start_date: start of the period.
            end_date: end of the period.
            years: list of years.
        """
        start_date_month = cls._extract_date_part_from_date("MONTH", start_date)
        start_date_day = cls._extract_date_part_from_date("DAY", start_date)

        end_date_month = cls._extract_date_part_from_date("MONTH", end_date)
        end_date_day = cls._extract_date_part_from_date("DAY", end_date)

        year_statement = "(year = {0} and (".format(years[0]) + "{})"
        if start_date_month != "12":
            start_date_partition = year_statement.format(
                "(month = {0} and day between {1} and 31)".format(
                    start_date_month, start_date_day
                )
                + " or (month between {0} and 12)".format(int(start_date_month) + 1)
            )
        else:
            start_date_partition = year_statement.format(
                "month = {0} and day between {1} and 31".format(
                    start_date_month, start_date_day
                )
            )

        period_years_partition = ""

        if len(years) == 3:
            period_years_partition = ") or (year = {0}".format(years[1])
        elif len(years) > 3:
            period_years_partition = ") or (year between {0} and {1})".format(
                years[1], years[-2]
            )

        if end_date_month != "01":
            end_date_partition = (
                ") or (year = {0} and ((month between 01 and {1})".format(
                    years[-1], int(end_date_month) - 1
                )
                + " or (month = {0} and day between 1 and {1})))".format(
                    end_date_month, end_date_day
                )
            )
        else:
            end_date_partition = (
                ") or (year = {0} and month = 1 and day between 01 and {1})".format(
                    years[-1], end_date_day
                )
            )
        partition_condition = (
            start_date_partition + period_years_partition + end_date_partition
        )

        return partition_condition

    @classmethod
    def _get_single_year_partition(cls, start_date: str, end_date: str) -> str:
        """Return partition when executing a single year.

        Args:
            start_date: start of the period.
            end_date: end of the period.
        """
        start_date_year = cls._extract_date_part_from_date("YEAR", start_date)
        start_date_month = cls._extract_date_part_from_date("MONTH", start_date)
        start_date_day = cls._extract_date_part_from_date("DAY", start_date)

        end_date_year = cls._extract_date_part_from_date("YEAR", end_date)
        end_date_month = cls._extract_date_part_from_date("MONTH", end_date)
        end_date_day = cls._extract_date_part_from_date("DAY", end_date)

        if start_date_month != end_date_month:
            months = []
            for i in range(int(start_date_month), int(end_date_month) + 1):
                months.append(i)

            start_date_partition = (
                "year = {0} and ((month={1} and day between {2} and 31)".format(
                    start_date_year, months[0], start_date_day
                )
            )
            period_years_partition = ""
            if len(months) == 2:
                period_years_partition = start_date_partition
            elif len(months) == 3:
                period_years_partition = (
                    start_date_partition + " or (month = {0})".format(months[1])
                )
            elif len(months) > 3:
                period_years_partition = (
                    start_date_partition
                    + " or (month between {0} and {1})".format(months[1], months[-2])
                )
            partition_condition = (
                period_years_partition
                + " or (month = {0} and day between 1 and {1}))".format(
                    end_date_month, end_date_day
                )
            )
        else:
            partition_condition = (
                "year = {0} and month = {1} and day between {2} and {3}".format(
                    end_date_year, end_date_month, start_date_day, end_date_day
                )
            )

        return partition_condition

    @classmethod
    def _extract_date_part_from_date(cls, part: str, date: str) -> str:
        """Extract date part from string date.

        Args:
            part: date part (possible values: DAY, MONTH, YEAR)
            date: string date.
        """
        if "DAY" == part.upper():
            return date[8:10]
        elif "MONTH" == part.upper():
            return date[5:7]
        else:
            return date[0:4]
