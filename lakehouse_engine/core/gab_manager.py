"""Module to define GAB Manager classes."""

import calendar
from datetime import datetime, timedelta
from typing import Tuple, Union, cast

import pendulum
from pendulum import DateTime
from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import GABCadence, GABDefaults
from lakehouse_engine.core.gab_sql_generator import GABViewGenerator
from lakehouse_engine.utils.gab_utils import GABUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler


class GABCadenceManager(object):
    """Class to control the GAB Cadence Window."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    def extended_window_calculator(
        self,
        cadence: str,
        reconciliation_cadence: str,
        current_date: datetime,
        start_date_str: str,
        end_date_str: str,
        query_type: str,
        rerun_flag: str,
        snapshot_flag: str,
    ) -> tuple[datetime, datetime, datetime, datetime]:
        """extended_window_calculator function.

        Calculates the extended window of any cadence despite the user providing
        custom dates which are not the exact start and end dates of a cadence.

        Args:
            cadence: cadence to process
            reconciliation_cadence: reconciliation to process.
            current_date: current date.
            start_date_str: start date of the period to process.
            end_date_str: end date of the period to process.
            query_type: use case query type.
            rerun_flag: flag indicating if it's a rerun or a normal run.
            snapshot_flag: flag indicating if for this cadence the snapshot is enabled.
        """
        cad_order = GABCadence.get_ordered_cadences()

        derived_cadence = self._get_reconciliation_cadence(
            cad_order, rerun_flag, cadence, reconciliation_cadence, snapshot_flag
        )

        self._LOGGER.info(f"cadence passed to extended window: {derived_cadence}")

        start_date = datetime.strptime(start_date_str, GABDefaults.DATE_FORMAT.value)
        end_date = datetime.strptime(end_date_str, GABDefaults.DATE_FORMAT.value)

        bucket_start_date, bucket_end_date = self.get_cadence_start_end_dates(
            cadence, derived_cadence, start_date, end_date, query_type, current_date
        )

        self._LOGGER.info(f"bucket dates: {bucket_start_date} - {bucket_end_date}")

        filter_start_date, filter_end_date = self.get_cadence_start_end_dates(
            cadence,
            (
                reconciliation_cadence
                if cad_order[cadence] < cad_order[reconciliation_cadence]
                else cadence
            ),
            start_date,
            end_date,
            query_type,
            current_date,
        )

        self._LOGGER.info(f"filter dates: {filter_start_date} - {filter_end_date}")

        return bucket_start_date, bucket_end_date, filter_start_date, filter_end_date

    @classmethod
    def _get_reconciliation_cadence(
        cls,
        cadence_order: dict,
        rerun_flag: str,
        cadence: str,
        reconciliation_cadence: str,
        snapshot_flag: str,
    ) -> str:
        """Get bigger cadence when rerun_flag or snapshot.

        Args:
            cadence_order: ordered cadences.
            rerun_flag: flag indicating if it's a rerun or a normal run.
            cadence: cadence to process.
            reconciliation_cadence: reconciliation to process.
            snapshot_flag: flag indicating if for this cadence the snapshot is enabled.
        """
        derived_cadence = reconciliation_cadence

        if rerun_flag == "Y":
            if cadence_order[cadence] > cadence_order[reconciliation_cadence]:
                derived_cadence = cadence
            elif cadence_order[cadence] < cadence_order[reconciliation_cadence]:
                derived_cadence = reconciliation_cadence
        else:
            if (
                cadence_order[cadence] > cadence_order[reconciliation_cadence]
                and snapshot_flag == "Y"
            ) or (cadence_order[cadence] < cadence_order[reconciliation_cadence]):
                derived_cadence = reconciliation_cadence
            elif (
                cadence_order[cadence] > cadence_order[reconciliation_cadence]
                and snapshot_flag == "N"
            ):
                derived_cadence = cadence

        return derived_cadence

    def get_cadence_start_end_dates(
        self,
        cadence: str,
        derived_cadence: str,
        start_date: datetime,
        end_date: datetime,
        query_type: str,
        current_date: datetime,
    ) -> tuple[datetime, datetime]:
        """Generate the new set of extended start and end dates based on the cadence.

        Running week cadence again to extend to correct week start and end date in case
            of recon window for Week cadence is present.
        For end_date 2012-12-31,in case of Quarter Recon window present for Week
            cadence, start and end dates are recalculated to 2022-10-01 to 2022-12-31.
        But these are not start and end dates of week. Hence, to correct this, new dates
            are passed again to get the correct dates.

        Args:
            cadence: cadence to process.
            derived_cadence: cadence reconciliation to process.
            start_date: start date of the period to process.
            end_date: end date of the period to process.
            query_type: use case query type.
            current_date: current date to be used in the end date, in case the end date
                is greater than current date so the end date should be the current date.
        """
        new_start_date = self._get_cadence_calculated_date(
            derived_cadence=derived_cadence, base_date=start_date, is_start=True
        )
        new_end_date = self._get_cadence_calculated_date(
            derived_cadence=derived_cadence, base_date=end_date, is_start=False
        )

        if cadence.upper() == "WEEK":
            new_start_date = (
                pendulum.datetime(
                    int(new_start_date.strftime("%Y")),
                    int(new_start_date.strftime("%m")),
                    int(new_start_date.strftime("%d")),
                )
                .start_of("week")
                .replace(tzinfo=None)
            )
            new_end_date = (
                pendulum.datetime(
                    int(new_end_date.strftime("%Y")),
                    int(new_end_date.strftime("%m")),
                    int(new_end_date.strftime("%d")),
                )
                .end_of("week")
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .replace(tzinfo=None)
            )

        new_end_date = new_end_date + timedelta(days=1)

        if new_end_date >= current_date:
            new_end_date = current_date

        if query_type == "NAM":
            new_end_date = new_end_date + timedelta(days=1)

        return new_start_date, new_end_date

    @classmethod
    def _get_cadence_calculated_date(
        cls, derived_cadence: str, base_date: datetime, is_start: bool
    ) -> Union[datetime, DateTime]:  # type: ignore
        cadence_base_date = cls._get_cadence_base_date(derived_cadence, base_date)
        cadence_date_calculated: Union[DateTime, datetime]

        if derived_cadence.upper() == "WEEK":
            cadence_date_calculated = cls._get_calculated_week_date(
                cast(DateTime, cadence_base_date), is_start
            )
        elif derived_cadence.upper() == "MONTH":
            cadence_date_calculated = cls._get_calculated_month_date(
                cast(datetime, cadence_base_date), is_start
            )
        elif derived_cadence.upper() in ["QUARTER", "YEAR"]:
            cadence_date_calculated = cls._get_calculated_quarter_or_year_date(
                cast(DateTime, cadence_base_date), is_start, derived_cadence
            )
        else:
            cadence_date_calculated = cadence_base_date  # type: ignore

        return cadence_date_calculated  # type: ignore

    @classmethod
    def _get_cadence_base_date(
        cls, derived_cadence: str, base_date: datetime
    ) -> Union[datetime, DateTime, str]:  # type: ignore
        """Get start date for the selected cadence.

        Args:
            derived_cadence: cadence reconciliation to process.
            base_date: base date used to compute the start date of the cadence.
        """
        if derived_cadence.upper() in ["DAY", "MONTH"]:
            cadence_date_calculated = base_date
        elif derived_cadence.upper() in ["WEEK", "QUARTER", "YEAR"]:
            cadence_date_calculated = pendulum.datetime(
                int(base_date.strftime("%Y")),
                int(base_date.strftime("%m")),
                int(base_date.strftime("%d")),
            )
        else:
            cadence_date_calculated = "0"  # type: ignore

        return cadence_date_calculated

    @classmethod
    def _get_calculated_week_date(
        cls, cadence_date_calculated: DateTime, is_start: bool
    ) -> DateTime:
        """Get WEEK start/end date.

        Args:
            cadence_date_calculated: base date to compute the week date.
            is_start: flag indicating if we should get the start or end for the cadence.
        """
        if is_start:
            cadence_date_calculated = cadence_date_calculated.start_of("week").replace(
                tzinfo=None
            )
        else:
            cadence_date_calculated = (
                cadence_date_calculated.end_of("week")
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .replace(tzinfo=None)
            )

        return cadence_date_calculated

    @classmethod
    def _get_calculated_month_date(
        cls, cadence_date_calculated: datetime, is_start: bool
    ) -> datetime:
        """Get MONTH start/end date.

        Args:
            cadence_date_calculated: base date to compute the month date.
            is_start: flag indicating if we should get the start or end for the cadence.
        """
        if is_start:
            cadence_date_calculated = cadence_date_calculated - timedelta(
                days=(int(cadence_date_calculated.strftime("%d")) - 1)
            )
        else:
            cadence_date_calculated = datetime(
                int(cadence_date_calculated.strftime("%Y")),
                int(cadence_date_calculated.strftime("%m")),
                calendar.monthrange(
                    int(cadence_date_calculated.strftime("%Y")),
                    int(cadence_date_calculated.strftime("%m")),
                )[1],
            )

        return cadence_date_calculated

    @classmethod
    def _get_calculated_quarter_or_year_date(
        cls, cadence_date_calculated: DateTime, is_start: bool, cadence: str
    ) -> DateTime:
        """Get QUARTER/YEAR start/end date.

        Args:
            cadence_date_calculated: base date to compute the quarter/year date.
            is_start: flag indicating if we should get the start or end for the cadence.
            cadence: selected cadence (possible values: QUARTER or YEAR).
        """
        if is_start:
            cadence_date_calculated = cadence_date_calculated.first_of(
                cadence.lower()
            ).replace(tzinfo=None)
        else:
            cadence_date_calculated = cadence_date_calculated.last_of(
                cadence.lower()
            ).replace(tzinfo=None)

        return cadence_date_calculated


class GABViewManager(object):
    """Class to control the GAB View creation."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    def __init__(
        self,
        query_id: str,
        lookup_query_builder: DataFrame,
        target_database: str,
        target_table: str,
    ):
        """Construct GABViewManager instances.

        Args:
            query_id: gab configuration table use case identifier.
            lookup_query_builder: gab configuration data.
            target_database: target database to write.
            target_table: target table to write.
        """
        self.query_id = query_id
        self.lookup_query_builder = lookup_query_builder
        self.target_database = target_database
        self.target_table = target_table

    def generate_use_case_views(self) -> None:
        """Generate all the use case views.

        Generates the DDLs for each of the views. This DDL is dynamically built based on
        the mappings provided in the config table.
        """
        reconciliation_window = GABUtils.get_json_column_as_dict(
            self.lookup_query_builder, self.query_id, "recon_window"
        )

        cadence_snapshot_status = self._get_cadence_snapshot_status(
            reconciliation_window
        )

        (
            cadences_with_snapshot,
            cadences_without_snapshot,
        ) = self._split_cadence_by_snapshot(cadence_snapshot_status)

        mappings = GABUtils.get_json_column_as_dict(
            self.lookup_query_builder, self.query_id, "mappings"
        )

        for view_name in mappings.keys():
            self._generate_use_case_view(
                mappings,
                view_name,
                cadence_snapshot_status,
                cadences_with_snapshot,
                cadences_without_snapshot,
                self.target_database,
                self.target_table,
                self.query_id,
            )

    @classmethod
    def _generate_use_case_view(
        cls,
        mappings: dict,
        view_name: str,
        cadence_snapshot_status: dict,
        cadences_with_snapshot: list[str],
        cadences_without_snapshot: list[str],
        target_database: str,
        target_table: str,
        query_id: str,
    ) -> None:
        """Generate the selected use case views.

        Args:
            mappings: use case mappings configuration.
            view_name: name of the view to be generated.
            cadence_snapshot_status: cadences to execute with the information if it has
                snapshot.
            cadences_with_snapshot: cadences to execute with snapshot.
            cadences_without_snapshot: cadences to execute without snapshot.
            target_database: target database to write.
            target_table: target table to write.
            query_id: gab configuration table use case identifier.
        """
        view_configuration = mappings[view_name]

        view_dimensions = view_configuration["dimensions"]
        view_metrics = view_configuration["metric"]
        custom_filter = view_configuration["filter"]

        view_filter = " "
        if custom_filter:
            view_filter = " AND " + custom_filter

        (
            dimensions,
            dimensions_and_metrics,
            dimensions_and_metrics_with_alias,
        ) = cls._get_dimensions_and_metrics_from_use_case_view(
            view_dimensions, view_metrics
        )

        (
            final_cols,
            final_calculated_script,
            final_calculated_script_snapshot,
        ) = cls._get_calculated_and_derived_metrics_from_use_case_view(
            view_metrics, view_dimensions, cadence_snapshot_status
        )

        GABViewGenerator(
            cadence_snapshot_status=cadence_snapshot_status,
            target_database=target_database,
            view_name=view_name,
            final_cols=final_cols,
            target_table=target_table,
            dimensions_and_metrics_with_alias=dimensions_and_metrics_with_alias,
            dimensions=dimensions,
            dimensions_and_metrics=dimensions_and_metrics,
            final_calculated_script=final_calculated_script,
            query_id=query_id,
            view_filter=view_filter,
            final_calculated_script_snapshot=final_calculated_script_snapshot,
            without_snapshot_cadences=cadences_without_snapshot,
            with_snapshot_cadences=cadences_with_snapshot,
        ).generate_sql()

    @classmethod
    def _get_dimensions_and_metrics_from_use_case_view(
        cls, view_dimensions: dict, view_metrics: dict
    ) -> Tuple[str, str, str]:
        """Get dimensions and metrics from use case.

        Args:
            view_dimensions: use case configured dimensions.
            view_metrics: use case configured metrics.
        """
        (
            extracted_dimensions_with_alias,
            extracted_dimensions_without_alias,
        ) = GABUtils.extract_columns_from_mapping(
            columns=view_dimensions,
            is_dimension=True,
            extract_column_without_alias=True,
            table_alias="a",
            is_extracted_value_as_name=False,
        )

        dimensions_without_default_columns = [
            extracted_dimension
            for extracted_dimension in extracted_dimensions_without_alias
            if extracted_dimension not in GABDefaults.DIMENSIONS_DEFAULT_COLUMNS.value
        ]

        dimensions = ",".join(dimensions_without_default_columns)
        dimensions_with_alias = ",".join(extracted_dimensions_with_alias)

        (
            extracted_metrics_with_alias,
            extracted_metrics_without_alias,
        ) = GABUtils.extract_columns_from_mapping(
            columns=view_metrics,
            is_dimension=False,
            extract_column_without_alias=True,
            table_alias="a",
            is_extracted_value_as_name=False,
        )
        metrics = ",".join(extracted_metrics_without_alias)
        metrics_with_alias = ",".join(extracted_metrics_with_alias)

        dimensions_and_metrics_with_alias = (
            dimensions_with_alias + "," + metrics_with_alias
        )
        dimensions_and_metrics = dimensions + "," + metrics

        return dimensions, dimensions_and_metrics, dimensions_and_metrics_with_alias

    @classmethod
    def _get_calculated_and_derived_metrics_from_use_case_view(
        cls, view_metrics: dict, view_dimensions: dict, cadence_snapshot_status: dict
    ) -> Tuple[str, str, str]:
        """Get calculated and derived metrics from use case.

        Args:
            view_dimensions: use case configured dimensions.
            view_metrics: use case configured metrics.
            cadence_snapshot_status: cadences to execute with the information if it has
                snapshot.
        """
        calculated_script = []
        calculated_script_snapshot = []
        derived_script = []
        for metric_key, metric_value in view_metrics.items():
            (
                calculated_metrics_script,
                calculated_metrics_script_snapshot,
                derived_metrics_script,
            ) = cls._get_calculated_metrics(
                metric_key, metric_value, view_dimensions, cadence_snapshot_status
            )
            calculated_script += [*calculated_metrics_script]
            calculated_script_snapshot += [*calculated_metrics_script_snapshot]
            derived_script += [*derived_metrics_script]

        joined_calculated_script = cls._join_list_to_string_when_present(
            calculated_script
        )
        joined_calculated_script_snapshot = cls._join_list_to_string_when_present(
            calculated_script_snapshot
        )

        joined_derived = cls._join_list_to_string_when_present(
            to_join=derived_script, starting_value="*,", default_value="*"
        )

        return (
            joined_derived,
            joined_calculated_script,
            joined_calculated_script_snapshot,
        )

    @classmethod
    def _join_list_to_string_when_present(
        cls,
        to_join: list[str],
        separator: str = ",",
        starting_value: str = ",",
        default_value: str = "",
    ) -> str:
        """Join list to string when has values, otherwise return the default value.

        Args:
            to_join: values to join.
            separator: separator to be used in the join.
            starting_value: value to be started before the join.
            default_value: value to be returned if the list is empty.
        """
        return starting_value + separator.join(to_join) if to_join else default_value

    @classmethod
    def _get_cadence_snapshot_status(cls, result: dict) -> dict:
        cadence_snapshot_status = {}
        for k, v in result.items():
            cadence_snapshot_status[k] = next(
                (
                    next(
                        (
                            snap_list["snapshot"]
                            for snap_list in loop_outer_cad.values()
                            if snap_list["snapshot"] == "Y"
                        ),
                        "N",
                    )
                    for loop_outer_cad in v.values()
                    if v
                ),
                "N",
            )

        return cadence_snapshot_status

    @classmethod
    def _split_cadence_by_snapshot(
        cls, cadence_snapshot_status: dict
    ) -> tuple[list[str], list[str]]:
        """Split cadences by the snapshot value.

        Args:
            cadence_snapshot_status: cadences to be split by snapshot status.
        """
        with_snapshot_cadences = []
        without_snapshot_cadences = []

        for key_snap_status, value_snap_status in cadence_snapshot_status.items():
            if value_snap_status == "Y":
                with_snapshot_cadences.append(key_snap_status)
            else:
                without_snapshot_cadences.append(key_snap_status)

        return with_snapshot_cadences, without_snapshot_cadences

    @classmethod
    def _get_calculated_metrics(
        cls,
        metric_key: str,
        metric_value: dict,
        view_dimensions: dict,
        cadence_snapshot_status: dict,
    ) -> tuple[list[str], list[str], list[str]]:
        """Get calculated metrics from use case.

        Args:
            metric_key: use case metric name.
            metric_value: use case metric value.
            view_dimensions: use case configured dimensions.
            cadence_snapshot_status: cadences to execute with the information if it has
                snapshot.
        """
        dim_partition = ",".join([str(i) for i in view_dimensions.keys()][2:])
        dim_partition = "cadence," + dim_partition
        calculated_metrics = metric_value["calculated_metric"]
        derived_metrics = metric_value["derived_metric"]
        calculated_metrics_script: list[str] = []
        calculated_metrics_script_snapshot: list[str] = []
        derived_metrics_script: list[str] = []

        if calculated_metrics:
            (
                calculated_metrics_script,
                calculated_metrics_script_snapshot,
            ) = cls._get_calculated_metric(
                metric_key, calculated_metrics, dim_partition, cadence_snapshot_status
            )

        if derived_metrics:
            derived_metrics_script = cls._get_derived_metrics(derived_metrics)

        return (
            calculated_metrics_script,
            calculated_metrics_script_snapshot,
            derived_metrics_script,
        )

    @classmethod
    def _get_derived_metrics(cls, derived_metric: dict) -> list[str]:
        """Get derived metrics from use case.

        Args:
            derived_metric: use case derived metrics.
        """
        derived_metric_script = []

        for i in range(0, len(derived_metric)):
            derived_formula = str(derived_metric[i]["formula"])
            derived_label = derived_metric[i]["label"]
            derived_metric_script.append(derived_formula + " AS " + derived_label)

        return derived_metric_script

    @classmethod
    def _get_calculated_metric(
        cls,
        metric_key: str,
        calculated_metric: dict,
        dimension_partition: str,
        cadence_snapshot_status: dict,
    ) -> tuple[list[str], list[str]]:
        """Get calculated metrics from use case.

        Args:
            metric_key: use case metric name.
            calculated_metric: use case calculated metrics.
            dimension_partition: dimension partition.
            cadence_snapshot_status: cadences to execute with the information if it has
                snapshot.
        """
        last_cadence_script: list[str] = []
        last_year_cadence_script: list[str] = []
        window_script: list[str] = []
        last_cadence_script_snapshot: list[str] = []
        last_year_cadence_script_snapshot: list[str] = []
        window_script_snapshot: list[str] = []

        if "last_cadence" in calculated_metric:
            (
                last_cadence_script,
                last_cadence_script_snapshot,
            ) = cls._get_cadence_calculated_metric(
                metric_key,
                dimension_partition,
                calculated_metric,
                cadence_snapshot_status,
                "last_cadence",
            )
        if "last_year_cadence" in calculated_metric:
            (
                last_year_cadence_script,
                last_year_cadence_script_snapshot,
            ) = cls._get_cadence_calculated_metric(
                metric_key,
                dimension_partition,
                calculated_metric,
                cadence_snapshot_status,
                "last_year_cadence",
            )
        if "window_function" in calculated_metric:
            window_script, window_script_snapshot = cls._get_window_calculated_metric(
                metric_key,
                dimension_partition,
                calculated_metric,
                cadence_snapshot_status,
            )

        calculated_script = [
            *last_cadence_script,
            *last_year_cadence_script,
            *window_script,
        ]
        calculated_script_snapshot = [
            *last_cadence_script_snapshot,
            *last_year_cadence_script_snapshot,
            *window_script_snapshot,
        ]

        return calculated_script, calculated_script_snapshot

    @classmethod
    def _get_window_calculated_metric(
        cls,
        metric_key: str,
        dimension_partition: str,
        calculated_metric: dict,
        cadence_snapshot_status: dict,
    ) -> tuple[list, list]:
        """Get window calculated metrics from use case.

        Args:
            metric_key: use case metric name.
            dimension_partition: dimension partition.
            calculated_metric: use case calculated metrics.
            cadence_snapshot_status: cadences to execute with the information if it has
                snapshot.
        """
        calculated_script = []
        calculated_script_snapshot = []

        for i in range(0, len(calculated_metric["window_function"])):
            window_function = calculated_metric["window_function"][i]["agg_func"]
            window_function_start = calculated_metric["window_function"][i]["window"][0]
            window_function_end = calculated_metric["window_function"][i]["window"][1]
            window_label = calculated_metric["window_function"][i]["label"]

            calculated_script.append(
                f"""
                NVL(
                    {window_function}({metric_key}) OVER
                    (
                        PARTITION BY {dimension_partition}
                        order by from_date ROWS BETWEEN
                            {str(window_function_start)} PRECEDING
                            AND {str(window_function_end)} PRECEDING
                    ),
                    0
                ) AS
                {window_label}
                """
            )

            if "Y" in cadence_snapshot_status.values():
                calculated_script_snapshot.append(
                    f"""
                    NVL(
                        {window_function}({metric_key}) OVER
                        (
                            PARTITION BY {dimension_partition} ,rn
                            order by from_date ROWS BETWEEN
                                {str(window_function_start)} PRECEDING
                                AND {str(window_function_end)} PRECEDING
                        ),
                        0
                    ) AS
                    {window_label}
                    """
                )

        return calculated_script, calculated_script_snapshot

    @classmethod
    def _get_cadence_calculated_metric(
        cls,
        metric_key: str,
        dimension_partition: str,
        calculated_metric: dict,
        cadence_snapshot_status: dict,
        cadence: str,
    ) -> tuple[list, list]:
        """Get cadence calculated metrics from use case.

        Args:
            metric_key: use case metric name.
            calculated_metric: use case calculated metrics.
            dimension_partition: dimension partition.
            cadence_snapshot_status: cadences to execute with the information if it has
                snapshot.
            cadence: cadence to process.
        """
        calculated_script = []
        calculated_script_snapshot = []

        for i in range(0, len(calculated_metric[cadence])):
            cadence_lag = cls._get_cadence_item_lag(calculated_metric, cadence, i)
            cadence_label = calculated_metric[cadence][i]["label"]

            calculated_script.append(
                cls._get_cadence_lag_statement(
                    metric_key,
                    cadence_lag,
                    dimension_partition,
                    cadence_label,
                    snapshot=False,
                    cadence=cadence,
                )
            )

            if "Y" in cadence_snapshot_status.values():
                calculated_script_snapshot.append(
                    cls._get_cadence_lag_statement(
                        metric_key,
                        cadence_lag,
                        dimension_partition,
                        cadence_label,
                        snapshot=True,
                        cadence=cadence,
                    )
                )

        return calculated_script, calculated_script_snapshot

    @classmethod
    def _get_cadence_item_lag(
        cls, calculated_metric: dict, cadence: str, item: int
    ) -> str:
        """Get calculated metric item lag.

        Args:
            calculated_metric: use case calculated metrics.
            cadence: cadence to process.
            item: metric item.
        """
        return str(calculated_metric[cadence][item]["window"])

    @classmethod
    def _get_cadence_lag_statement(
        cls,
        metric_key: str,
        cadence_lag: str,
        dimension_partition: str,
        cadence_label: str,
        snapshot: bool,
        cadence: str,
    ) -> str:
        """Get cadence lag statement.

        Args:
            metric_key: use case metric name.
            cadence_lag: cadence window lag.
            dimension_partition: dimension partition.
            cadence_label: cadence name.
            snapshot: indicate if the snapshot is enabled.
            cadence: cadence to process.
        """
        cadence_lag_statement = ""
        if cadence == "last_cadence":
            cadence_lag_statement = (
                "NVL(LAG("
                + metric_key
                + ","
                + cadence_lag
                + ") OVER(PARTITION BY "
                + dimension_partition
                + (",rn" if snapshot else "")
                + " order by from_date),0) AS "
                + cadence_label
            )
        elif cadence == "last_year_cadence":
            cadence_lag_statement = (
                "NVL(LAG("
                + metric_key
                + ","
                + cadence_lag
                + ") OVER(PARTITION BY "
                + dimension_partition
                + (",rn" if snapshot else "")
                + """,
                    case
                        when cadence in ('DAY','MONTH','QUARTER')
                            then struct(month(from_date), day(from_date))
                        when cadence in('WEEK')
                            then struct(weekofyear(from_date+1),1)
                    end order by from_date),0) AS """
                + cadence_label
            )
        else:
            cls._LOGGER.error(f"Cadence {cadence} not implemented yet")

        return cadence_lag_statement
