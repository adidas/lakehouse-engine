"""Module to define Gold Asset Builder algorithm behavior."""

import copy
from datetime import datetime, timedelta
from typing import Union

import pendulum
from jinja2 import Template
from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from lakehouse_engine.algorithms.algorithm import Algorithm
from lakehouse_engine.core.definitions import (
    GABCadence,
    GABCombinedConfiguration,
    GABDefaults,
    GABKeys,
    GABReplaceableKeys,
    GABSpec,
    GABStartOfWeek,
)
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.core.gab_manager import GABCadenceManager, GABViewManager
from lakehouse_engine.core.gab_sql_generator import (
    GABDeleteGenerator,
    GABInsertGenerator,
)
from lakehouse_engine.utils.gab_utils import GABPartitionUtils, GABUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler


class GAB(Algorithm):
    """Class representing the gold asset builder."""

    _LOGGER = LoggingHandler(__name__).get_logger()
    _SPARK_DEFAULT_PARALLELISM_CONFIG = (
        "spark.sql.sources.parallelPartitionDiscovery.parallelism"
    )
    _SPARK_DEFAULT_PARALLELISM_VALUE = "10000"

    def __init__(self, acon: dict):
        """Construct GAB instances.

        Args:
            acon: algorithm configuration.
        """
        self.spec: GABSpec = GABSpec.create_from_acon(acon=acon)

    def execute(self) -> None:
        """Execute the Gold Asset Builder."""
        self._LOGGER.info(f"Reading {self.spec.lookup_table} as lkp_query_builder")
        lookup_query_builder_df = ExecEnv.SESSION.read.table(self.spec.lookup_table)
        ExecEnv.SESSION.read.table(self.spec.calendar_table).createOrReplaceTempView(
            "df_cal"
        )
        self._LOGGER.info(f"Generating calendar from {self.spec.calendar_table}")

        query_label = self.spec.query_label_filter
        queue = self.spec.queue_filter
        cadence = self.spec.cadence_filter

        self._LOGGER.info(f"Query Label Filter {query_label}")
        self._LOGGER.info(f"Queue Filter {queue}")
        self._LOGGER.info(f"Cadence Filter {cadence}")

        gab_path = self.spec.gab_base_path
        self._LOGGER.info(f"Gab Base Path {gab_path}")

        lookup_query_builder_df = lookup_query_builder_df.filter(
            (
                (lookup_query_builder_df.query_label.isin(query_label))
                & (lookup_query_builder_df.queue.isin(queue))
                & (lookup_query_builder_df.is_active != lit("N"))
            )
        )

        lookup_query_builder_df.cache()

        for use_case in lookup_query_builder_df.collect():
            self._process_use_case(
                use_case=use_case,
                lookup_query_builder=lookup_query_builder_df,
                selected_cadences=cadence,
                gab_path=gab_path,
            )

        lookup_query_builder_df.unpersist()

    def _process_use_case(
        self,
        use_case: Row,
        lookup_query_builder: DataFrame,
        selected_cadences: list[str],
        gab_path: str,
    ) -> None:
        """Process each gab use case.

        Args:
            use_case: gab use case to process.
            lookup_query_builder: gab configuration data.
            selected_cadences: selected cadences to process.
            gab_path: gab base path used to get the use case stages sql files.
        """
        self._LOGGER.info(f"Executing use case: {use_case['query_label']}")

        reconciliation = GABUtils.get_json_column_as_dict(
            lookup_query_builder=lookup_query_builder,
            query_id=use_case["query_id"],
            query_column="recon_window",
        )
        self._LOGGER.info(f"reconcilation window - {reconciliation}")
        configured_cadences = list(reconciliation.keys())

        stages = GABUtils.get_json_column_as_dict(
            lookup_query_builder=lookup_query_builder,
            query_id=use_case["query_id"],
            query_column="intermediate_stages",
        )
        self._LOGGER.info(f"intermediate stages - {stages}")

        self._LOGGER.info(f"selected_cadences: {selected_cadences}")
        self._LOGGER.info(f"configured_cadences: {configured_cadences}")
        cadences = self._get_filtered_cadences(selected_cadences, configured_cadences)
        self._LOGGER.info(f"filtered cadences - {cadences}")

        latest_run_date, latest_config_date = self._get_latest_usecase_data(
            use_case["query_id"]
        )
        self._LOGGER.info(f"latest_config_date: {latest_config_date}")
        self._LOGGER.info(f"latest_run_date: - {latest_run_date}")
        self._set_use_case_stage_template_file(stages, gab_path, use_case)
        processed_cadences = []

        for cadence in cadences:
            is_cadence_processed = self._process_use_case_query_cadence(
                cadence,
                reconciliation,
                use_case,
                stages,
                lookup_query_builder,
            )
            if is_cadence_processed:
                processed_cadences.append(is_cadence_processed)

        if processed_cadences:
            self._generate_ddl(
                latest_config_date=latest_config_date,
                latest_run_date=latest_run_date,
                query_id=use_case["query_id"],
                lookup_query_builder=lookup_query_builder,
            )
        else:
            self._LOGGER.info(
                f"Skipping use case {use_case['query_label']}. No cadence processed "
                "for the use case."
            )

    @classmethod
    def _set_use_case_stage_template_file(
        cls, stages: dict, gab_path: str, use_case: Row
    ) -> None:
        """Set templated file for each stage.

        Args:
            stages: use case stages with their configuration.
            gab_path: gab base path used to get the use case stages SQL files.
            use_case: gab use case to process.
        """
        cls._LOGGER.info("Reading templated file for each stage...")

        for i in range(1, len(stages) + 1):
            stage = stages[str(i)]
            stage_file_path = stage["file_path"]
            full_path = gab_path + stage_file_path
            cls._LOGGER.info(f"Stage file path is: {full_path}")
            file_read = open(full_path, "r").read()
            templated_file = file_read.replace(
                "replace_offset_value", str(use_case["timezone_offset"])
            )
            stage["templated_file"] = templated_file
            stage["full_file_path"] = full_path

    def _process_use_case_query_cadence(
        self,
        cadence: str,
        reconciliation: dict,
        use_case: Row,
        stages: dict,
        lookup_query_builder: DataFrame,
    ) -> bool:
        """Identify use case reconciliation window and cadence.

        Args:
            cadence:  cadence to process.
            reconciliation: configured use case reconciliation window.
            use_case: gab use case to process.
            stages: use case stages with their configuration.
            lookup_query_builder: gab configuration data.
        """
        selected_reconciliation_window = {}
        selected_cadence = reconciliation.get(cadence)
        self._LOGGER.info(f"Processing cadence: {cadence}")
        self._LOGGER.info(f"Reconciliation Window - {selected_cadence}")

        if selected_cadence:
            selected_reconciliation_window = selected_cadence.get("recon_window")

        self._LOGGER.info(f"{cadence}: {self.spec.start_date} - {self.spec.end_date}")

        start_of_week = use_case["start_of_the_week"]

        self._set_week_configuration_by_uc_start_of_week(start_of_week)

        cadence_configuration_at_end_date = (
            GABUtils.get_cadence_configuration_at_end_date(self.spec.end_date)
        )

        reconciliation_cadences = GABUtils().get_reconciliation_cadences(
            cadence=cadence,
            selected_reconciliation_window=selected_reconciliation_window,
            cadence_configuration_at_end_date=cadence_configuration_at_end_date,
            rerun_flag=self.spec.rerun_flag,
        )

        start_date_str = GABUtils.format_datetime_to_default(self.spec.start_date)
        end_date_str = GABUtils.format_datetime_to_default(self.spec.end_date)

        for reconciliation_cadence, snapshot_flag in reconciliation_cadences.items():
            self._process_reconciliation_cadence(
                reconciliation_cadence=reconciliation_cadence,
                snapshot_flag=snapshot_flag,
                cadence=cadence,
                start_date_str=start_date_str,
                end_date_str=end_date_str,
                use_case=use_case,
                lookup_query_builder=lookup_query_builder,
                stages=stages,
            )

        return (cadence in reconciliation.keys()) or (
            reconciliation_cadences is not None
        )

    def _process_reconciliation_cadence(
        self,
        reconciliation_cadence: str,
        snapshot_flag: str,
        cadence: str,
        start_date_str: str,
        end_date_str: str,
        use_case: Row,
        lookup_query_builder: DataFrame,
        stages: dict,
    ) -> None:
        """Process use case reconciliation window.

        Reconcile the pre-aggregated data to cover the late events.

        Args:
            reconciliation_cadence: reconciliation to process.
            snapshot_flag: flag indicating if for this cadence the snapshot is enabled.
            cadence: cadence to process.
            start_date_str: start date of the period to process.
            end_date_str: end date of the period to process.
            use_case: gab use case to process.
            lookup_query_builder: gab configuration data.
            stages: use case stages with their configuration.

        Example:
            Cadence: week;
            Reconciliation: monthly;
            This means every weekend previous week aggregations will be calculated and
                on month end we will reconcile the numbers calculated for last 4 weeks
                to readjust the number for late events.
        """
        (
            window_start_date,
            window_end_date,
            filter_start_date,
            filter_end_date,
        ) = GABCadenceManager().extended_window_calculator(
            cadence,
            reconciliation_cadence,
            self.spec.current_date,
            start_date_str,
            end_date_str,
            use_case["query_type"],
            self.spec.rerun_flag,
            snapshot_flag,
        )

        if use_case["timezone_offset"]:
            filter_start_date = filter_start_date + timedelta(
                hours=use_case["timezone_offset"]
            )
            filter_end_date = filter_end_date + timedelta(
                hours=use_case["timezone_offset"]
            )

        filter_start_date_str = GABUtils.format_datetime_to_default(filter_start_date)
        filter_end_date_str = GABUtils.format_datetime_to_default(filter_end_date)

        partition_end = GABUtils.format_datetime_to_default(
            (window_end_date - timedelta(days=1))
        )

        window_start_date_str = GABUtils.format_datetime_to_default(window_start_date)
        window_end_date_str = GABUtils.format_datetime_to_default(window_end_date)

        partition_filter = GABPartitionUtils.get_partition_condition(
            filter_start_date_str, partition_end
        )

        self._LOGGER.info(
            "extended window for start and end dates are: "
            f"{filter_start_date_str} - {filter_end_date_str}"
        )

        unpersist_list = []

        for i in range(1, len(stages) + 1):
            stage = stages[str(i)]
            templated_file = stage["templated_file"]
            stage_file_path = stage["full_file_path"]

            templated = self._process_use_case_query_step(
                stage=stages[str(i)],
                templated_file=templated_file,
                use_case=use_case,
                reconciliation_cadence=reconciliation_cadence,
                cadence=cadence,
                snapshot_flag=snapshot_flag,
                window_start_date=window_start_date_str,
                partition_end=partition_end,
                filter_start_date=filter_start_date_str,
                filter_end_date=filter_end_date_str,
                partition_filter=partition_filter,
            )

            temp_stage_view_name = self._create_stage_view(
                templated,
                stages[str(i)],
                window_start_date_str,
                window_end_date_str,
                use_case["query_id"],
                use_case["query_label"],
                cadence,
                stage_file_path,
            )
            unpersist_list.append(temp_stage_view_name)

        insert_success = self._generate_view_statement(
            query_id=use_case["query_id"],
            cadence=cadence,
            temp_stage_view_name=temp_stage_view_name,
            lookup_query_builder=lookup_query_builder,
            window_start_date=window_start_date_str,
            window_end_date=window_end_date_str,
            query_label=use_case["query_label"],
        )
        self._LOGGER.info(f"Inserted data to generate the view: {insert_success}")

        self._unpersist_cached_views(unpersist_list)

    def _process_use_case_query_step(
        self,
        stage: dict,
        templated_file: str,
        use_case: Row,
        reconciliation_cadence: str,
        cadence: str,
        snapshot_flag: str,
        window_start_date: str,
        partition_end: str,
        filter_start_date: str,
        filter_end_date: str,
        partition_filter: str,
    ) -> str:
        """Process each use case step.

        Process any intermediate view defined in the gab configuration table as step for
            the use case.

        Args:
            stage: stage to process.
            templated_file: sql file to process at this stage.
            use_case: gab use case to process.
            reconciliation_cadence: configured use case reconciliation window.
            cadence: cadence to process.
            snapshot_flag: flag indicating if for this cadence the snapshot is enabled.
            window_start_date: start date for the configured stage.
            partition_end: end date for the configured stage.
            filter_start_date: filter start date to replace in the stage query.
            filter_end_date: filter end date to replace in the stage query.
            partition_filter: partition condition.
        """
        filter_col = stage["project_date_column"]
        if stage["filter_date_column"]:
            filter_col = stage["filter_date_column"]

        # dummy value to avoid empty error if empty on the configuration
        project_col = stage.get("project_date_column", "X")

        gab_base_configuration_copy = copy.deepcopy(
            GABCombinedConfiguration.COMBINED_CONFIGURATION.value
        )

        for item in gab_base_configuration_copy.values():
            self._update_rendered_item_cadence(
                reconciliation_cadence, cadence, project_col, item  # type: ignore
            )

        (
            rendered_date,
            rendered_to_date,
            join_condition,
        ) = self._get_cadence_configuration(
            gab_base_configuration_copy,
            cadence,
            reconciliation_cadence,
            snapshot_flag,
            use_case["start_of_the_week"],
            project_col,
            window_start_date,
            partition_end,
        )

        rendered_file = self._render_template_query(
            templated=templated_file,
            cadence=cadence,
            start_of_the_week=use_case["start_of_the_week"],
            query_id=use_case["query_id"],
            rendered_date=rendered_date,
            filter_start_date=filter_start_date,
            filter_end_date=filter_end_date,
            filter_col=filter_col,
            timezone_offset=use_case["timezone_offset"],
            join_condition=join_condition,
            partition_filter=partition_filter,
            rendered_to_date=rendered_to_date,
        )

        return rendered_file

    @classmethod
    def _get_filtered_cadences(
        cls, selected_cadences: list[str], configured_cadences: list[str]
    ) -> list[str]:
        """Get filtered cadences.

        Get the intersection of user selected cadences and use case configured cadences.

        Args:
            selected_cadences: user selected cadences.
            configured_cadences: use case configured cadences.
        """
        return (
            configured_cadences
            if "All" in selected_cadences
            else GABCadence.order_cadences(
                list(set(selected_cadences).intersection(configured_cadences))
            )
        )

    def _get_latest_usecase_data(self, query_id: str) -> tuple[datetime, datetime]:
        """Get latest use case data.

        Args:
            query_id: use case query id.
        """
        return (
            self._get_latest_run_date(query_id),
            self._get_latest_use_case_date(query_id),
        )

    def _get_latest_run_date(self, query_id: str) -> datetime:
        """Get latest use case run date.

        Args:
            query_id: use case query id.
        """
        last_success_run_sql = """
            SELECT run_start_time
            FROM {database}.gab_log_events
            WHERE query_id = {query_id}
            AND stage_name = 'Final Insert'
            AND status = 'Success'
            ORDER BY 1 DESC
            LIMIT 1
            """.format(  # nosec: B608
            database=self.spec.target_database, query_id=query_id
        )
        try:
            latest_run_date: datetime = ExecEnv.SESSION.sql(
                last_success_run_sql
            ).collect()[0][0]
        except Exception:
            latest_run_date = datetime.strptime(
                "2020-01-01", GABDefaults.DATE_FORMAT.value
            )

        return latest_run_date

    def _get_latest_use_case_date(self, query_id: str) -> datetime:
        """Get latest use case configured date.

        Args:
            query_id: use case query id.
        """
        query_config_sql = """
            SELECT lh_created_on
            FROM {lkp_query_builder}
            WHERE query_id = {query_id}
        """.format(  # nosec: B608
            lkp_query_builder=self.spec.lookup_table,
            query_id=query_id,
        )

        latest_config_date: datetime = ExecEnv.SESSION.sql(query_config_sql).collect()[
            0
        ][0]

        return latest_config_date

    @classmethod
    def _set_week_configuration_by_uc_start_of_week(cls, start_of_week: str) -> None:
        """Set week configuration by use case start of week.

        Args:
            start_of_week: use case start of week (MONDAY or SUNDAY).
        """
        if start_of_week.upper() == "MONDAY":
            pendulum.week_starts_at(pendulum.MONDAY)
            pendulum.week_ends_at(pendulum.SUNDAY)
        elif start_of_week.upper() == "SUNDAY":
            pendulum.week_starts_at(pendulum.SUNDAY)
            pendulum.week_ends_at(pendulum.SATURDAY)
        else:
            raise NotImplementedError(
                f"The requested {start_of_week} is not implemented."
                "Supported `start_of_week` values: [MONDAY, SUNDAY]"
            )

    @classmethod
    def _update_rendered_item_cadence(
        cls, reconciliation_cadence: str, cadence: str, project_col: str, item: dict
    ) -> None:
        """Override item properties based in the rendered item cadence.

        Args:
            reconciliation_cadence: configured use case reconciliation window.
            cadence: cadence to process.
            project_col: use case projection date column name.
            item: predefined use case combination.
        """
        rendered_item = cls._get_rendered_item_cadence(
            reconciliation_cadence, cadence, project_col, item
        )
        item["join_select"] = rendered_item["join_select"]
        item["project_start"] = rendered_item["project_start"]
        item["project_end"] = rendered_item["project_end"]

    @classmethod
    def _get_rendered_item_cadence(
        cls, reconciliation_cadence: str, cadence: str, project_col: str, item: dict
    ) -> dict:
        """Update pre-configured gab parameters with use case data.

        Args:
            reconciliation_cadence: configured use case reconciliation window.
            cadence: cadence to process.
            project_col: use case projection date column name.
            item: predefined use case combination.
        """
        return {
            GABKeys.JOIN_SELECT: (
                item[GABKeys.JOIN_SELECT]
                .replace(GABReplaceableKeys.CONFIG_WEEK_START, "Monday")
                .replace(
                    GABReplaceableKeys.RECONCILIATION_CADENCE,
                    reconciliation_cadence,
                )
                .replace(GABReplaceableKeys.CADENCE, cadence)
            ),
            GABKeys.PROJECT_START: (
                item[GABKeys.PROJECT_START]
                .replace(GABReplaceableKeys.CADENCE, cadence)
                .replace(GABReplaceableKeys.DATE_COLUMN, project_col)
            ),
            GABKeys.PROJECT_END: (
                item[GABKeys.PROJECT_END]
                .replace(GABReplaceableKeys.CADENCE, cadence)
                .replace(GABReplaceableKeys.DATE_COLUMN, project_col)
            ),
        }

    @classmethod
    def _get_cadence_configuration(
        cls,
        use_case_configuration: dict,
        cadence: str,
        reconciliation_cadence: str,
        snapshot_flag: str,
        start_of_week: str,
        project_col: str,
        window_start_date: str,
        partition_end: str,
    ) -> tuple[str, str, str]:
        """Get use case configuration fields to replace pre-configured parameters.

        Args:
            use_case_configuration: use case configuration.
            cadence: cadence to process.
            reconciliation_cadence: cadence to be reconciliated.
            snapshot_flag: flag indicating if for this cadence the snapshot is enabled.
            start_of_week: use case start of week (MONDAY or SUNDAY).
            project_col: use case projection date column name.
            window_start_date: start date for the configured stage.
            partition_end: end date for the configured stage.

        Returns:
            rendered_from_date: projection start date.
            rendered_to_date: projection end date.
            join_condition: string containing the join condition to replace in the
                templated query by jinja substitution.
        """
        cadence_dict = next(
            (
                dict(configuration)
                for configuration in use_case_configuration.values()
                if (
                    (cadence in configuration["cadence"])
                    and (reconciliation_cadence in configuration["recon"])
                    and (snapshot_flag in configuration["snap_flag"])
                    and (
                        GABStartOfWeek.get_start_of_week()[start_of_week.upper()]
                        in configuration["week_start"]
                    )
                )
            ),
            None,
        )
        rendered_from_date = None
        rendered_to_date = None
        join_condition = None

        if cadence_dict:
            rendered_from_date = (
                cadence_dict[GABKeys.PROJECT_START]
                .replace(GABReplaceableKeys.CADENCE, cadence)
                .replace(GABReplaceableKeys.DATE_COLUMN, project_col)
            )
            rendered_to_date = (
                cadence_dict[GABKeys.PROJECT_END]
                .replace(GABReplaceableKeys.CADENCE, cadence)
                .replace(GABReplaceableKeys.DATE_COLUMN, project_col)
            )

            if cadence_dict[GABKeys.JOIN_SELECT]:
                join_condition = """
                 inner join (
                     {join_select} from df_cal
                     where calendar_date
                     between '{bucket_start}' and '{bucket_end}'
                 )
                 df_cal on date({date_column})
                     between df_cal.cadence_start_date and df_cal.cadence_end_date
                 """.format(
                    join_select=cadence_dict[GABKeys.JOIN_SELECT],
                    bucket_start=window_start_date,
                    bucket_end=partition_end,
                    date_column=project_col,
                )

        return rendered_from_date, rendered_to_date, join_condition

    def _render_template_query(
        self,
        templated: str,
        cadence: str,
        start_of_the_week: str,
        query_id: str,
        rendered_date: str,
        filter_start_date: str,
        filter_end_date: str,
        filter_col: str,
        timezone_offset: str,
        join_condition: str,
        partition_filter: str,
        rendered_to_date: str,
    ) -> str:
        """Replace jinja templated parameters in the SQL with the actual data.

        Args:
            templated: templated sql file to process at this stage.
            cadence: cadence to process.
            start_of_the_week: use case start of week (MONDAY or SUNDAY).
            query_id: gab configuration table use case identifier.
            rendered_date: projection start date.
            filter_start_date: filter start date to replace in the stage query.
            filter_end_date: filter end date to replace in the stage query.
            filter_col: use case projection date column name.
            timezone_offset: timezone offset configured in the use case.
            join_condition: string containing the join condition.
            partition_filter: partition condition.
            rendered_to_date: projection end date.
        """
        return Template(templated).render(
            cadence="'{cadence}' as cadence".format(cadence=cadence),
            cadence_run=cadence,
            week_start=start_of_the_week,
            query_id="'{query_id}' as query_id".format(query_id=query_id),
            project_date_column=rendered_date,
            target_table=self.spec.target_table,
            database=self.spec.source_database,
            start_date=filter_start_date,
            end_date=filter_end_date,
            filter_date_column=filter_col,
            offset_value=timezone_offset,
            joins=join_condition if join_condition else "",
            partition_filter=partition_filter,
            to_date=rendered_to_date,
        )

    def _create_stage_view(
        self,
        rendered_template: str,
        stage: dict,
        window_start_date: str,
        window_end_date: str,
        query_id: str,
        query_label: str,
        cadence: str,
        stage_file_path: str,
    ) -> str:
        """Create each use case stage view.

        Each stage has a specific order and refer to a specific SQL to be executed.

        Args:
            rendered_template: rendered stage SQL file.
            stage: stage to process.
            window_start_date: start date for the configured stage.
            window_end_date: end date for the configured stage.
            query_id: gab configuration table use case identifier.
            query_label: gab configuration table use case name.
            cadence: cadence to process.
            stage_file_path: full stage file path (gab path + stage path).
        """
        run_start_time = datetime.now()
        creation_status: str
        error_message: Union[Exception, str]

        try:
            tmp = ExecEnv.SESSION.sql(rendered_template)
            num_partitions = ExecEnv.SESSION.conf.get(
                self._SPARK_DEFAULT_PARALLELISM_CONFIG,
                self._SPARK_DEFAULT_PARALLELISM_VALUE,
            )

            if stage["repartition"]:
                if stage["repartition"].get("numPartitions"):
                    num_partitions = stage["repartition"]["numPartitions"]

                if stage["repartition"].get("keys"):
                    tmp = tmp.repartition(
                        int(num_partitions), *stage["repartition"]["keys"]
                    )
                    self._LOGGER.info("Repartitioned on given Key(s)")
                else:
                    tmp = tmp.repartition(int(num_partitions))
                    self._LOGGER.info("Repartitioned on given partition count")

            temp_step_view_name: str = stage["table_alias"]
            tmp.createOrReplaceTempView(temp_step_view_name)

            if stage["storage_level"]:
                ExecEnv.SESSION.sql(
                    "CACHE TABLE {tbl} "
                    "OPTIONS ('storageLevel' '{type}')".format(
                        tbl=temp_step_view_name,
                        type=stage["storage_level"],
                    )
                )
                ExecEnv.SESSION.sql(
                    "SELECT COUNT(*) FROM {tbl}".format(  # nosec: B608
                        tbl=temp_step_view_name
                    )
                )
                self._LOGGER.info(f"Cached stage view - {temp_step_view_name} ")

            creation_status = "Success"
            error_message = "NA"
        except Exception as err:
            creation_status = "Failed"
            error_message = err
            raise err
        finally:
            run_end_time = datetime.now()
            GABUtils().logger(
                run_start_time,
                run_end_time,
                window_start_date,
                window_end_date,
                query_id,
                query_label,
                cadence,
                stage_file_path,
                rendered_template,
                creation_status,
                error_message,
                self.spec.target_database,
            )

        return temp_step_view_name

    def _generate_view_statement(
        self,
        query_id: str,
        cadence: str,
        temp_stage_view_name: str,
        lookup_query_builder: DataFrame,
        window_start_date: str,
        window_end_date: str,
        query_label: str,
    ) -> bool:
        """Feed use case data to the insights table (default: unified use case table).

        Args:
            query_id: gab configuration table use case identifier.
            cadence: cadence to process.
            temp_stage_view_name: name of the temp view generated by the stage.
            lookup_query_builder: gab configuration data.
            window_start_date: start date for the configured stage.
            window_end_date: end date for the configured stage.
            query_label: gab configuration table use case name.
        """
        run_start_time = datetime.now()
        creation_status: str
        error_message: Union[Exception, str]

        GABDeleteGenerator(
            query_id=query_id,
            cadence=cadence,
            temp_stage_view_name=temp_stage_view_name,
            lookup_query_builder=lookup_query_builder,
            target_database=self.spec.target_database,
            target_table=self.spec.target_table,
        ).generate_sql()

        gen_ins = GABInsertGenerator(
            query_id=query_id,
            cadence=cadence,
            final_stage_table=temp_stage_view_name,
            lookup_query_builder=lookup_query_builder,
            target_database=self.spec.target_database,
            target_table=self.spec.target_table,
        ).generate_sql()
        try:
            ExecEnv.SESSION.sql(gen_ins)

            creation_status = "Success"
            error_message = "NA"
            inserted = True
        except Exception as err:
            creation_status = "Failed"
            error_message = err
            raise
        finally:
            run_end_time = datetime.now()
            GABUtils().logger(
                run_start_time,
                run_end_time,
                window_start_date,
                window_end_date,
                query_id,
                query_label,
                cadence,
                "Final Insert",
                gen_ins,
                creation_status,
                error_message,
                self.spec.target_database,
            )

        return inserted

    @classmethod
    def _unpersist_cached_views(cls, unpersist_list: list[str]) -> None:
        """Unpersist cached views.

        Args:
            unpersist_list: list containing the view names to unpersist.
        """
        [
            ExecEnv.SESSION.sql("UNCACHE TABLE {tbl}".format(tbl=i))
            for i in unpersist_list
        ]

    def _generate_ddl(
        self,
        latest_config_date: datetime,
        latest_run_date: datetime,
        query_id: str,
        lookup_query_builder: DataFrame,
    ) -> None:
        """Generate the actual gold asset.

        It will create and return the view containing all specified dimensions, metrics
            and computed metric for each cadence/reconciliation window.

        Args:
            latest_config_date: latest use case configuration date.
            latest_run_date: latest use case run date.
            query_id: gab configuration table use case identifier.
            lookup_query_builder: gab configuration data.
        """
        if str(latest_config_date) > str(latest_run_date):
            GABViewManager(
                query_id=query_id,
                lookup_query_builder=lookup_query_builder,
                target_database=self.spec.target_database,
                target_table=self.spec.target_table,
            ).generate_use_case_views()
        else:
            self._LOGGER.info(
                "View is not being re-created as there are no changes in the "
                "configuration after the latest run"
            )
