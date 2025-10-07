"""Module to define Heartbeat Sensor algorithm behavior."""

import re
from typing import Optional

from delta import DeltaTable
from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    current_timestamp,
    lit,
    regexp_replace,
    row_number,
    trim,
    upper,
)
from pyspark.sql.window import Window

from lakehouse_engine.algorithms.algorithm import Algorithm
from lakehouse_engine.algorithms.sensors.sensor import Sensor
from lakehouse_engine.core.definitions import (
    HEARTBEAT_SENSOR_UPDATE_SET,
    HeartbeatConfigSpec,
    HeartbeatSensorSource,
    HeartbeatStatus,
    SensorStatus,
)
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.core.sensor_manager import (
    SensorJobRunManager,
    SensorUpstreamManager,
)
from lakehouse_engine.terminators.sensor_terminator import SensorTerminator
from lakehouse_engine.utils.databricks_utils import DatabricksUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler


class Heartbeat(Algorithm):
    """Class representing a Heartbeat to check if the upstream has new data."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    def __init__(self, acon: dict):
        """Construct Heartbeat instances.

        Args:
            acon: algorithm configuration.
        """
        self.spec: HeartbeatConfigSpec = HeartbeatConfigSpec.create_from_acon(acon=acon)

    def execute(self) -> None:
        """Execute the heartbeat."""
        latest_event_current_timestamp = current_timestamp()
        heartbeat_sensor_delta_table = DeltaTable.forName(
            ExecEnv.SESSION,
            self.spec.heartbeat_sensor_db_table,
        )
        sensor_source = self.spec.sensor_source

        active_jobs_from_heartbeat_control_table_df = self._get_active_heartbeat_jobs(
            heartbeat_sensor_delta_table, sensor_source
        )

        for (
            control_table_df_row
        ) in active_jobs_from_heartbeat_control_table_df.collect():

            sensor_acon = self._get_sensor_acon_from_heartbeat(
                self.spec, control_table_df_row
            )

            sensors_with_new_data = self._execute_batch_of_sensor(
                sensor_acon, control_table_df_row
            )

            if sensors_with_new_data:

                self._update_heartbeat_status_with_sensor_info(
                    active_jobs_from_heartbeat_control_table_df,
                    heartbeat_sensor_delta_table,
                    self._get_heartbeat_sensor_condition(sensors_with_new_data),
                    latest_event_current_timestamp,
                    sensor_source,
                )

    @classmethod
    def _get_active_heartbeat_jobs(
        cls, heartbeat_sensor_delta_table: DeltaTable, sensor_source: str
    ) -> DataFrame:
        """Get UNPAUSED and NULL or COMPLETED status record from control table.

        :param heartbeat_sensor_delta_table: DeltaTable for heartbeat sensor.
        :param sensor_source: source system from Spec(e.g. sap_b4, delta, kafka etc.).

        Returns:
            A control table DataFrame containing records for specified sensor_source
            that are UNPAUSED and have a status of either NULL or COMPLETED.
        """
        full_control_table = heartbeat_sensor_delta_table.toDF()

        filtered_control_table = full_control_table.filter(
            f"lower(sensor_source) == '{sensor_source}'"
        ).filter(
            "job_state == 'UNPAUSED' and (status is null OR status == 'COMPLETED')"
        )

        return filtered_control_table

    @classmethod
    def generate_unique_column_values(cls, main_col: str, col_to_append: str) -> str:
        """Generate a unique value by appending columns and replacing specific chars.

        Generate a unique value by appending another column and replacing spaces,
        dots, and colons with underscores for consistency.

        :param main_col: The primary column value.
        :param col_to_append: Column value to append for uniqueness.

        Returns:
            A unique, combined column value.
        """
        return f"{re.sub(r'[ :.]', '_', main_col)}_{col_to_append}"

    @classmethod
    def _get_sensor_acon_from_heartbeat(
        cls, heartbeat_spec: HeartbeatConfigSpec, control_table_df_row: Row
    ) -> dict:
        """Create sensor acon from heartbeat config and specifications.

        :param heartbeat_spec: Heartbeat specifications.
        :param control_table_df_row: Control table active records Dataframe Row.

        Returns:
            The sensor acon dict.
        """
        sensors_to_execute: dict = {
            "sensor_id": (
                cls.generate_unique_column_values(
                    control_table_df_row["sensor_id"],
                    control_table_df_row["trigger_job_id"],
                )
            ),  # 1. sensor_id can be same for two or more different trigger_job_id
            # 2. Replacing colon,space,dot(.) with underscore(_) is required to get the
            # checkpoint_location fixed in case of delta_table and kafka source
            "assets": [
                cls.generate_unique_column_values(
                    control_table_df_row["asset_description"],
                    control_table_df_row["trigger_job_id"],
                )
            ],
            "control_db_table_name": heartbeat_spec.lakehouse_engine_sensor_db_table,
            "input_spec": {
                "spec_id": "sensor_upstream",
                "read_type": control_table_df_row["sensor_read_type"],
                "data_format": heartbeat_spec.data_format,
                "db_table": (
                    control_table_df_row["sensor_id"]
                    if heartbeat_spec.data_format == "delta"
                    else None
                ),
                "options": heartbeat_spec.options,
                "location": (
                    (
                        heartbeat_spec.base_trigger_file_location
                        + "/"
                        + control_table_df_row["sensor_id"]
                    )
                    if heartbeat_spec.base_trigger_file_location is not None
                    else None
                ),
                "schema": heartbeat_spec.schema_dict,
            },
            "preprocess_query": control_table_df_row["preprocess_query"],
            "base_checkpoint_location": heartbeat_spec.base_checkpoint_location,
            "fail_on_empty_result": False,
        }

        final_sensors_to_execute = cls._enhance_sensor_acon_extra_options(
            heartbeat_spec, control_table_df_row, sensors_to_execute
        )

        return final_sensors_to_execute

    @classmethod
    def _enhance_sensor_acon_extra_options(
        cls,
        heartbeat_spec: HeartbeatConfigSpec,
        control_table_df_row: Row,
        sensors_to_execute: dict,
    ) -> dict:
        """Enhance sensor acon with extra options for specific source system.

        :param heartbeat_spec: Heartbeat specifications.
        :param control_table_df_row: Control table active records Dataframe Row.
        :param sensors_to_execute: sensor acon dictionary from previous step.

        Returns:
            The sensor acon dict having enhanced options for specific sensor_source.
        """
        LATEST_FETCH_EVENT_TIMESTAMP = (
            control_table_df_row.latest_event_fetched_timestamp
        )

        upstream_key = control_table_df_row["upstream_key"]

        upstream_value = (
            LATEST_FETCH_EVENT_TIMESTAMP.strftime("%Y%m%d%H%M%S")
            if LATEST_FETCH_EVENT_TIMESTAMP is not None
            else "19000101000000"
        )

        if control_table_df_row.sensor_source.lower() in [
            HeartbeatSensorSource.SAP_B4.value,
            HeartbeatSensorSource.SAP_BW.value,
        ]:

            sensors_to_execute["input_spec"]["options"]["prepareQuery"] = (
                SensorUpstreamManager.generate_sensor_sap_logchain_query(
                    chain_id=control_table_df_row.sensor_id,
                    dbtable=heartbeat_spec.jdbc_db_table,
                )
            )
            sensors_to_execute["input_spec"]["options"]["query"] = (
                SensorUpstreamManager.generate_filter_exp_query(
                    sensor_id=control_table_df_row.sensor_id,
                    filter_exp="?upstream_key > '?upstream_value'",
                    control_db_table_name=(
                        heartbeat_spec.lakehouse_engine_sensor_db_table
                    ),
                    upstream_key=upstream_key,
                    upstream_value=upstream_value,
                )
            )

        elif (
            control_table_df_row.sensor_source.lower()
            == HeartbeatSensorSource.LMU_DELTA_TABLE.value
        ):

            sensors_to_execute["preprocess_query"] = (
                SensorUpstreamManager.generate_filter_exp_query(
                    sensor_id=control_table_df_row.sensor_id,
                    filter_exp="?upstream_key > '?upstream_value'",
                    control_db_table_name=(
                        heartbeat_spec.lakehouse_engine_sensor_db_table
                    ),
                    upstream_key=upstream_key,
                    upstream_value=upstream_value,
                )
            )

        elif (
            control_table_df_row.sensor_source.lower()
            == HeartbeatSensorSource.KAFKA.value
        ):

            kafka_options = cls._get_all_kafka_options(
                heartbeat_spec.kafka_configs,
                control_table_df_row["sensor_id"],
                heartbeat_spec.kafka_secret_scope,
            )

            sensors_to_execute["input_spec"]["options"] = kafka_options

        return sensors_to_execute

    @classmethod
    def _get_all_kafka_options(
        cls,
        kafka_configs: dict,
        kafka_sensor_id: str,
        kafka_secret_scope: str,
    ) -> dict:
        """Get all Kafka extra options for sensor ACON.

        Read all heartbeat sensor related kafka config dynamically based on
        data product name or any other prefix which should match with sensor_id prefix.

        :param kafka_configs: kafka config read from yaml file.
        :param kafka_sensor_id: kafka topic for which new event to be fetched.
        :param kafka_secret_scope: secret scope used for kafka processing.

        Returns:
            The sensor acon dict having enhanced options for kafka source.
        """
        sensor_id_desc = kafka_sensor_id.split(":")
        dp_name_filter = sensor_id_desc[0].strip()
        KAFKA_TOPIC = sensor_id_desc[1].strip()

        KAFKA_BOOTSTRAP_SERVERS = kafka_configs[dp_name_filter][
            "kafka_bootstrap_servers_list"
        ]
        KAFKA_TRUSTSTORE_LOCATION = kafka_configs[dp_name_filter][
            "kafka_ssl_truststore_location"
        ]
        KAFKA_KEYSTORE_LOCATION = kafka_configs[dp_name_filter][
            "kafka_ssl_keystore_location"
        ]
        KAFKA_TRUSTSTORE_PSWD_SECRET_KEY = kafka_configs[dp_name_filter][
            "truststore_pwd_secret_key"
        ]
        KAFKA_TRUSTSTORE_PSWD = (
            DatabricksUtils.get_db_utils(ExecEnv.SESSION).secrets.get(
                scope=kafka_secret_scope,
                key=KAFKA_TRUSTSTORE_PSWD_SECRET_KEY,
            )
            if KAFKA_TRUSTSTORE_PSWD_SECRET_KEY
            else None
        )
        KAFKA_KEYSTORE_PSWD_SECRET_KEY = kafka_configs[dp_name_filter][
            "keystore_pwd_secret_key"
        ]
        KAFKA_KEYSTORE_PSWD = (
            DatabricksUtils.get_db_utils(ExecEnv.SESSION).secrets.get(
                scope=kafka_secret_scope,
                key=KAFKA_KEYSTORE_PSWD_SECRET_KEY,
            )
            if KAFKA_KEYSTORE_PSWD_SECRET_KEY
            else None
        )

        kafka_options_dict = {
            "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "subscribe": KAFKA_TOPIC,
            "startingOffsets": "earliest",
            "kafka.security.protocol": "SSL",
            "kafka.ssl.truststore.location": KAFKA_TRUSTSTORE_LOCATION,
            "kafka.ssl.truststore.password": KAFKA_TRUSTSTORE_PSWD,
            "kafka.ssl.keystore.location": KAFKA_KEYSTORE_LOCATION,
            "kafka.ssl.keystore.password": KAFKA_KEYSTORE_PSWD,
        }

        return kafka_options_dict

    @classmethod
    def _execute_batch_of_sensor(
        cls, sensor_acon: dict, control_table_df_row: Row
    ) -> dict:
        """Execute sensor acon to fetch NEW EVENT AVAILABLE for sensor source system.

        :param sensor_acon: sensor acon created from heartbeat config and specs.
        :param control_table_df_row: Control table active records Dataframe Row.

        Returns:
            Dict containing sensor_id and trigger_job_id for sensor with new data.
        """
        sensors_with_new_data: dict = {}

        cls._LOGGER.info(f"Executing sensor: {sensor_acon}")
        has_new_data = Sensor(sensor_acon).execute()

        if has_new_data:
            sensors_with_new_data["sensor_id"] = control_table_df_row["sensor_id"]
            sensors_with_new_data["trigger_job_id"] = control_table_df_row[
                "trigger_job_id"
            ]

        return sensors_with_new_data

    @classmethod
    def _get_heartbeat_sensor_condition(
        cls,
        sensors_with_new_data: dict,
    ) -> Optional[str]:
        """Get heartbeat sensor new event available condition.

        :param sensors_with_new_data: dict having NEW_EVENT_AVAILABLE sensor_id record.

        Returns:
            String having condition for sensor having new data available.
        """
        heartbeat_sensor_with_new_event_available = (
            f"(sensor_id = '{sensors_with_new_data['sensor_id']}' AND "
            f"trigger_job_id = '{sensors_with_new_data['trigger_job_id']}')"
        )

        return heartbeat_sensor_with_new_event_available

    @classmethod
    def _update_heartbeat_status_with_sensor_info(
        cls,
        heartbeat_sensor_jobs: DataFrame,
        heartbeat_sensor_delta_table: DeltaTable,
        heartbeat_with_new_event_available_condition: str,
        latest_event_current_timestamp: Column,
        sensor_source: str,
    ) -> None:
        """Update heartbeat status with sensor info.

        :param heartbeat_sensor_jobs: active UNPAUSED jobs from Control table dataframe.
        :param heartbeat_sensor_delta_table: heartbeat sensor Delta table.
        :param heartbeat_with_new_event_available_condition: new event available cond.
        :param latest_event_current_timestamp: timestamp when new event was captured.
        """
        if heartbeat_with_new_event_available_condition:
            sensors_with_new_event_available = (
                heartbeat_sensor_jobs.filter(
                    heartbeat_with_new_event_available_condition
                )
                .withColumn("status", lit(HeartbeatStatus.NEW_EVENT_AVAILABLE.value))
                .withColumn("status_change_timestamp", current_timestamp())
                .withColumn(
                    "latest_event_fetched_timestamp", latest_event_current_timestamp
                )
            )

            new_event_merge_condition = f"""target.sensor_id = src.sensor_id AND
                target.trigger_job_id = src.trigger_job_id AND
                target.sensor_source = '{sensor_source}'"""

            if sensors_with_new_event_available.count() > 0:
                cls.update_heartbeat_control_table(
                    heartbeat_sensor_delta_table,
                    sensors_with_new_event_available,
                    new_event_merge_condition,
                )
        else:
            cls._LOGGER.info("No sensors to execute!")

    @classmethod
    def update_heartbeat_control_table(
        cls,
        heartbeat_sensor_delta_table: DeltaTable,
        updated_data: DataFrame,
        heartbeat_control_table_merge_condition: str,
    ) -> None:
        """Update heartbeat control table with the new data.

        :param heartbeat_sensor_delta_table: db_table heartbeat sensor control table.
        :param updated_data: data to update the control table.
        :param heartbeat_control_table_merge_condition: merge condition for table.
        """
        cls._LOGGER.info(f"updated data: {updated_data}")

        heartbeat_sensor_delta_table.alias("target").merge(
            updated_data.alias("src"),
            (heartbeat_control_table_merge_condition),
        ).whenMatchedUpdate(
            set=HEARTBEAT_SENSOR_UPDATE_SET
        ).whenNotMatchedInsertAll().execute()

    @classmethod
    def get_heartbeat_jobs_to_trigger(
        cls,
        heartbeat_sensor_db_table: str,
        heartbeat_sensor_control_table_df: DataFrame,
    ) -> list[Row]:
        """Get heartbeat jobs to trigger.

        Check if all the dependencies are satisfied to trigger the job.
        dependency_flag column to be checked for all sensor_id and
        trigger_job_id combination keeping status as NEW_EVENT_AVAILABLE in mind.

        Check dependencies based trigger_job_id. From all control table record having
        status as NEW_EVENT_AVAILABLE, then it will fetch status and dependency_flag
        for all records having same trigger_job_id. If trigger_job_id, status,
        dependency_flag combination is same for all dependencies, Get distinct record
        and do count level aggregation for trigger_job_id, dependency_flag.

        Count level aggregation based on trigger_job_id, dependency_flag picks all
        those trigger_job_id which doesn`t satisfy dependency as it denotes there are
        more than one record present having dependency_flag = "TRUE" and status is
        different for same trigger_job_id. If count is not more than 1, means condition
        satisfied, Job id will be considered for triggering.

        If trigger_job_id, status, dependency_flag combination is not same for all
        dependencies, aggregated count will result in more than one record and it will
        go under jobs_to_not_trigger and will not trigger job.

        :param heartbeat_sensor_db_table: heartbeat sensor table name.
        :param heartbeat_sensor_control_table_df: Dataframe for heartbeat control table.
        :return: list of jobs to be triggered.
        """
        # Get all distinct trigger_job_id where status is NEW_EVENT_AVAILABLE
        trigger_jobs_new_events_df = (
            heartbeat_sensor_control_table_df.filter(
                f"status == '{HeartbeatStatus.NEW_EVENT_AVAILABLE.value}'"
            )
            .select(col("trigger_job_id"))
            .distinct()
        )

        # Get distinct trigger_job_id, status, dependency_flag for control table records
        full_data_df = (
            ExecEnv.SESSION.table(heartbeat_sensor_db_table)
            .select(
                col("trigger_job_id"),
                col("status"),
                upper(col("dependency_flag")).alias("dependency_flag"),
            )
            .distinct()
        )

        # Join NEW_EVENT_AVAILABLE records with full table to get all dependencies
        # based on trigger_job_id. dependency_flag = "TRUE" needs to be checked as
        # we are only concerned with records where dependencies needs to be checked.
        full_data_trigger_job_id = col("full_data.trigger_job_id")
        dep_flag_comparison = trim(upper(col("dependency_flag"))) == "TRUE"
        jobs_with_new_events_df = (
            full_data_df.alias("full_data")
            .join(
                trigger_jobs_new_events_df.alias("jobs_with_new_events"),
                col("jobs_with_new_events.trigger_job_id") == full_data_trigger_job_id,
                "inner",
            )
            .select(
                full_data_trigger_job_id,
                col("full_data.status"),
                col("full_data.dependency_flag"),
            )
        ).filter(dep_flag_comparison)

        # Count level aggregation based on trigger_job_id, dependency_flag picks all
        # those trigger_job_id which doesn`t satisfy dependency as it denotes there
        # are more than one record present having dependency_flag = "TRUE" and status
        # is different for same trigger_job_id.
        jobs_to_not_trigger_with_new_event_df = (
            jobs_with_new_events_df.filter(dep_flag_comparison)
            .groupBy("trigger_job_id", "dependency_flag")
            .agg(count("trigger_job_id").alias("count"))
            .where(col("count") > 1)
        )

        jobs_to_trigger_df = (
            jobs_with_new_events_df.alias("full_data")
            .join(
                jobs_to_not_trigger_with_new_event_df.alias("jobs_to_not_trigger"),
                (col("jobs_to_not_trigger.trigger_job_id") == full_data_trigger_job_id),
                "left_anti",
            )
            .groupBy("trigger_job_id", "status")
            .agg(count("trigger_job_id").alias("count"))
            .where(col("count") == 1)
        )

        jobs_to_trigger_df = jobs_to_trigger_df.select("trigger_job_id").distinct()
        jobs_to_trigger = jobs_to_trigger_df.collect()

        return jobs_to_trigger

    @classmethod
    def get_anchor_job_record(
        cls, heartbeat_sensor_table_df: DataFrame, job_id: str, sensor_source: str
    ) -> DataFrame:
        """Identify anchor jobs from the control table.

        Using trigger_job_id as the partition key, ordered by status_change_timestamp
        in descending order and sensor_id in ascending order, filtered by the specific
        sensor_source.

        This method partitions records by trigger_job_id, orders them by
        status_change_timestamp (descending) and sensor_id (ascending), and filters
        by the specified sensor_source. Filtering on sensor_source makes sure if
        current source is eligible for triggering the job and updates or not. This
        process ensures that only the appropriate single record triggers the job and
        the control table is updated accordingly. This approach eliminates redundant
        triggers and unnecessary updates.

        :param heartbeat_sensor_table_df: Heartbeat sensor control table Dataframe.
        :param job_id: Trigger job_id from table for which dependency also satisfies.
        :param sensor_source: source of the heartbeat sensor record.

        Returns:
            Control table DataFrame containing anchor job records valid for triggering.
        """
        heartbeat_anchor_records_df = heartbeat_sensor_table_df.filter(
            col("trigger_job_id") == job_id
        ).withColumn(
            "row_no",
            row_number().over(
                Window.partitionBy("trigger_job_id").orderBy(
                    col("status_change_timestamp").desc(), col("sensor_id").asc()
                )
            ),
        )

        heartbeat_anchor_records_df = heartbeat_anchor_records_df.filter(
            f"row_no = 1 AND sensor_source = '{sensor_source}'"
        ).drop("row_no")

        return heartbeat_anchor_records_df

    def heartbeat_sensor_trigger_jobs(self) -> None:
        """Get heartbeat jobs to trigger.

        :param self.spec: HeartbeatConfigSpec having config and control table spec.
        """
        heartbeat_sensor_db_table = self.spec.heartbeat_sensor_db_table
        sensor_source = self.spec.sensor_source

        heartbeat_sensor_delta_table = DeltaTable.forName(
            ExecEnv.SESSION, heartbeat_sensor_db_table
        )

        heartbeat_sensor_control_table_df = ExecEnv.SESSION.table(
            heartbeat_sensor_db_table
        ).filter(
            f"lower(sensor_source) == '{sensor_source}' and (job_state == 'UNPAUSED')"
        )

        jobs_to_trigger = self.get_heartbeat_jobs_to_trigger(
            heartbeat_sensor_db_table, heartbeat_sensor_control_table_df
        )

        heartbeat_sensor_table_df = ExecEnv.SESSION.table(heartbeat_sensor_db_table)
        final_df: DataFrame = None

        for row in jobs_to_trigger:
            run_id = None
            exception = None

            heartbeat_anchor_job_records_df = self.get_anchor_job_record(
                heartbeat_sensor_table_df, row["trigger_job_id"], sensor_source
            )

            if heartbeat_anchor_job_records_df.take(1):
                run_id, exception = SensorJobRunManager.run_job(
                    row["trigger_job_id"], self.spec.token, self.spec.domain
                )

                if exception is None and run_id is not None:
                    status_df = (
                        heartbeat_sensor_table_df.filter(
                            (col("trigger_job_id") == row["trigger_job_id"])
                        )
                        .withColumn("job_start_timestamp", current_timestamp())
                        .withColumn("status", lit(HeartbeatStatus.IN_PROGRESS.value))
                        .withColumn("status_change_timestamp", current_timestamp())
                    )
                    final_df = final_df.union(status_df) if final_df else status_df

        if final_df is not None:
            in_progress_merge_condition = """target.sensor_id = src.sensor_id AND
                target.trigger_job_id = src.trigger_job_id AND
                target.sensor_source = src.sensor_source"""

            self.update_heartbeat_control_table(
                heartbeat_sensor_delta_table, final_df, in_progress_merge_condition
            )

    @classmethod
    def _read_heartbeat_sensor_data_feed_csv(
        cls, heartbeat_sensor_data_feed_path: str
    ) -> DataFrame:
        """Get rows to insert or delete in heartbeat_sensor table.

        It reads the CSV file stored from the `heartbeat_sensor_data_feed_path` and
        perform UPSERT and DELETE in control table.
        - **heartbeat_sensor_data_feed_path**: path where CSV file is stored.
        """
        data_feed_csv_df = (
            ExecEnv.SESSION.read.format("csv")
            .option("header", True)
            .load(heartbeat_sensor_data_feed_path)
        )
        data_feed_csv_df = data_feed_csv_df.withColumn(
            "job_state", upper(col("job_state"))
        )
        return data_feed_csv_df

    @classmethod
    def merge_control_table_data_feed_records(
        cls,
        heartbeat_sensor_control_table: str,
        heartbeat_sensor_data_feed_csv_df: DataFrame,
    ) -> None:
        """Perform merge operation based on the condition.

        It reads the CSV file stored at `heartbeat_sensor_data_feed_path` folder
        and perform UPSERT and DELETE in control table.
        - **heartbeat_sensor_control_table**: Heartbeat sensor control table.
        - **heartbeat_sensor_data_feed_csv_df**: Dataframe after reading CSV file.
        """
        delta_table = DeltaTable.forName(
            ExecEnv.SESSION, heartbeat_sensor_control_table
        )

        delta_table.alias("trgt").merge(
            heartbeat_sensor_data_feed_csv_df.alias("source"),
            (
                """source.sensor_id = trgt.sensor_id and
                trgt.trigger_job_id = source.trigger_job_id"""
            ),
        ).whenNotMatchedInsert(
            values={
                "sensor_source": "source.sensor_source",
                "sensor_id": "source.sensor_id",
                "sensor_read_type": "source.sensor_read_type",
                "asset_description": "source.asset_description",
                "upstream_key": "source.upstream_key",
                "preprocess_query": "source.preprocess_query",
                "latest_event_fetched_timestamp": "null",
                "trigger_job_id": "source.trigger_job_id",
                "trigger_job_name": "source.trigger_job_name",
                "status": "null",
                "status_change_timestamp": "null",
                "job_start_timestamp": "null",
                "job_end_timestamp": "null",
                "job_state": "source.job_state",
                "dependency_flag": "source.dependency_flag",
            }
        ).whenMatchedUpdate(
            set={
                "sensor_source": "source.sensor_source",
                "sensor_id": "source.sensor_id",
                "sensor_read_type": "source.sensor_read_type",
                "asset_description": "source.asset_description",
                "upstream_key": "source.upstream_key",
                "preprocess_query": "source.preprocess_query",
                "latest_event_fetched_timestamp": "trgt.latest_event_fetched_timestamp",
                "trigger_job_id": "source.trigger_job_id",
                "trigger_job_name": "source.trigger_job_name",
                "status": "trgt.status",
                "status_change_timestamp": "trgt.status_change_timestamp",
                "job_start_timestamp": "trgt.job_start_timestamp",
                "job_end_timestamp": "trgt.job_end_timestamp",
                "job_state": "source.job_state",
                "dependency_flag": "source.dependency_flag",
            }
        ).whenNotMatchedBySourceDelete().execute()

    @classmethod
    def heartbeat_sensor_control_table_data_feed(
        cls,
        heartbeat_sensor_data_feed_path: str,
        heartbeat_sensor_control_table: str,
    ) -> None:
        """Control table Data feeder.

        It reads the CSV file stored at `heartbeat_sensor_data_feed_path` and
        perform UPSERT and DELETE in control table.
        - **heartbeat_sensor_data_feed_path**: path where CSV file is stored.
        - **heartbeat_sensor_control_table**: CONTROL table of Heartbeat sensor.
        """
        heartbeat_sensor_data_feed_csv_df = cls._read_heartbeat_sensor_data_feed_csv(
            heartbeat_sensor_data_feed_path
        )

        cls.merge_control_table_data_feed_records(
            heartbeat_sensor_control_table, heartbeat_sensor_data_feed_csv_df
        )

    @classmethod
    def update_sensor_processed_status(
        cls,
        sensor_table: str,
        job_id_filter_control_table_df: DataFrame,
    ) -> None:
        """UPDATE sensor PROCESSED_NEW_DATA status.

        Update sensor control table with PROCESSED_NEW_DATA status and
        status_change_timestamp for the triggered job.

        Args:
            sensor_table: lakehouse engine sensor table name.
            job_id_filter_control_table_df: Job Id filtered Heartbeat sensor
            control table dataframe.
        """
        sensor_id_df = job_id_filter_control_table_df.withColumn(
            "sensor_table_sensor_id",
            concat_ws(
                "_",
                regexp_replace(col("sensor_id"), r"[ :\.]", "_"),
                col("trigger_job_id"),
            ),
        )

        for row in sensor_id_df.select("sensor_table_sensor_id").collect():
            SensorTerminator.update_sensor_status(
                sensor_id=row["sensor_table_sensor_id"],
                control_db_table_name=sensor_table,
                status=SensorStatus.PROCESSED_NEW_DATA.value,
                assets=None,
            )

    @classmethod
    def update_heartbeat_sensor_completion_status(
        cls,
        heartbeat_sensor_control_table: str,
        sensor_table: str,
        job_id: str,
    ) -> None:
        """UPDATE heartbeat sensor status.

        Update heartbeat sensor control table with COMPLETE status and
        job_end_timestamp for the triggered job.
        Update sensor control table with PROCESSED_NEW_DATA status and
        status_change_timestamp for the triggered job.

        Args:
            job_id: job_id of the running job. It will refer to
            trigger_job_id in Control table.
            sensor_table: lakehouse engine sensor table name.
            heartbeat_sensor_control_table: Heartbeat sensor control table.
        """
        job_id_filter_control_table_df = (
            ExecEnv.SESSION.table(heartbeat_sensor_control_table)
            .filter(col("trigger_job_id") == job_id)
            .withColumn("status", lit(HeartbeatStatus.COMPLETED.value))
            .withColumn("status_change_timestamp", current_timestamp())
            .withColumn("job_end_timestamp", current_timestamp())
        )

        cls.update_sensor_processed_status(sensor_table, job_id_filter_control_table_df)

        delta_table = DeltaTable.forName(
            ExecEnv.SESSION, heartbeat_sensor_control_table
        )

        (
            delta_table.alias("target")
            .merge(
                job_id_filter_control_table_df.alias("source"),
                (
                    f"""target.sensor_source = source.sensor_source and
                target.sensor_id = source.sensor_id and
                target.trigger_job_id = '{job_id}'"""
                ),
            )
            .whenMatchedUpdate(
                set={
                    "target.status": "source.status",
                    "target.status_change_timestamp": "source.status_change_timestamp",
                    "target.job_end_timestamp": "source.job_end_timestamp",
                }
            )
            .execute()
        )
