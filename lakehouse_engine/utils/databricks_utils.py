"""Utilities for databricks operations."""

import ast
import json
import os
import re
from typing import Any, Tuple

from pyspark.sql import SparkSession

from lakehouse_engine.core.definitions import EngineStats
from lakehouse_engine.utils.logging_handler import LoggingHandler


class DatabricksUtils(object):
    """Databricks utilities class."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    @staticmethod
    def is_serverless_workload() -> bool:
        """Check if the current databricks workload is serverless.

        Returns:
            True if the current databricks workload is serverless, False otherwise.
        """
        if os.getenv("IS_SERVERLESS", "false").lower() == "true":
            return True
        else:
            return False

    @staticmethod
    def get_db_utils(spark: SparkSession) -> Any:
        """Get db utils on databricks.

        Args:
            spark: spark session.

        Returns:
            Dbutils from databricks.
        """
        try:
            from pyspark.dbutils import DBUtils

            if "dbutils" not in locals():
                dbutils = DBUtils(spark)
            else:
                dbutils = locals().get("dbutils")
        except ImportError:
            import IPython

            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

    @staticmethod
    def get_databricks_job_information(spark: SparkSession) -> Tuple[str, str]:
        """Get notebook context from running acon.

        Args:
            spark: spark session.

        Returns:
            Dict containing databricks notebook context.
        """
        dbutils = DatabricksUtils.get_db_utils(spark)
        notebook_context = json.loads(
            (
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .safeToJson()
            )
        )

        return notebook_context["attributes"].get("orgId"), notebook_context[
            "attributes"
        ].get("jobName")

    @staticmethod
    def _get_dp_name(job_name: str) -> str:
        """Extract the dp_name from a Databricks job name.

        The job name is expected to have a suffix separated by '-', and the dp_name is
        the part before the last '-'. Only '_' is used in the rest of the job name.
        E.g. 'sadp-template-my_awesome_job'

        Args:
            job_name: The Databricks job name string.

        Returns:
            The extracted dp_name.
        """
        return job_name.rsplit("-", 1)[0] if job_name and "-" in job_name else job_name

    @staticmethod
    def get_spark_conf_values(usage_stats: dict, spark_confs: dict) -> None:
        """Get information from spark session configurations.

        Args:
            usage_stats: usage_stats dictionary file.
            spark_confs: optional dictionary with the spark tags to be used when
                collecting the engine usage.
        """
        from lakehouse_engine.core.exec_env import ExecEnv

        spark_confs = (
            EngineStats.DEF_SPARK_CONFS
            if spark_confs is None
            else EngineStats.DEF_SPARK_CONFS | spark_confs
        )

        for spark_conf_key, spark_conf_value in spark_confs.items():
            # whenever the spark_conf_value has #, it means it is an array, so we need
            # to split it and adequately process it
            if "#" in spark_conf_value:
                array_key = spark_conf_value.split("#")
                array_values = ast.literal_eval(
                    ExecEnv.SESSION.conf.get(array_key[0], "[]")
                )
                final_value = [
                    key_val["value"]
                    for key_val in array_values
                    if key_val["key"] == array_key[1]
                ]
                usage_stats[spark_conf_key] = (
                    final_value[0] if len(final_value) > 0 else ""
                )
            else:
                usage_stats[spark_conf_key] = ExecEnv.SESSION.conf.get(
                    spark_conf_value, ""
                )

        run_id_extracted = re.search("run-([1-9]\\w+)", usage_stats.get("run_id", ""))
        usage_stats["run_id"] = run_id_extracted.group(1) if run_id_extracted else ""

    @classmethod
    def get_usage_context_for_serverless(cls, usage_stats: dict) -> None:
        """Get information from the execution environment for serverless scenarios.

        Since in serverless environments we might not have access to all the spark
        confs we want to collect, we will try to get that information from the
        execution environment when possible.

        Args:
            usage_stats: usage_stats dictionary file.
        """
        try:
            from dbruntime.databricks_repl_context import get_context

            from lakehouse_engine.core.exec_env import ExecEnv

            context = get_context()
            for key, attr in EngineStats.DEF_DATABRICKS_CONTEXT_KEYS.items():
                if key == "dp_name":
                    usage_stats[key] = DatabricksUtils._get_dp_name(
                        getattr(context, attr, None)
                    )
                elif key == "environment":
                    usage_stats[key] = ExecEnv.get_environment()
                else:
                    usage_stats[key] = getattr(context, attr, None)
        except Exception as ex:
            cls._LOGGER.error(f"Error getting Serverless Usage Context: {ex}")
