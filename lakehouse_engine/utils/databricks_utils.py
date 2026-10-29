"""Utilities for databricks operations."""

import ast
import importlib
import json
import os
import re
from typing import Any

from pyspark.sql import SparkSession

from lakehouse_engine.core.definitions import EngineStats
from lakehouse_engine.utils.logging_handler import LoggingHandler


class DatabricksUtils:
    """Databricks utilities class."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    @staticmethod
    def is_serverless_workload() -> bool:
        """Check if the current databricks workload is serverless.

        Returns:
            True if the current databricks workload is serverless, False otherwise.
        """
        return os.getenv("IS_SERVERLESS", "false").lower() == "true"

    @staticmethod
    def get_db_utils(spark: SparkSession) -> Any:
        """Get db utils on databricks.

        Args:
            spark: spark session.

        Returns:
            Dbutils from databricks.
        """
        try:
            dbutils_module = importlib.import_module("pyspark.dbutils")
            dbutils_class = dbutils_module.DBUtils

            if "dbutils" not in locals():
                dbutils = dbutils_class(spark)
            else:
                dbutils = locals().get("dbutils")
        except ImportError:
            ipython_module = importlib.import_module("IPython")
            dbutils = ipython_module.get_ipython().user_ns["dbutils"]
        return dbutils

    @staticmethod
    def get_databricks_job_information(spark: SparkSession) -> tuple[str, str]:
        """Get notebook context from running acon.

        Args:
            spark: spark session.

        Returns:
            Dict containing databricks notebook context.
        """
        dbutils = DatabricksUtils.get_db_utils(spark)
        notebook_context = json.loads(
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .safeToJson()
        )

        return notebook_context["attributes"].get("orgId"), notebook_context[
            "attributes"
        ].get("jobName")

    @staticmethod
    def get_dbutils_entry_name(entry: Any) -> str:
        """Extract entry name from a dbutils.fs.ls response item.

        Args:
            entry: A dbutils.fs.ls response item.

        Returns:
            The name of the entry.
        """
        return str(getattr(entry, "name", "") or "").strip()

    @staticmethod
    def get_dbutils_entry_path(entry: Any) -> str:
        """Extract entry path from a dbutils.fs.ls response item.

        Args:
            entry: A dbutils.fs.ls response item.

        Returns:
            The path of the entry.
        """
        return str(getattr(entry, "path", "") or "").strip()

    @staticmethod
    def check_dbutils_path_exists(dbutils: Any, path: str) -> bool:
        """Check if a path exists using dbutils.

        Args:
            dbutils: The dbutils instance.
            path: The path to check.

        Returns:
            True if the path exists, False otherwise.
        """
        try:
            dbutils.fs.ls(path)
            return True
        except Exception:  # noqa: BLE001
            return False

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
                raw_value = ExecEnv.SESSION.conf.get(array_key[0], "[]") or "[]"
                array_values = ast.literal_eval(raw_value)
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
            import importlib

            from lakehouse_engine.core.exec_env import ExecEnv

            context_module = importlib.import_module(
                "dbruntime.databricks_repl_context"
            )
            context = context_module.get_context()
            for key, attr in EngineStats.DEF_DATABRICKS_CONTEXT_KEYS.items():
                if key == "dp_name":
                    usage_stats[key] = DatabricksUtils._get_dp_name(
                        getattr(context, attr, "")
                    )
                elif key == "environment":
                    usage_stats[key] = ExecEnv.get_environment()
                else:
                    usage_stats[key] = getattr(context, attr, None)
        except Exception as ex:  # noqa: BLE001
            cls._LOGGER.error(f"Error getting Serverless Usage Context: {ex}")
