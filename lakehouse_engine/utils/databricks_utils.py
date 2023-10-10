"""Utilities for databricks operations."""
import json
from typing import Any, Tuple

from pyspark.sql import SparkSession

from lakehouse_engine.core.exec_env import ExecEnv


class DatabricksUtils(object):
    """Databricks utilities class."""

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

            dbutils = DBUtils(spark)
        except ImportError:
            import IPython

            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

    @staticmethod
    def get_databricks_job_information() -> Tuple[str, str]:
        """Get notebook context from running acon.

        Returns:
            Dict containing databricks notebook context.
        """
        if "local" in ExecEnv.SESSION.sparkContext.applicationId:
            return "local", "local"
        else:
            dbutils = DatabricksUtils.get_db_utils(ExecEnv.SESSION)
            notebook_context = json.loads(
                (
                    dbutils.notebook.entry_point.getDbutils()
                    .notebook()
                    .getContext()
                    .toJson()
                )
            )

            return notebook_context["tags"].get("orgId"), notebook_context["tags"].get(
                "jobName"
            )
