"""Utilities for databricks operations."""

import json
from typing import Any, Tuple

from pyspark.sql import SparkSession


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
        if "local" in spark.getActiveSession().conf.get("spark.app.id"):
            return "local", "local"
        else:
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
