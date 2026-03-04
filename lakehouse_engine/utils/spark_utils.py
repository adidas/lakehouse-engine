"""Utilities to facilitate spark dataframe management."""

from pyspark.sql import DataFrame

from lakehouse_engine.core.exec_env import ExecEnv


class SparkUtils(object):
    """Spark utils that help retrieve and manage dataframes."""

    @staticmethod
    def create_temp_view(
        df: DataFrame, view_name: str, return_prefix: bool = False
    ) -> None | str:
        """Create a temporary view from a dataframe.

        If the execution environment is serverless, it creates a temporary view,
        otherwise it creates a global temporary view.
        Serverless environments don't support global temporary views, so we need to
        create a temporary view in that case, but it still gets accessible from other
        queries in the same session.
        In non-serverless environments, we create a global temporary view to make
        sure it is accessible from other sessions as well.

        Args:
            df: dataframe to create the view from.
            view_name: name of the view to create.
            return_prefix: whether to return the prefix to use in queries
            for this view or not.

        Returns:
            None or the prefix to use in queries for this view, depending on the
            value of return_prefix.
        """
        if ExecEnv.IS_SERVERLESS:
            df.createOrReplaceTempView(view_name)
            prefix = ""
        else:
            df.createOrReplaceGlobalTempView(view_name)
            prefix = "global_temp."
        if return_prefix:
            return prefix
        return None
