"""Module to take care of creating a singleton of the execution environment class."""
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

from lakehouse_engine.utils.logging_handler import LoggingHandler


class ExecEnv(object):
    """Represents the basic resources regarding the engine execution environment.

    Currently, it is solely used to encapsulate the logic to get a Spark session.
    """

    SESSION: SparkSession
    _LOGGER = LoggingHandler(__name__).get_logger()
    DEFAULT_AWS_REGION = "eu-west-1"

    @classmethod
    def get_or_create(
        cls,
        session: SparkSession = None,
        enable_hive_support: bool = True,
        app_name: str = None,
        config: dict = None,
    ) -> None:
        """Get or create an execution environment session (currently Spark).

        It instantiates a singleton session that can be accessed anywhere from the
        lakehouse engine.

        Args:
            session: spark session.
            enable_hive_support: whether to enable hive support or not.
            app_name: application name.
            config: extra spark configs to supply to the spark session.
        """
        default_config = {
            "spark.databricks.delta.optimizeWrite.enabled": True,
            "spark.sql.adaptive.enabled": True,
            "spark.databricks.delta.merge.enableLowShuffle": True,
        }
        cls._LOGGER.info(
            f"Using the following default configs you may want to override them for "
            f"your job: {default_config}"
        )
        final_config: dict = {**default_config, **(config if config else {})}
        cls._LOGGER.info(f"Final config is: {final_config}")

        if session:
            cls.SESSION = session
        else:
            session_builder = SparkSession.builder.appName(app_name)
            if config:
                session_builder = session_builder.config(
                    conf=SparkConf().setAll(final_config.items())  # type: ignore
                )
            if enable_hive_support:
                session_builder = session_builder.enableHiveSupport()
            cls.SESSION = session_builder.getOrCreate()

            cls._set_environment_variables(final_config.get("os_env_vars"))

    @classmethod
    def _set_environment_variables(cls, os_env_vars: dict = None) -> None:
        """Set environment variables at OS level.

        By default, we are setting the AWS_DEFAULT_REGION as we have identified this is
        beneficial to avoid getBucketLocation permission problems.

        Args:
            os_env_vars: this parameter can be used to pass the environment variables to
            be defined.
        """
        if os_env_vars is None:
            os_env_vars = {}

        for env_var in os_env_vars.items():
            os.environ[env_var[0]] = env_var[1]

        if "AWS_DEFAULT_REGION" not in os_env_vars:
            os.environ["AWS_DEFAULT_REGION"] = cls.SESSION.sparkContext.getConf().get(
                "spark.databricks.clusterUsageTags.region", cls.DEFAULT_AWS_REGION
            )
