"""Module to take care of creating a singleton of the execution environment class."""

import os

from pyspark.sql import SparkSession

from lakehouse_engine.core.definitions import EngineConfig
from lakehouse_engine.utils.configs.config_utils import ConfigUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler


class ExecEnv(object):
    """Represents the basic resources regarding the engine execution environment.

    Currently, it is used to encapsulate both the logic to get the Spark
    session and the engine configurations.
    """

    SESSION: SparkSession
    _LOGGER = LoggingHandler(__name__).get_logger()
    DEFAULT_AWS_REGION = "eu-west-1"
    ENGINE_CONFIG: EngineConfig = EngineConfig(**ConfigUtils.get_config())

    @classmethod
    def set_default_engine_config(cls, package: str) -> None:
        """Set default engine configurations by reading them from a specified package.

        Args:
            package: package where the engine configurations can be found.
        """
        cls.ENGINE_CONFIG = EngineConfig(**ConfigUtils.get_config(package))

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
            # with active session we do not need app name
            if SparkSession.getActiveSession():
                app_name = SparkSession.getActiveSession().conf.get("spark.app.name")
                cls._LOGGER.info(f"Detected active session: {app_name}")
            elif not SparkSession.getActiveSession() and not app_name:
                cls._LOGGER.info("No active session or appname detected")
                app_name = "lakehouse_engine"
            # we will still add this part to set configs
            session_builder = SparkSession.builder.appName(app_name)
            if config:
                for k, v in final_config.items():
                    session_builder.config(k, v)

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
            os.environ["AWS_DEFAULT_REGION"] = cls.SESSION.conf.get(
                "spark.databricks.clusterUsageTags.region", cls.DEFAULT_AWS_REGION
            )
