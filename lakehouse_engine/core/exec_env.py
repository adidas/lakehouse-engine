"""Module to take care of creating a singleton of the execution environment class."""

from dataclasses import replace

from pyspark.sql import DataFrame, SparkSession

from lakehouse_engine.core.definitions import EngineConfig
from lakehouse_engine.utils.configs.config_utils import ConfigUtils
from lakehouse_engine.utils.databricks_utils import DatabricksUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler


class ExecEnv(object):
    """Represents the basic resources regarding the engine execution environment.

    Currently, it is used to encapsulate both the logic to get the Spark
    session and the engine configurations.
    """

    SESSION: SparkSession
    _LOGGER = LoggingHandler(__name__).get_logger()
    ENGINE_CONFIG: EngineConfig = EngineConfig(**ConfigUtils.get_config())
    IS_SERVERLESS = DatabricksUtils.is_serverless_workload()

    @classmethod
    def set_default_engine_config(
        cls,
        package: str = "lakehouse_engine.configs",
        custom_configs_dict: dict = None,
        custom_configs_file_path: str = None,
    ) -> None:
        """Set default engine configurations.

        The function set the default engine configurations by reading
        them from a specified package and overwrite them if the user
        pass a dictionary or a file path with new configurations.

        Args:
            package: package where the engine default configurations can be found.
            custom_configs_dict: a dictionary with custom configurations
            to overwrite the default ones.
            custom_configs_file_path: path for the file with custom
            configurations to overwrite the default ones.
        """
        cls.ENGINE_CONFIG = EngineConfig(**ConfigUtils.get_config(package))
        if custom_configs_dict:
            cls.ENGINE_CONFIG = replace(cls.ENGINE_CONFIG, **custom_configs_dict)
        if custom_configs_file_path:
            cls.ENGINE_CONFIG = replace(
                cls.ENGINE_CONFIG,
                **ConfigUtils.get_config_from_file(custom_configs_file_path),
            )

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
        lakehouse engine. By default, if there is an existing Spark Session in
        the environment (getActiveSession()), this function re-uses it. It can
        be further extended in the future to support forcing the creation of new
        isolated sessions even when a Spark Session is already active.

        Args:
            session: spark session.
            enable_hive_support: whether to enable hive support or not.
            app_name: application name.
            config: extra spark configs to supply to the spark session.
        """
        if not cls.IS_SERVERLESS:
            default_config = {
                "spark.databricks.delta.optimizeWrite.enabled": True,
                "spark.sql.adaptive.enabled": True,
                "spark.databricks.delta.merge.enableLowShuffle": True,
            }
            cls._LOGGER.info(
                f"Using the following default configs you may want to override them "
                f"for your job: {default_config}"
            )
        else:
            default_config = {}
        final_config: dict = {**default_config, **(config if config else {})}
        cls._LOGGER.info(f"Final config is: {final_config}")

        if session:
            cls.SESSION = session
        elif SparkSession.getActiveSession():
            cls.SESSION = SparkSession.getActiveSession()
            cls._set_spark_configs(final_config)
        else:
            cls._LOGGER.info("Creating a new Spark Session")

            session_builder = SparkSession.builder.appName(app_name)
            cls._set_spark_configs(final_config, session_builder)

            if enable_hive_support:
                session_builder = session_builder.enableHiveSupport()
            cls.SESSION = session_builder.getOrCreate()

    @classmethod
    def get_for_each_batch_session(cls, df: DataFrame) -> None:
        """Get the execution environment session for foreachBatch operations.

        For Spark connect scenarios, spark is not able to re-use the Spark session
        from an external scope as it cannot serialise it, so the session
        needs to be retrieved and stored again in the ExecEnv class.
        """
        cls.SESSION = df.sparkSession.getActiveSession()

    @classmethod
    def _set_spark_configs(
        cls, final_config: dict, session_builder: SparkSession.Builder = None
    ) -> None:
        """Set Spark session configurations based on final_config.

        This method attempts to set each configuration key-value pair in the provided
        final_config dictionary to the Spark session. If a configuration key is not
        available in the current environment, it logs a warning and skips that key.

        Args:
            final_config: dictionary with spark configurations to set.
            session_builder: spark session builder.
        """
        for key, value in final_config.items():
            try:
                if session_builder:
                    session_builder.config(key, value)
                else:
                    cls.SESSION.conf.set(key, value)
            except Exception as e:
                if (
                    "[CONFIG_NOT_AVAILABLE]" in str(e)
                    and not ExecEnv.ENGINE_CONFIG.raise_on_config_not_available
                ):
                    cls._LOGGER.warning(
                        f"Spark config '{key}' is not available in this "
                        f"environment and will be skipped."
                    )
                else:
                    raise e

    @classmethod
    def get_environment(cls) -> str:
        """Get the environment where the process is running.

        Returns:
            Name of the environment.
        """
        if cls.ENGINE_CONFIG.environment:
            return cls.ENGINE_CONFIG.environment

        catalog = cls.SESSION.sql("SELECT current_catalog()").collect()[0][0]
        if catalog.lower() == cls.ENGINE_CONFIG.prod_catalog:
            return "prod"
        else:
            return "dev"
