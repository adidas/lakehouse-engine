"""Module with helper functions to interact with test execution environment."""

import os

from lakehouse_engine.core.exec_env import ExecEnv


class ExecEnvHelpers(object):
    """Class with helper functions to interact with test execution environment."""

    @staticmethod
    def prepare_exec_env(spark_driver_memory: str, spark_mode: str = "local") -> None:
        """Create single execution environment session.

        Args:
            spark_driver_memory: Memory limit for spark driver.
            spark_mode: Mode to run spark - 'local' or 'connect'.
        """
        if spark_mode == "connect":
            connect_url = os.getenv("SPARK_REMOTE", "sc://localhost:15002")
            ExecEnv.get_or_create(
                app_name="Lakehouse Engine Tests",
                enable_hive_support=False,
                config={
                    "spark.remote": connect_url,
                    "spark.packages": "io.delta:delta-connect-client_2.13:4.0.1,org.xerial:sqlite-jdbc:3.50.3.0,com.google.protobuf:protobuf-java:3.25.1",  # noqa: E501
                    "spark.jars.repositories": str(ExecEnv.ENGINE_CONFIG.maven_repo),
                    "spark.sql.sources.parallelPartitionDiscovery.parallelism": "2",
                },
            )
        else:
            ExecEnv.get_or_create(
                app_name="Lakehouse Engine Tests",
                enable_hive_support=False,
                config={
                    "spark.master": "local[2]",
                    "spark.driver.memory": spark_driver_memory,
                    "spark.sql.warehouse.dir": "file:///app/tests/lakehouse/spark-warehouse/",  # noqa: E501
                    "spark.sql.shuffle.partitions": "2",
                    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",  # noqa: E501
                    "spark.jars.packages": "io.delta:delta-spark_2.13:4.0.1,org.xerial:sqlite-jdbc:3.50.3.0",  # noqa: E501
                    "spark.jars.repositories": str(ExecEnv.ENGINE_CONFIG.maven_repo),
                    "spark.jars.excludes": "net.sourceforge.f2j:arpack_combined_all",
                    "spark.sql.sources.parallelPartitionDiscovery.parallelism": "2",
                    "spark.sql.legacy.charVarcharAsString": True,
                },
            )

    @classmethod
    def set_exec_env_config(cls, key: str, value: str) -> None:
        """Set any execution environment (e.g., spark) session setting."""
        ExecEnv.SESSION.conf.set(key, value)

    @classmethod
    def reset_default_spark_session_configs(cls) -> None:
        """Reset spark session configs."""
        cls.set_exec_env_config(
            "spark.databricks.delta.schema.autoMerge.enabled", "false"
        )
        cls.set_exec_env_config("spark.sql.streaming.schemaInference", "false")
        cls.set_exec_env_config(
            "spark.sql.sources.partitionColumnTypeInference.enabled", "true"
        )
