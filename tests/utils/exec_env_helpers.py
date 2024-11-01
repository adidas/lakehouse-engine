"""Module with helper functions to interact with test execution environment."""

from lakehouse_engine.core.exec_env import ExecEnv


class ExecEnvHelpers(object):
    """Class with helper functions to interact with test execution environment."""

    @staticmethod
    def prepare_exec_env(spark_driver_memory: str) -> None:
        """Create single execution environment session."""
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
                "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.0,org.xerial:sqlite-jdbc:3.45.3.0,com.databricks:spark-xml_2.12:0.18.0",  # noqa: E501
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
