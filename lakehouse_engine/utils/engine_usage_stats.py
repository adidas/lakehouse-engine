"""Utilities for recording the engine activity."""

import json
from datetime import datetime
from urllib.parse import urlparse

from lakehouse_engine.core.definitions import CollectEngineUsage
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.configs.config_utils import ConfigUtils
from lakehouse_engine.utils.databricks_utils import DatabricksUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.storage.file_storage_functions import FileStorageFunctions


class EngineUsageStats(object):
    """Engine Usage utilities class."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    @classmethod
    def store_engine_usage(
        cls,
        acon: dict,
        func_name: str,
        collect_engine_usage: str = None,
        spark_confs: dict = None,
    ) -> None:
        """Collects and store Lakehouse Engine usage statistics.

        These statistics include the acon and other relevant information, such as
        the lakehouse engine version and the functions/algorithms being used.

        Args:
            acon: acon dictionary file.
            func_name: function name that called this log acon.
            collect_engine_usage: Lakehouse usage statistics collection strategy.
            spark_confs: optional dictionary with the spark confs to be used when
                collecting the engine usage.
        """
        if not cls._should_collect_usage(collect_engine_usage):
            return
        try:
            start_timestamp = datetime.now()
            timestamp_str = start_timestamp.strftime("%Y%m%d%H%M%S")
            usage_stats = cls._prepare_usage_stats(acon, spark_confs)
            engine_usage_path = cls._select_usage_path(
                usage_stats, collect_engine_usage
            )
            if engine_usage_path is None:
                return

            cls._add_metadata_to_stats(usage_stats, func_name, start_timestamp)
            log_file_name = f"eng_usage_{func_name}_{timestamp_str}.json"
            usage_stats_str = json.dumps(usage_stats, default=str)
            url = urlparse(
                f"{engine_usage_path}/{usage_stats['dp_name']}/"
                f"{start_timestamp.year}/{start_timestamp.month}/"
                f"{log_file_name}",
                allow_fragments=False,
            )
            try:
                FileStorageFunctions.write_payload(
                    engine_usage_path, url, usage_stats_str
                )
                cls._LOGGER.info("Storing Lakehouse Engine usage statistics")
            except FileNotFoundError as e:
                cls._LOGGER.error(f"Could not write engine stats into file: {e}.")
        except Exception as e:
            cls._LOGGER.error(
                "Failed while collecting the lakehouse engine stats: "
                f"Unexpected {e=}, {type(e)=}."
            )

    @classmethod
    def _should_collect_usage(cls, collect_engine_usage: str) -> bool:
        return (
            collect_engine_usage
            in [CollectEngineUsage.ENABLED.value, CollectEngineUsage.PROD_ONLY.value]
            or ExecEnv.ENGINE_CONFIG.collect_engine_usage
            in CollectEngineUsage.ENABLED.value
        )

    @classmethod
    def _prepare_usage_stats(cls, acon: dict, spark_confs: dict) -> dict:
        usage_stats = {"acon": ConfigUtils.remove_sensitive_info(acon)}
        if not ExecEnv.IS_SERVERLESS:
            DatabricksUtils.get_spark_conf_values(usage_stats, spark_confs)
        else:
            DatabricksUtils.get_usage_context_for_serverless(usage_stats)
        return usage_stats

    @classmethod
    def _select_usage_path(
        cls, usage_stats: dict, collect_engine_usage: str
    ) -> str | None:
        if usage_stats.get("environment") == "prod":
            return ExecEnv.ENGINE_CONFIG.engine_usage_path
        elif collect_engine_usage != CollectEngineUsage.PROD_ONLY.value:
            return ExecEnv.ENGINE_CONFIG.engine_dev_usage_path
        return None

    @classmethod
    def _add_metadata_to_stats(
        cls, usage_stats: dict, func_name: str, start_timestamp: datetime
    ) -> None:
        usage_stats["function"] = func_name
        usage_stats["engine_version"] = ConfigUtils.get_engine_version()
        usage_stats["start_timestamp"] = start_timestamp
        usage_stats["year"] = start_timestamp.year
        usage_stats["month"] = start_timestamp.month
