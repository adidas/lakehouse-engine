"""Utilities for recording the engine activity."""

import ast
import json
import re
from datetime import datetime
from typing import Dict
from urllib.parse import urlparse

from lakehouse_engine.core.definitions import CollectEngineUsage, EngineStats
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.configs.config_utils import ConfigUtils
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
        try:
            if (
                collect_engine_usage
                in [
                    CollectEngineUsage.ENABLED.value,
                    CollectEngineUsage.PROD_ONLY.value,
                ]
                or ExecEnv.ENGINE_CONFIG.collect_engine_usage
                in CollectEngineUsage.ENABLED.value
            ):
                start_timestamp = datetime.now()
                timestamp_str = start_timestamp.strftime("%Y%m%d%H%M%S")

                usage_stats: Dict = {"acon": ConfigUtils.remove_sensitive_info(acon)}
                EngineUsageStats.get_spark_conf_values(usage_stats, spark_confs)

                engine_usage_path = None
                if usage_stats["environment"] == "prod":
                    engine_usage_path = ExecEnv.ENGINE_CONFIG.engine_usage_path
                elif collect_engine_usage != CollectEngineUsage.PROD_ONLY.value:
                    engine_usage_path = ExecEnv.ENGINE_CONFIG.engine_dev_usage_path

                if engine_usage_path is not None:
                    usage_stats["function"] = func_name
                    usage_stats["engine_version"] = ConfigUtils.get_engine_version()
                    usage_stats["start_timestamp"] = start_timestamp
                    usage_stats["year"] = start_timestamp.year
                    usage_stats["month"] = start_timestamp.month
                    run_id_extracted = re.search(
                        "run-([1-9]\\w+)", usage_stats["run_id"]
                    )
                    usage_stats["run_id"] = (
                        run_id_extracted.group(1) if run_id_extracted else ""
                    )

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
                        cls._LOGGER.error(
                            f"Could not write engine stats into file: {e}."
                        )
        except Exception as e:
            cls._LOGGER.error(
                "Failed while collecting the lakehouse engine stats: "
                f"Unexpected {e=}, {type(e)=}."
            )

    @classmethod
    def get_spark_conf_values(cls, usage_stats: dict, spark_confs: dict) -> None:
        """Get information from spark session configurations.

        Args:
            usage_stats: usage_stats dictionary file.
            spark_confs: optional dictionary with the spark tags to be used when
                collecting the engine usage.
        """
        spark_confs = (
            EngineStats.DEF_SPARK_CONFS.value
            if spark_confs is None
            else EngineStats.DEF_SPARK_CONFS.value | spark_confs
        )

        for spark_conf_key, spark_conf_value in spark_confs.items():
            # whenever the spark_conf_value has #, it means it is an array, so we need
            # to split it and adequately process it
            if "#" in spark_conf_value:
                array_key = spark_conf_value.split("#")
                array_values = ast.literal_eval(
                    ExecEnv.SESSION.conf.get(array_key[0], "[]")
                )
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
