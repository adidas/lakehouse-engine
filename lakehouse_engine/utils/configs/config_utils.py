"""Module to read configurations."""

import importlib.resources
from typing import Any, Optional, Union

import pkg_resources
import yaml

from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.storage.file_storage_functions import FileStorageFunctions


class ConfigUtils(object):
    """Config utilities class."""

    _LOGGER = LoggingHandler(__name__).get_logger()
    SENSITIVE_INFO = [
        "kafka.ssl.keystore.password",
        "kafka.ssl.truststore.password",
        "password",
        "secret",
        "credential",
        "credentials",
        "pass",
        "key",
    ]

    @classmethod
    def get_acon(
        cls,
        acon_path: Optional[str] = None,
        acon: Optional[dict] = None,
        disable_dbfs_retry: bool = False,
    ) -> dict:
        """Get acon based on a filesystem path or on a dict.

        Args:
            acon_path: path of the acon (algorithm configuration) file.
            acon: acon provided directly through python code (e.g., notebooks
                or other apps).
            disable_dbfs_retry: optional flag to disable file storage dbfs.

        Returns:
            Dict representation of an acon.
        """
        acon = (
            acon if acon else ConfigUtils.read_json_acon(acon_path, disable_dbfs_retry)
        )
        return acon

    @staticmethod
    def get_config(package: str = "lakehouse_engine.configs") -> Any:
        """Get the lakehouse engine configuration file.

        Returns:
            Configuration dictionary
        """
        with importlib.resources.open_binary(package, "engine.yaml") as config:
            config = yaml.safe_load(config)
        return config

    @staticmethod
    def get_config_from_file(config_file_path: str) -> Any:
        """Get the lakehouse engine configurations using a file path.

         Args:
            config_file_path: a string with a path for a yaml file
            with custom configurations.

        Returns:
            Configuration dictionary
        """
        with open(config_file_path, "r") as config:
            config = yaml.safe_load(config)
        return config

    @classmethod
    def get_engine_version(cls) -> str:
        """Get Lakehouse Engine version from the installed packages.

        Returns:
            String of engine version.
        """
        try:
            version = pkg_resources.get_distribution("lakehouse-engine").version
        except pkg_resources.DistributionNotFound:
            cls._LOGGER.info("Could not identify Lakehouse Engine version.")
            version = ""
        return str(version)

    @staticmethod
    def read_json_acon(path: str, disable_dbfs_retry: bool = False) -> Any:
        """Read an acon (algorithm configuration) file.

        Args:
            path: path to the acon file.
            disable_dbfs_retry: optional flag to disable file storage dbfs.

        Returns:
            The acon file content as a dict.
        """
        return FileStorageFunctions.read_json(path, disable_dbfs_retry)

    @staticmethod
    def read_sql(path: str, disable_dbfs_retry: bool = False) -> Any:
        """Read a DDL file in Spark SQL format from a cloud object storage system.

        Args:
            path: path to the SQL file.
            disable_dbfs_retry: optional flag to disable file storage dbfs.

        Returns:
            Content of the SQL file.
        """
        return FileStorageFunctions.read_sql(path, disable_dbfs_retry)

    @classmethod
    def remove_sensitive_info(
        cls, dict_to_replace: Union[dict, list]
    ) -> Union[dict, list]:
        """Remove sensitive info from a dictionary.

        Args:
            dict_to_replace: dict where we want to remove sensitive info.

        Returns:
            dict without sensitive information.
        """
        if isinstance(dict_to_replace, list):
            return [cls.remove_sensitive_info(k) for k in dict_to_replace]
        elif isinstance(dict_to_replace, dict):
            return {
                k: "******" if k in cls.SENSITIVE_INFO else cls.remove_sensitive_info(v)
                for k, v in dict_to_replace.items()
            }
        else:
            return dict_to_replace
