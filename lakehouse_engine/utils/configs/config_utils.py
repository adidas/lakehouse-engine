"""Module to read configurations."""
from importlib.resources import open_binary
from typing import Any, Optional
from urllib.parse import urlparse

import yaml

from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.storage.file_storage_functions import (
    FileStorageFunctions,
    LocalFSStorage,
    S3Storage,
)


class ConfigUtils(object):
    """Config utilities class."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def get_acon(
        cls,
        acon_path: Optional[str] = None,
        acon: Optional[dict] = None,
    ) -> dict:
        """Get acon based on a filesystem path or on a dict.

        Args:
            acon_path: path of the acon (algorithm configuration) file.
            acon: acon provided directly through python code (e.g., notebooks
                or other apps).

        Returns:
            Dict representation of an acon.
        """
        acon = acon if acon else ConfigUtils.read_json_acon(acon_path)
        cls._logger.info(f"Read Algorithm Configuration: {str(acon)}")
        return acon

    @staticmethod
    def get_config() -> dict:
        """Get Lakehouse Engine configurations.

        Returns:
             A dictionary with the engine configurations.
        """
        with open_binary("lakehouse_engine.configs", "engine.yaml") as bin_config:
            config = yaml.safe_load(bin_config)
        return dict(config)

    @staticmethod
    def read_json_acon(path: str) -> Any:
        """Read an acon (algorithm configuration) file.

        Args:
            path: path to the acon file.

        Returns:
            The acon file content as a dict.
        """
        return FileStorageFunctions.read_json(path)

    @staticmethod
    def read_sql(path: str) -> Any:
        """Read a DDL file in Spark SQL format from a cloud object storage system.

        Args:
            path: path to the acon (algorithm configuration) file.

        Returns:
            Content of the SQL file.
        """
        url = urlparse(path, allow_fragments=False)
        if url.scheme == "s3":
            return S3Storage.get_file_payload(url).read().decode("utf-8")
        elif url.scheme == "file":
            local = LocalFSStorage.get_file_payload(url).read()
            return local
        else:
            raise NotImplementedError(
                f"Object storage protocol not implemented for {path}."
            )
