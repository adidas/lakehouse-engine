"""Module for abstract representation of a file manager system."""

from abc import ABC, abstractmethod
from typing import Any

from lakehouse_engine.algorithms.exceptions import RestoreTypeNotFoundException
from lakehouse_engine.utils.storage.file_storage_functions import FileStorageFunctions


class FileManager(ABC):  # noqa: B024
    """Abstract file manager class."""

    def __init__(self, configs: dict):
        """Construct FileManager algorithm instances.

        Args:
            configs: configurations for the FileManager algorithm.
        """
        self.configs = configs
        self.function = self.configs["function"]

    @abstractmethod
    def delete_objects(self) -> None:
        """Delete objects and 'directories'.

        If dry_run is set to True the function will print a dict with all the
        paths that would be deleted based on the given keys.
        """
        pass

    @abstractmethod
    def copy_objects(self) -> None:
        """Copies objects and 'directories'.

        If dry_run is set to True the function will print a dict with all the
        paths that would be copied based on the given keys.
        """
        pass

    @abstractmethod
    def move_objects(self) -> None:
        """Moves objects and 'directories'.

        If dry_run is set to True the function will print a dict with all the
        paths that would be moved based on the given keys.
        """
        pass


class FileManagerFactory(ABC):  # noqa: B024
    """Class for file manager factory."""

    @staticmethod
    def execute_function(configs: dict) -> Any:
        """Get a specific File Manager and function to execute."""
        from lakehouse_engine.core.dbfs_file_manager import DBFSFileManager
        from lakehouse_engine.core.s3_file_manager import S3FileManager

        disable_dbfs_retry = (
            configs["disable_dbfs_retry"]
            if "disable_dbfs_retry" in configs.keys()
            else False
        )

        if disable_dbfs_retry:
            S3FileManager(configs).get_function()
        elif FileStorageFunctions.is_boto3_configured():
            try:
                S3FileManager(configs).get_function()
            except (ValueError, NotImplementedError, RestoreTypeNotFoundException):
                raise
            except Exception:
                DBFSFileManager(configs).get_function()
        else:
            DBFSFileManager(configs).get_function()
