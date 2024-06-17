"""Module for common file storage functions."""

import json
from abc import ABC
from typing import Any
from urllib.parse import ParseResult, urlparse

import boto3

from lakehouse_engine.utils.storage.dbfs_storage import DBFSStorage
from lakehouse_engine.utils.storage.local_fs_storage import LocalFSStorage
from lakehouse_engine.utils.storage.s3_storage import S3Storage


class FileStorageFunctions(ABC):  # noqa: B024
    """Class for common file storage functions."""

    @classmethod
    def read_json(cls, path: str, disable_dbfs_retry: bool = False) -> Any:
        """Read a json file.

        The file should be in a supported file system (e.g., s3, dbfs or
        local filesystem).

        Args:
            path: path to the json file.
            disable_dbfs_retry: optional flag to disable file storage dbfs.

        Returns:
            Dict with json file content.
        """
        url = urlparse(path, allow_fragments=False)
        if disable_dbfs_retry:
            return json.load(S3Storage.get_file_payload(url))
        elif url.scheme == "s3" and cls.is_boto3_configured():
            try:
                return json.load(S3Storage.get_file_payload(url))
            except Exception:
                return json.loads(DBFSStorage.get_file_payload(url))
        elif url.scheme == "file":
            return json.load(LocalFSStorage.get_file_payload(url))
        elif url.scheme in ["dbfs", "s3"]:
            return json.loads(DBFSStorage.get_file_payload(url))
        else:
            raise NotImplementedError(
                f"File storage protocol not implemented for {path}."
            )

    @classmethod
    def read_sql(cls, path: str, disable_dbfs_retry: bool = False) -> Any:
        """Read a sql file.

        The file should be in a supported file system (e.g., s3, dbfs or local
        filesystem).

        Args:
            path: path to the sql file.
            disable_dbfs_retry: optional flag to disable file storage dbfs.

        Returns:
            Content of the SQL file.
        """
        url = urlparse(path, allow_fragments=False)
        if disable_dbfs_retry:
            return S3Storage.get_file_payload(url).read().decode("utf-8")
        elif url.scheme == "s3" and cls.is_boto3_configured():
            try:
                return S3Storage.get_file_payload(url).read().decode("utf-8")
            except Exception:
                return DBFSStorage.get_file_payload(url)
        elif url.scheme == "file":
            return LocalFSStorage.get_file_payload(url).read()
        elif url.scheme in ["dbfs", "s3"]:
            return DBFSStorage.get_file_payload(url)
        else:
            raise NotImplementedError(
                f"Object storage protocol not implemented for {path}."
            )

    @classmethod
    def write_payload(
        cls, path: str, url: ParseResult, content: str, disable_dbfs_retry: bool = False
    ) -> None:
        """Write payload into a file.

        The file should be in a supported file system (e.g., s3, dbfs or local
        filesystem).

        Args:
            path: path to validate the file type.
            url: url of the file.
            content: content to write into the file.
            disable_dbfs_retry: optional flag to disable file storage dbfs.
        """
        if disable_dbfs_retry:
            S3Storage.write_payload_to_file(url, content)
        elif path.startswith("s3://") and cls.is_boto3_configured():
            try:
                S3Storage.write_payload_to_file(url, content)
            except Exception:
                DBFSStorage.write_payload_to_file(url, content)
        elif path.startswith(("s3://", "dbfs:/")):
            DBFSStorage.write_payload_to_file(url, content)
        else:
            LocalFSStorage.write_payload_to_file(url, content)

    @staticmethod
    def is_boto3_configured() -> bool:
        """Check if boto3 is able to locate credentials and properly configured.

        If boto3 is not properly configured, we might want to try a different reader.
        """
        try:
            boto3.client("sts").get_caller_identity()
            return True
        except Exception:
            return False
