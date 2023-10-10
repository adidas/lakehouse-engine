"""Module to represent a s3 file storage system."""
from typing import Any
from urllib.parse import ParseResult

import boto3

from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.storage.file_storage import FileStorage


class S3Storage(FileStorage):
    """Class to represent a s3 file storage system."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    @classmethod
    def get_file_payload(cls, url: ParseResult) -> Any:
        """Get the payload of a config file.

        Args:
            url: url of the file.

        Returns:
            File payload/content.
        """
        s3 = boto3.resource("s3")
        obj = s3.Object(url.netloc, url.path.lstrip("/"))
        cls._LOGGER.info(f"Reading from file: {url.scheme}://{url.netloc}{url.path}")
        return obj.get()["Body"]

    @classmethod
    def write_payload_to_file(cls, url: ParseResult, content: str) -> None:
        """Write payload into a file.

        Args:
            url: url of the file.
            content: content to write into the file.
        """
        s3 = boto3.resource("s3")
        obj = s3.Object(url.netloc, url.path.lstrip("/"))
        cls._LOGGER.info(f"Writing into file: {url.scheme}://{url.netloc}{url.path}")
        obj.put(Body=content)
