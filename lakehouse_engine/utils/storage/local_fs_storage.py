"""Module to represent a local file storage system."""
import os
from typing import TextIO
from urllib.parse import ParseResult

from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.storage.file_storage import FileStorage


class LocalFSStorage(FileStorage):
    """Class to represent a local file storage system."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    @classmethod
    def get_file_payload(cls, url: ParseResult) -> TextIO:
        """Get the payload of a file.

        Args:
            url: url of the file.

        Returns:
            file payload/content.
        """
        cls._LOGGER.info(f"Reading from file: {url.scheme}:{url.netloc}/{url.path}")
        return open(f"{url.netloc}/{url.path}", "r")

    @classmethod
    def write_payload_to_file(cls, url: ParseResult, content: str) -> None:
        """Write payload into a file.

        Args:
            url: url of the file.
            content: content to write into the file.
        """
        cls._LOGGER.info(f"Writing into file: {url.scheme}:{url.netloc}/{url.path}")
        os.makedirs(os.path.dirname(f"{url.netloc}/{url.path}"), exist_ok=True)
        with open(f"{url.netloc}/{url.path}", "w") as file:
            file.write(content)
