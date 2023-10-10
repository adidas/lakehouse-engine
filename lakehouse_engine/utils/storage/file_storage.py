"""Module for abstract representation of a storage system holding files."""
from abc import ABC, abstractmethod
from typing import Any
from urllib.parse import ParseResult


class FileStorage(ABC):
    """Abstract file storage class."""

    @classmethod
    @abstractmethod
    def get_file_payload(cls, url: ParseResult) -> Any:
        """Get the payload of a file.

        Args:
            url: url of the file.

        Returns:
            File payload/content.
        """
        pass

    @classmethod
    @abstractmethod
    def write_payload_to_file(cls, url: ParseResult, content: str) -> None:
        """Write payload into a file.

        Args:
            url: url of the file.
            content: content to write into the file.
        """
        pass
