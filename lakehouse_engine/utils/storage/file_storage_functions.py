"""Module for common file storage functions."""
import json
from abc import ABC
from typing import Any
from urllib.parse import urlparse

from lakehouse_engine.utils.storage.local_fs_storage import LocalFSStorage
from lakehouse_engine.utils.storage.s3_storage import S3Storage


class FileStorageFunctions(ABC):  # noqa: B024
    """Class for common file storage functions."""

    @staticmethod
    def read_json(path: str) -> Any:
        """Read a json file.

        The file should be in a supported file system (e.g., s3 or local filesystem -
        for local tests only).

        Args:
            path: path to the json file.

        Returns:
            Dict with json file content.
        """
        url = urlparse(path, allow_fragments=False)
        if url.scheme == "s3":
            return json.load(S3Storage.get_file_payload(url))
        elif url.scheme == "file":
            return json.load(LocalFSStorage.get_file_payload(url))
        else:
            raise NotImplementedError(
                f"File storage protocol not implemented for {path}."
            )
