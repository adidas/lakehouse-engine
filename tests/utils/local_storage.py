"""Utilities to interact with the local file system used in the tests."""
import glob
from os import makedirs, path, remove
from pathlib import Path
from shutil import copy, rmtree


class LocalStorage(object):
    """Helper class to support local storage operations in tests."""

    @staticmethod
    def copy_file(from_path: str, to_path: str) -> None:
        """Copy files (supports regex) into target file or folder.

        :param str from_path: path from where to copy files from (supports regex).
        :param str to_path: path to where to copy files to.
        """
        makedirs(path.dirname(to_path), exist_ok=True)

        for file in glob.glob(from_path):
            copy(file, to_path)

    @staticmethod
    def clean_folder(folder_path: str) -> None:
        """Clean a folder content.

        :param str folder_path: path of the folder to clean.
        """
        if Path(folder_path).is_dir():
            rmtree(folder_path)

    @staticmethod
    def delete_file(file_path: str) -> None:
        """Delete a file.

        :param str file_path: path of the file(s) to delete (supports regex).
        """
        for file in glob.glob(file_path):
            if Path(file).exists():
                remove(file)

    @staticmethod
    def read_file(file_path: str) -> str:
        """Read file from directory.

        Args:
            file_path: path of the file to be read.
        """
        with open(file_path, "r") as f:
            result = f.read()
        return result
