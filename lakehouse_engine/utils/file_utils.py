"""Utilities for file name based operations."""
import re
from os import listdir


def get_file_names_without_file_type(
    path: str, file_type: str, exclude_regex: str
) -> list:
    """Function to retrieve list of file names in a folder.

    This function filters by file type and removes the extension of the file name
    it returns.

    Args:
        path: path to the folder to list files
        file_type: type of the file to include in list
        exclude_regex: regex of file names to exclude

    Returns:
        A list of file names without file type.
    """
    file_list = []

    for file in listdir(path):
        if not re.search(exclude_regex, file) and file.endswith(file_type):
            file_list.append(file.split(".")[0])

    return file_list
