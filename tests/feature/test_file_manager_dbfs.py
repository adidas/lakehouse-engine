"""Test file manager for dbfs."""
import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

import pytest

from lakehouse_engine.engine import manage_files
from lakehouse_engine.utils.databricks_utils import DatabricksUtils
from tests.conftest import FEATURE_RESOURCES

TEST_PATH = "file_manager_dbfs"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_DBFS = "tests/lakehouse/dbfs"


@dataclass
class FileInfoFixture:
    """This class mocks the DBUtils FileInfo object."""

    path: str
    name: str
    size: int

    def isDir(self) -> bool:
        """Construct to check if the path is a directory.

        Returns:
            A bool as true is it is a directory.
        """
        return os.path.isdir(self.path)

    def isFile(self) -> bool:
        """Construct to check if the path is a file.

        Returns:
            A bool as true is it is a file.
        """
        return os.path.isfile(self.path)


class DBUtilsFixture:
    """This class is used for mocking the behaviour of DBUtils inside tests."""

    def __init__(self) -> None:
        """Construct to mock DBUtils filesystem operations."""
        self.fs = self

    @staticmethod
    def cp(src: str, dest: str, recurse: bool = False) -> None:
        """This mocks the behavior of dbutils when copy files or directories.

        Args:
            src: string with the path to copy from.
            dest: string with the path to copy to.
            recurse: bool to recursively move files or directories.
        """
        if os.path.isfile(src):
            shutil.copy(src, dest)
        elif recurse:
            shutil.copytree(src, dest)
        else:
            shutil.copy(src, dest)

    @staticmethod
    def ls(path: str) -> list:
        """This mocks the behavior of dbutils when reading a directory or files inside.

        Args:
            path: string with the path to read the directory or files inside.
        """
        paths = Path(path).glob("*")
        objects = [
            FileInfoFixture(str(p.absolute()), p.name, p.stat().st_size) for p in paths
        ]
        return objects

    @staticmethod
    def mkdirs(path: str) -> None:
        """This mocks the behavior of dbutils when creating a directory.

        Args:
            path: string with the path to create the directory.
        """
        Path(path).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def mv(src: str, dest: str, recurse: bool = False) -> None:
        """This mocks the behavior of dbutils when moving files or directories.

        Args:
            src: string with the path to move from.
            dest: string with the path to move to.
            recurse: bool to recursively move files or directories.
        """
        if os.path.isfile(src):
            shutil.move(src, dest, copy_function=shutil.copy)
        elif recurse:
            shutil.move(src, dest, copy_function=shutil.copytree)
        else:
            shutil.move(src, dest, copy_function=shutil.copy)

    @staticmethod
    def put(path: str, content: str, overwrite: bool = False) -> None:
        """This mocks the behavior of dbutils when inserting in files.

        Args:
            path: string with the path to insert content.
            content: string with the content to insert in the file.
            overwrite: bool to overwrite file with the content.
        """
        file = Path(path)

        if file.exists() and not overwrite:
            raise FileExistsError("File already exists")

        file.write_text(content, encoding="utf-8")

    @staticmethod
    def rm(path: str, recurse: bool = False) -> None:
        """This mocks the behavior of dbutils when removing files or directories.

        Args:
            path: string with the path to remove.
            recurse: bool to recursively remove files or directories.
        """
        if os.path.isfile(path):
            os.remove(path)
        elif recurse:
            shutil.rmtree(path)
        else:
            os.remove(path)


@pytest.fixture(scope="session", autouse=True)
def dbutils_fixture() -> Iterator[None]:
    """This fixture patches the `get_db_utils` function."""
    with patch.object(DatabricksUtils, "get_db_utils", lambda _: DBUtilsFixture()):
        yield


@patch(
    "lakehouse_engine.utils.storage.file_storage_functions."
    "FileStorageFunctions.is_boto3_configured",
    return_value=False,
)
def test_file_manager_dbfs(_patch: Any, caplog: Any) -> None:
    """Test functions from file manager.

    Args:
        caplog: captured log.
    """
    dbutils = DBUtilsFixture()

    with caplog.at_level(logging.INFO):
        # Creating test files/folders in dbfs
        dbutils.fs.mkdirs(path=TEST_LAKEHOUSE_DBFS)
        dbutils.fs.put(path=f"{TEST_LAKEHOUSE_DBFS}/test_single_file.json", content="")
        dbutils.fs.mkdirs(path=f"{TEST_LAKEHOUSE_DBFS}/test_directory/")
        for x in range(0, 2000):
            dbutils.fs.put(
                path=f"{TEST_LAKEHOUSE_DBFS}/test_directory/"
                f"test_recursive_file{x}.json",
                content="",
            )
        dbutils.fs.mkdirs(path=f"{TEST_LAKEHOUSE_DBFS}/test_directory_test/")
        for x in range(0, 2000):
            dbutils.fs.put(
                path=f"{TEST_LAKEHOUSE_DBFS}/test_directory_test/"
                f"test_recursive_file{x}.json",
                content="",
            )

        _test_file_manager_dbfs_copy(caplog, dbutils)
        _test_file_manager_dbfs_delete(caplog, dbutils)
        _test_file_manager_dbfs_move(caplog, dbutils)


def _list_objects(path: str, objects_list: list, dbutils: Any) -> list:
    list_objects = dbutils.fs.ls(path)

    for file_or_directory in list_objects:
        if file_or_directory.isDir():
            _list_objects(file_or_directory.path, objects_list, dbutils)
        else:
            objects_list.append(file_or_directory.path)
    return objects_list


def _test_file_manager_dbfs_copy(caplog: Any, dbutils: Any) -> None:
    """Testing file manager copy operations.

    Args:
        caplog: captured log.
        dbutils: Dbutils from databricks.
    """
    manage_files(
        acon_path=f"file://{TEST_RESOURCES}/copy_objects/"
        f"acon_copy_directory_dry_run.json"
    )
    for x in range(0, 2000):
        assert (
            f"/app/tests/lakehouse/dbfs/test_directory/test_recursive_file{x}.json"
            in caplog.text
        )

    manage_files(
        acon_path=f"file://{TEST_RESOURCES}/copy_objects/acon_copy_directory.json"
    )

    assert len(dbutils.fs.ls("tests/lakehouse/dbfs/test_directory")) == len(
        dbutils.fs.ls("tests/lakehouse/dbfs/destination_directory")
    )

    manage_files(
        acon_path=f"file://{TEST_RESOURCES}/copy_objects/acon_copy_single_object.json"
    )

    assert "tests/lakehouse/dbfs/test_single_file.json" in str(
        dbutils.fs.ls("tests/lakehouse/dbfs/")
    )


def _test_file_manager_dbfs_delete(caplog: Any, dbutils: Any) -> None:
    """Testing file manager delete operations.

    Args:
        caplog: captured log.
        dbutils: Dbutils from databricks.
    """
    manage_files(
        acon_path=f"file://{TEST_RESOURCES}/delete_objects/"
        f"acon_delete_objects_dry_run.json"
    )
    assert (
        "{'tests/lakehouse/dbfs/test_directory': "
        "['/app/tests/lakehouse/dbfs/test_directory/" in caplog.text
    )
    for x in range(0, 2000):
        assert (
            f"/app/tests/lakehouse/dbfs/test_directory/"
            f"test_recursive_file{x}.json" in caplog.text
        )
    for x in range(0, 2000):
        assert (
            f"/app/tests/lakehouse/dbfs/destination_directory/"
            f"test_recursive_file{x}.json" in caplog.text
        )

    manage_files(
        acon_path=f"file://{TEST_RESOURCES}/delete_objects/acon_delete_objects.json"
    )
    assert len(dbutils.fs.ls("tests/lakehouse/dbfs/destination_directory")) == 0


def _test_file_manager_dbfs_move(caplog: Any, dbutils: Any) -> None:
    """Testing file manager move operations.

    Args:
        caplog: captured log.
        dbutils: Dbutils from databricks.
    """
    manage_files(
        acon_path=f"file://{TEST_RESOURCES}/move_objects/acon_move_objects_dry_run.json"
    )
    assert (
        "{'tests/lakehouse/dbfs/test_directory': "
        "['/app/tests/lakehouse/dbfs/test_directory/" in caplog.text
    )
    for x in range(0, 2000):
        assert (
            f"/app/tests/lakehouse/dbfs/test_directory/"
            f"test_recursive_file{x}.json" in caplog.text
        )
    for x in range(0, 2000):
        assert (
            f"/app/tests/lakehouse/dbfs/destination_directory/"
            f"test_recursive_file{x}.json" in caplog.text
        )

    manage_files(
        acon_path=f"file://{TEST_RESOURCES}/move_objects/acon_move_objects.json"
    )
    assert len(dbutils.fs.ls("tests/lakehouse/dbfs/test_directory")) == 0
    assert len(dbutils.fs.ls("tests/lakehouse/dbfs/test_mv_directory")) == 2000
