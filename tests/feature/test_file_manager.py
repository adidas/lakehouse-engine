"""Test file manager."""
import logging
from typing import Any

import boto3
import pytest
from moto import mock_s3  # type: ignore

from lakehouse_engine.engine import manage_files
from tests.conftest import FEATURE_RESOURCES

TEST_PATH = "file_manager"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"


@mock_s3
def test_file_manager(caplog: Any) -> None:
    """Test functions from file manager.

    Args:
        caplog: captured log.
    """
    s3_res = boto3.resource("s3", region_name="us-east-1")
    s3_cli = boto3.client("s3", region_name="us-east-1")

    s3_res.create_bucket(Bucket="test_bucket")
    s3_res.create_bucket(Bucket="destination_bucket")

    with caplog.at_level(logging.INFO):
        # Creating test files/folders in S3
        # 2000 files are created to test the pagination is being correctly performed
        s3_cli.put_object(Bucket="test_bucket", Key="test_single_file.json", Body="")
        s3_cli.put_object(Bucket="test_bucket", Key="test_directory", Body="")
        for x in range(0, 2000):
            s3_cli.put_object(
                Bucket="test_bucket",
                Key=f"test_directory/test_recursive_file{x}.json",
                Body="",
            )

        _test_file_manager_copy(caplog, s3_cli)
        _test_file_manager_delete(caplog, s3_cli)


def _test_file_manager_copy(caplog: Any, s3_cli: Any) -> None:
    """Testing file manager copy operations.

    Args:
        caplog: captured log.
        s3_cli: s3 client interface.
    """
    manage_files(
        f"file://{TEST_RESOURCES}/copy_object/acon_copy_single_object_dry_run.json"
    )
    assert "{'test_single_file.json': ['test_single_file.json']}" in caplog.text

    manage_files(
        f"file://{TEST_RESOURCES}/copy_object/acon_copy_directory_dry_run.json"
    )
    for x in range(0, 2000):
        assert f"test_directory/test_recursive_file{x}.json" in caplog.text

    manage_files(f"file://{TEST_RESOURCES}/copy_object/acon_copy_single_object.json")
    assert "'KeyCount': 1" in str(s3_cli.list_objects_v2(Bucket="destination_bucket"))

    manage_files(f"file://{TEST_RESOURCES}/copy_object/acon_copy_directory.json")

    assert "'KeyCount': 2002" in str(
        s3_cli.list_objects_v2(Bucket="destination_bucket", MaxKeys=100000)
    )


def _test_file_manager_delete(caplog: Any, s3_cli: Any) -> None:
    """Testing file manager delete operations.

    Args:
        caplog: captured log.
        s3_cli: s3 client interface.
    """
    manage_files(
        f"file://{TEST_RESOURCES}/delete_objects/acon_delete_objects_dry_run.json"
    )
    assert (
        "{'test_single_file.json': ['test_single_file.json'], "
        "'test_directory': ['test_directory'," in caplog.text
    )
    for x in range(0, 2000):
        assert f"test_directory/test_recursive_file{x}.json" in caplog.text

    manage_files(f"file://{TEST_RESOURCES}/delete_objects/acon_delete_objects.json")
    assert "'KeyCount': 0" in str(s3_cli.list_objects_v2(Bucket="test_bucket"))


@mock_s3
@pytest.mark.parametrize(
    "scenario",
    [
        {"scenario_name": "glacier", "storage_class": "GLACIER"},
        {"scenario_name": "glacier_ir", "storage_class": "GLACIER_IR"},
        {"scenario_name": "deep_archive", "storage_class": "DEEP_ARCHIVE"},
    ],
)
def test_file_manager_restore_archive(scenario: dict, caplog: Any) -> None:
    """Test restore functions from file manager.

    Args:
        scenario: scenario to test.
        caplog: captured log.
    """
    s3_res = boto3.resource("s3", region_name="us-east-1")
    s3_cli = boto3.client("s3", region_name="us-east-1")

    s3_res.create_bucket(Bucket="test_bucket")
    s3_res.create_bucket(Bucket="destination_bucket")

    with caplog.at_level(logging.INFO):
        s3_cli.put_object(
            Bucket="test_bucket",
            Key="test_single_file.json",
            Body="",
            StorageClass=scenario.get("storage_class"),
        )
        s3_cli.put_object(Bucket="test_bucket", Key="test_directory", Body="")
        for x in range(0, 3):
            s3_cli.put_object(
                Bucket="test_bucket",
                Key=f"test_directory/test_recursive_file{x}.json",
                Body="",
                StorageClass=scenario.get("storage_class"),
            )

        _test_file_manager_restore_request(caplog, s3_cli, s3_res)
        _test_file_manager_restore_check(caplog, s3_cli, s3_res)


def _test_file_manager_restore_check(caplog: Any, s3_cli: Any, s3_res: Any) -> None:
    """Testing file manager restore check.

    Args:
        caplog: captured log.
        s3_cli: s3 client interface.
        s3_res: s3 resource interface.
    """
    test_bucket = s3_res.Bucket("test_bucket")
    expected_restored_objects = 4
    restored_objects = 0

    manage_files(
        f"file://{TEST_RESOURCES}/check_restore_status/"
        "acon_check_restore_status_directory.json"
    )
    for x in range(0, 3):
        assert (
            f"Checking restore status for: test_directory/test_recursive_file{x}.json"
            in caplog.text
        )

    for bucket_object in test_bucket.objects.all():
        obj = s3_res.Object(bucket_object.bucket_name, bucket_object.key)
        if obj.restore is not None and 'ongoing-request="false"' in obj.restore:
            restored_objects += 1

    assert "'KeyCount': 5" in str(
        s3_cli.list_objects_v2(Bucket="test_bucket", MaxKeys=100000)
    )
    assert expected_restored_objects == restored_objects


def _test_file_manager_restore_request(caplog: Any, s3_cli: Any, s3_res: Any) -> None:
    """Testing file manager restore request.

    Args:
        caplog: captured log.
        s3_cli: s3 client interface.
        s3_res: s3 resource interface.
    """
    test_bucket = s3_res.Bucket("test_bucket")
    expected_restored_objects = 4
    restored_objects = 0

    manage_files(
        f"file://{TEST_RESOURCES}/request_restore/"
        "acon_request_restore_single_object.json"
    )
    manage_files(
        f"file://{TEST_RESOURCES}/request_restore/"
        "acon_request_restore_directory.json"
    )

    for bucket_object in test_bucket.objects.all():
        obj = s3_res.Object(bucket_object.bucket_name, bucket_object.key)
        if obj.restore is not None and 'ongoing-request="false"' in obj.restore:
            restored_objects += 1

    assert "'KeyCount': 5" in str(
        s3_cli.list_objects_v2(Bucket="test_bucket", MaxKeys=100000)
    )
    assert expected_restored_objects == restored_objects


@mock_s3
@pytest.mark.parametrize(
    "scenario",
    [
        {"scenario_name": "glacier", "storage_class": "GLACIER"},
        {"scenario_name": "glacier_ir", "storage_class": "GLACIER_IR"},
        {"scenario_name": "deep_archive", "storage_class": "DEEP_ARCHIVE"},
    ],
)
def test_file_manager_restore_sync(scenario: dict, caplog: Any) -> None:
    """Test restore functions from file manager.

    Args:
        scenario: scenario to test.
        caplog: captured log.
    """
    s3_res = boto3.resource("s3", region_name="us-east-1")
    s3_cli = boto3.client("s3", region_name="us-east-1")

    s3_res.create_bucket(Bucket="test_bucket")
    s3_res.create_bucket(Bucket="destination_bucket")

    with caplog.at_level(logging.INFO):
        s3_cli.put_object(
            Bucket="test_bucket",
            Key="test_single_file.json",
            Body="",
            StorageClass=scenario.get("storage_class"),
        )
        s3_cli.put_object(Bucket="test_bucket", Key="test_directory", Body="")
        for x in range(0, 3):
            s3_cli.put_object(
                Bucket="test_bucket",
                Key=f"test_directory/test_recursive_file{x}.json",
                Body="",
                StorageClass=scenario.get("storage_class"),
            )

        _test_file_manager_restore_sync(caplog, s3_cli, s3_res)
        _test_file_manager_restore_sync_retrieval_tier_exception(caplog)


def _test_file_manager_restore_sync(caplog: Any, s3_cli: Any, s3_res: Any) -> None:
    """Testing file manager restore file sync.

    Args:
        caplog: captured log.
        s3_cli: s3 client interface.
        s3_res: s3 resource interface.
    """
    test_bucket = s3_res.Bucket("test_bucket")
    expected_single_restored_objects = 1
    restored_objects = 0

    manage_files(
        f"file://{TEST_RESOURCES}/request_restore_to_destination_and_wait/"
        "acon_request_restore_to_destination_and_wait_single_object.json"
    )

    for bucket_object in test_bucket.objects.all():
        obj = s3_res.Object(bucket_object.bucket_name, bucket_object.key)
        if obj.restore is not None and 'ongoing-request="false"' in obj.restore:
            restored_objects += 1

    assert "'KeyCount': 1" in str(
        s3_cli.list_objects_v2(Bucket="destination_bucket", MaxKeys=100000)
    )
    assert expected_single_restored_objects == restored_objects

    restored_objects = 0
    expected_restored_objects = 4

    manage_files(
        f"file://{TEST_RESOURCES}/request_restore_to_destination_and_wait/"
        "acon_request_restore_to_destination_and_wait_directory.json"
    )

    for bucket_object in test_bucket.objects.all():
        obj = s3_res.Object(bucket_object.bucket_name, bucket_object.key)
        if obj.restore is not None and 'ongoing-request="false"' in obj.restore:
            restored_objects += 1

    assert "'KeyCount': 5" in str(
        s3_cli.list_objects_v2(Bucket="destination_bucket", MaxKeys=100000)
    )
    assert expected_restored_objects == restored_objects


def _test_file_manager_restore_sync_retrieval_tier_exception(caplog: Any) -> None:
    """Testing file manager restore sync operation when raising exception.

    Args:
        caplog: captured log.
    """
    with pytest.raises(ValueError) as exception:
        manage_files(
            f"file://{TEST_RESOURCES}/request_restore_to_destination_and_wait/"
            "acon_request_restore_to_destination_and_wait_single"
            "_object_raise_error.json"
        )

    assert (
        "Retrieval Tier Bulk not allowed on this operation! "
        "This kind of restore should be used just with `Expedited` retrieval tier "
        "to save cluster costs." in str(exception.value)
    )
