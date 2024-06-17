"""File manager module using boto3."""

import time
from typing import Any, Optional, Tuple

import boto3

from lakehouse_engine.algorithms.exceptions import RestoreTypeNotFoundException
from lakehouse_engine.core.definitions import (
    ARCHIVE_STORAGE_CLASS,
    FileManagerAPIKeys,
    RestoreStatus,
    RestoreType,
)
from lakehouse_engine.core.file_manager import FileManager
from lakehouse_engine.utils.file_utils import get_directory_path
from lakehouse_engine.utils.logging_handler import LoggingHandler


def _dry_run(bucket: str, object_paths: list) -> dict:
    """Build the dry run request return format.

    Args:
        bucket: name of bucket to perform operation.
        object_paths: paths of object to list.

    Returns:
        A dict with a list of objects that would be copied/deleted.
    """
    response = {}

    for path in object_paths:
        if _check_directory(bucket, path):
            path = get_directory_path(path)

        res = _list_objects_recursively(bucket=bucket, path=path)

        if res:
            response[path] = res
        else:
            response[path] = ["No such key"]

    return response


def _list_objects(
    s3_client: Any, bucket: str, path: str, paginator: str = ""
) -> Tuple[list, str]:
    """List 1000 objects in a bucket given a prefix and paginator in s3.

    Args:
        bucket: name of bucket to perform the list.
        path: path to be used as a prefix.
        paginator: paginator token to be used.

    Returns:
         A list of object names.
    """
    object_list = []

    if not paginator:
        list_response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path)
    else:
        list_response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=path,
            ContinuationToken=paginator,
        )

    if FileManagerAPIKeys.CONTENTS.value in list_response:
        for obj in list_response[FileManagerAPIKeys.CONTENTS.value]:
            object_list.append(obj[FileManagerAPIKeys.KEY.value])

    if FileManagerAPIKeys.CONTINUATION.value in list_response:
        pagination = list_response[FileManagerAPIKeys.CONTINUATION.value]
    else:
        pagination = ""

    return object_list, pagination


def _list_objects_recursively(bucket: str, path: str) -> list:
    """Recursively list all objects given a prefix in s3.

    Args:
        bucket: name of bucket to perform the list.
        path: path to be used as a prefix.

    Returns:
        A list of object names fetched recursively.
    """
    object_list = []
    more_objects = True
    paginator = ""

    s3 = boto3.client("s3")

    while more_objects:
        temp_list, paginator = _list_objects(s3, bucket, path, paginator)

        object_list.extend(temp_list)

        if not paginator:
            more_objects = False

    return object_list


def _check_directory(bucket: str, path: str) -> bool:
    """Checks if the object is a 'directory' in s3.

    Args:
        bucket: name of bucket to perform the check.
        path: path to be used as a prefix.

    Returns:
        If path represents a 'directory'.
    """
    s3 = boto3.client("s3")
    objects, _ = _list_objects(s3, bucket, path)
    return len(objects) > 1


class S3FileManager(FileManager):
    """Set of actions to manipulate s3 files in several ways."""

    _logger = LoggingHandler(__name__).get_logger()

    def get_function(self) -> None:
        """Get a specific function to execute."""
        available_functions = {
            "delete_objects": self.delete_objects,
            "copy_objects": self.copy_objects,
            "request_restore": self.request_restore,
            "check_restore_status": self.check_restore_status,
            "request_restore_to_destination_and_wait": (
                self.request_restore_to_destination_and_wait
            ),
        }

        self._logger.info("Function being executed: {}".format(self.function))
        if self.function in available_functions.keys():
            func = available_functions[self.function]
            func()
        else:
            raise NotImplementedError(
                f"The requested function {self.function} is not implemented."
            )

    def _delete_objects(self, bucket: str, objects_paths: list) -> None:
        """Delete objects recursively in s3.

        Params:
            bucket: name of bucket to perform the delete operation.
            objects_paths: objects to be deleted.
        """
        s3 = boto3.client("s3")

        for path in objects_paths:
            if _check_directory(bucket, path):
                path = get_directory_path(path)
            else:
                path = path.strip()

            more_objects = True
            paginator = ""
            objects_to_delete = []

            while more_objects:
                objects_found, paginator = _list_objects(
                    s3_client=s3, bucket=bucket, path=path, paginator=paginator
                )
                for obj in objects_found:
                    objects_to_delete.append({FileManagerAPIKeys.KEY.value: obj})

                if not paginator:
                    more_objects = False

                response = s3.delete_objects(
                    Bucket=bucket,
                    Delete={FileManagerAPIKeys.OBJECTS.value: objects_to_delete},
                )
                self._logger.info(response)
                objects_to_delete = []

    def delete_objects(self) -> None:
        """Delete objects and 'directories'.

        If dry_run is set to True the function will print a dict with all the
        paths that would be deleted based on the given keys.
        """
        bucket = self.configs["bucket"]
        objects_paths = self.configs["object_paths"]
        dry_run = self.configs["dry_run"]

        if dry_run:
            response = _dry_run(bucket=bucket, object_paths=objects_paths)

            self._logger.info("Paths that would be deleted:")
            self._logger.info(response)
        else:
            self._delete_objects(bucket, objects_paths)

    def copy_objects(self) -> None:
        """Copies objects and 'directories'.

        If dry_run is set to True the function will print a dict with all the
        paths that would be copied based on the given keys.
        """
        source_bucket = self.configs["bucket"]
        source_object = self.configs["source_object"]
        destination_bucket = self.configs["destination_bucket"]
        destination_object = self.configs["destination_object"]
        dry_run = self.configs["dry_run"]

        S3FileManager._copy_objects(
            source_bucket=source_bucket,
            source_object=source_object,
            destination_bucket=destination_bucket,
            destination_object=destination_object,
            dry_run=dry_run,
        )

    def move_objects(self) -> None:
        """Moves objects and 'directories'.

        If dry_run is set to True the function will print a dict with all the
        paths that would be moved based on the given keys.
        """
        pass

    def request_restore(self) -> None:
        """Request the restore of archived data."""
        source_bucket = self.configs["bucket"]
        source_object = self.configs["source_object"]
        restore_expiration = self.configs["restore_expiration"]
        retrieval_tier = self.configs["retrieval_tier"]
        dry_run = self.configs["dry_run"]

        ArchiveFileManager.request_restore(
            source_bucket,
            source_object,
            restore_expiration,
            retrieval_tier,
            dry_run,
        )

    def check_restore_status(self) -> None:
        """Check the restore status of archived data."""
        source_bucket = self.configs["bucket"]
        source_object = self.configs["source_object"]

        restore_status = ArchiveFileManager.check_restore_status(
            source_bucket, source_object
        )

        self._logger.info(
            f"""
            Restore status:
            - Not Started: {restore_status.get('not_started_objects')}
            - Ongoing: {restore_status.get('ongoing_objects')}
            - Restored: {restore_status.get('restored_objects')}
            Total objects in this restore process: {restore_status.get('total_objects')}
            """
        )

    def request_restore_to_destination_and_wait(self) -> None:
        """Request and wait for the restore to complete, polling the restore status.

        After the restore is done, copy the restored files to destination
        """
        source_bucket = self.configs["bucket"]
        source_object = self.configs["source_object"]
        destination_bucket = self.configs["destination_bucket"]
        destination_object = self.configs["destination_object"]
        restore_expiration = self.configs["restore_expiration"]
        retrieval_tier = self.configs["retrieval_tier"]
        dry_run = self.configs["dry_run"]

        ArchiveFileManager.request_restore_and_wait(
            source_bucket=source_bucket,
            source_object=source_object,
            restore_expiration=restore_expiration,
            retrieval_tier=retrieval_tier,
            dry_run=dry_run,
        )

        S3FileManager._logger.info(
            f"Restoration complete for {source_bucket} and {source_object}"
        )
        S3FileManager._logger.info(
            f"Starting to copy data from {source_bucket}/{source_object} to "
            f"{destination_bucket}/{destination_object}"
        )
        S3FileManager._copy_objects(
            source_bucket=source_bucket,
            source_object=source_object,
            destination_bucket=destination_bucket,
            destination_object=destination_object,
            dry_run=dry_run,
        )
        S3FileManager._logger.info(
            f"Finished copying data, data should be available on {destination_bucket}/"
            f"{destination_object}"
        )

    @staticmethod
    def _copy_objects(
        source_bucket: str,
        source_object: str,
        destination_bucket: str,
        destination_object: str,
        dry_run: bool,
    ) -> None:
        """Copies objects and 'directories' in s3.

        Args:
            source_bucket: name of bucket to perform the copy.
            source_object: object/folder to be copied.
            destination_bucket: name of the target bucket to copy.
            destination_object: target object/folder to copy.
            dry_run: if dry_run is set to True the function will print a dict with
                all the paths that would be deleted based on the given keys.
        """
        s3 = boto3.client("s3")

        if dry_run:
            response = _dry_run(bucket=source_bucket, object_paths=[source_object])

            S3FileManager._logger.info("Paths that would be copied:")
            S3FileManager._logger.info(response)
        else:
            original_object_name = source_object.split("/")[-1]

            if _check_directory(source_bucket, source_object):
                source_object = get_directory_path(source_object)

                copy_object = _list_objects_recursively(
                    bucket=source_bucket, path=source_object
                )

                for obj in copy_object:
                    S3FileManager._logger.info(f"Copying obj: {obj}")

                    final_path = obj.replace(source_object, "")

                    response = s3.copy_object(
                        Bucket=destination_bucket,
                        CopySource={
                            FileManagerAPIKeys.BUCKET.value: source_bucket,
                            FileManagerAPIKeys.KEY.value: obj,
                        },
                        Key=f"{destination_object}/{original_object_name}/{final_path}",
                    )
                    S3FileManager._logger.info(response)
            else:
                S3FileManager._logger.info(f"Copying obj: {source_object}")

                response = s3.copy_object(
                    Bucket=destination_bucket,
                    CopySource={
                        FileManagerAPIKeys.BUCKET.value: source_bucket,
                        FileManagerAPIKeys.KEY.value: source_object,
                    },
                    Key=f"""{destination_object}/{original_object_name}""",
                )
                S3FileManager._logger.info(response)


class ArchiveFileManager(object):
    """Set of actions to restore archives."""

    _logger = LoggingHandler(__name__).get_logger()

    @staticmethod
    def _get_archived_object(bucket: str, object_key: str) -> Optional[Any]:
        """Get the archived object if it's an object.

        Args:
            bucket: name of bucket to check get the object.
            object_key: object to get.

        Returns:
            S3 Object if it's an archived object, otherwise None.
        """
        s3 = boto3.resource("s3")
        object_to_restore = s3.Object(bucket, object_key)

        if (
            object_to_restore.storage_class is not None
            and object_to_restore.storage_class in ARCHIVE_STORAGE_CLASS
        ):
            return object_to_restore
        else:
            return None

    @staticmethod
    def _check_object_restore_status(
        bucket: str, object_key: str
    ) -> Optional[RestoreStatus]:
        """Check the restore status of the archive.

        Args:
            bucket: name of bucket to check the restore status.
            object_key: object to check the restore status.

        Returns:
            The restore status represented by an enum, possible values are:
                NOT_STARTED, ONGOING or RESTORED
        """
        archived_object = ArchiveFileManager._get_archived_object(bucket, object_key)

        if archived_object is None:
            status = None
        elif archived_object.restore is None:
            status = RestoreStatus.NOT_STARTED
        elif 'ongoing-request="true"' in archived_object.restore:
            status = RestoreStatus.ONGOING
        else:
            status = RestoreStatus.RESTORED

        return status

    @staticmethod
    def check_restore_status(source_bucket: str, source_object: str) -> dict:
        """Check the restore status of archived data.

        Args:
            source_bucket: name of bucket to check the restore status.
            source_object: object to check the restore status.

        Returns:
            A dict containing the amount of objects in each status.
        """
        not_started_objects = 0
        ongoing_objects = 0
        restored_objects = 0
        total_objects = 0

        if _check_directory(source_bucket, source_object):
            source_object = get_directory_path(source_object)

        objects_to_restore = _list_objects_recursively(
            bucket=source_bucket, path=source_object
        )

        for obj in objects_to_restore:
            ArchiveFileManager._logger.info(f"Checking restore status for: {obj}")

            restore_status = ArchiveFileManager._check_object_restore_status(
                source_bucket, obj
            )
            if not restore_status:
                ArchiveFileManager._logger.warning(
                    f"Restore status not found for {source_bucket}/{obj}"
                )
            else:
                total_objects += 1

                if RestoreStatus.NOT_STARTED == restore_status:
                    not_started_objects += 1
                elif RestoreStatus.ONGOING == restore_status:
                    ongoing_objects += 1
                else:
                    restored_objects += 1

                ArchiveFileManager._logger.info(
                    f"{obj} restore status is {restore_status.value}"
                )

        return {
            "total_objects": total_objects,
            "not_started_objects": not_started_objects,
            "ongoing_objects": ongoing_objects,
            "restored_objects": restored_objects,
        }

    @staticmethod
    def _request_restore_object(
        bucket: str, object_key: str, expiration: int, retrieval_tier: str
    ) -> None:
        """Request a restore of the archive.

        Args:
            bucket: name of bucket to perform the restore.
            object_key: object to be restored.
            expiration: restore expiration.
            retrieval_tier: type of restore, possible values are:
                Bulk, Standard or Expedited.
        """
        if not RestoreType.exists(retrieval_tier):
            raise RestoreTypeNotFoundException(
                f"Restore type {retrieval_tier} not supported."
            )

        if _check_directory(bucket, object_key):
            object_key = get_directory_path(object_key)

        archived_object = ArchiveFileManager._get_archived_object(bucket, object_key)

        if archived_object and archived_object.restore is None:
            ArchiveFileManager._logger.info(f"Restoring archive {bucket}/{object_key}.")
            archived_object.restore_object(
                RestoreRequest={
                    "Days": expiration,
                    "GlacierJobParameters": {"Tier": retrieval_tier},
                }
            )
        else:
            ArchiveFileManager._logger.info(
                f"Restore request for {bucket}/{object_key} not performed."
            )

    @staticmethod
    def request_restore(
        source_bucket: str,
        source_object: str,
        restore_expiration: int,
        retrieval_tier: str,
        dry_run: bool,
    ) -> None:
        """Request the restore of archived data.

        Args:
            source_bucket: name of bucket to perform the restore.
            source_object: object to be restored.
            restore_expiration: restore expiration in days.
            retrieval_tier: type of restore, possible values are:
                Bulk, Standard or Expedited.
            dry_run: if dry_run is set to True the function will print a dict with
                all the paths that would be deleted based on the given keys.
        """
        if _check_directory(source_bucket, source_object):
            source_object = get_directory_path(source_object)

        if dry_run:
            response = _dry_run(bucket=source_bucket, object_paths=[source_object])

            ArchiveFileManager._logger.info("Paths that would be restored:")
            ArchiveFileManager._logger.info(response)
        else:
            objects_to_restore = _list_objects_recursively(
                bucket=source_bucket, path=source_object
            )

            for obj in objects_to_restore:
                ArchiveFileManager._request_restore_object(
                    source_bucket,
                    obj,
                    restore_expiration,
                    retrieval_tier,
                )

    @staticmethod
    def request_restore_and_wait(
        source_bucket: str,
        source_object: str,
        restore_expiration: int,
        retrieval_tier: str,
        dry_run: bool,
    ) -> None:
        """Request and wait for the restore to complete, polling the restore status.

        Args:
            source_bucket: name of bucket to perform the restore.
            source_object: object to be restored.
            restore_expiration: restore expiration in days.
            retrieval_tier: type of restore, possible values are:
                Bulk, Standard or Expedited.
            dry_run: if dry_run is set to True the function will print a dict with
                all the paths that would be deleted based on the given keys.
        """
        if retrieval_tier != RestoreType.EXPEDITED.value:
            ArchiveFileManager._logger.error(
                f"Retrieval Tier {retrieval_tier} not allowed on this operation! This "
                "kind of restore should be used just with `Expedited` retrieval tier "
                "to save cluster costs."
            )
            raise ValueError(
                f"Retrieval Tier {retrieval_tier} not allowed on this operation! This "
                "kind of restore should be used just with `Expedited` retrieval tier "
                "to save cluster costs."
            )

        ArchiveFileManager.request_restore(
            source_bucket=source_bucket,
            source_object=source_object,
            restore_expiration=restore_expiration,
            retrieval_tier=retrieval_tier,
            dry_run=dry_run,
        )
        restore_status = ArchiveFileManager.check_restore_status(
            source_bucket, source_object
        )
        ArchiveFileManager._logger.info(f"Restore status: {restore_status}")

        if not dry_run:
            ArchiveFileManager._logger.info("Checking the restore status in 5 minutes.")
            wait_time = 300
            while restore_status.get("total_objects") > restore_status.get(
                "restored_objects"
            ):
                ArchiveFileManager._logger.info(
                    "Not all objects have been restored yet, checking the status again "
                    f"in {wait_time} seconds."
                )
                time.sleep(wait_time)
                wait_time = 30
                restore_status = ArchiveFileManager.check_restore_status(
                    source_bucket, source_object
                )
                ArchiveFileManager._logger.info(f"Restore status: {restore_status}")
