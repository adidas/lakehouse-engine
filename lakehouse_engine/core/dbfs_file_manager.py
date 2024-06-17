"""File manager module using dbfs."""

from lakehouse_engine.core.file_manager import FileManager
from lakehouse_engine.utils.databricks_utils import DatabricksUtils
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
        path = _get_path(bucket, path)

        object_list: list = []
        object_list = _list_objects(path, object_list)

        if object_list:
            response[path] = object_list
        else:
            response[path] = ["No such key"]

    return response


def _list_objects(path: str, objects_list: list) -> list:
    """List all the objects in a path.

    Args:
        path: path to be used to perform the list.
        objects_list: A list of object names, empty by default.

    Returns:
         A list of object names.
    """
    from lakehouse_engine.core.exec_env import ExecEnv

    ls_objects_list = DatabricksUtils.get_db_utils(ExecEnv.SESSION).fs.ls(path)

    for file_or_directory in ls_objects_list:
        if file_or_directory.isDir():
            _list_objects(file_or_directory.path, objects_list)
        else:
            objects_list.append(file_or_directory.path)
    return objects_list


def _get_path(bucket: str, path: str) -> str:
    """Get complete path.

    For s3 path, the bucket (e.g. bucket-example) and path
    (e.g. folder1/folder2) will be filled with part of the path.
    For dbfs path, the path will have the complete path
    (dbfs:/example) and bucket as null.

    Args:
        bucket: bucket for s3 objects.
        path: path to access the directory of file.

    Returns:
         The complete path with or without bucket.
    """
    if bucket.strip():
        path = f"s3://{bucket}/{path}".strip()
    else:
        path = path.strip()

    return path


class DBFSFileManager(FileManager):
    """Set of actions to manipulate dbfs files in several ways."""

    _logger = LoggingHandler(__name__).get_logger()

    def get_function(self) -> None:
        """Get a specific function to execute."""
        available_functions = {
            "delete_objects": self.delete_objects,
            "copy_objects": self.copy_objects,
            "move_objects": self.move_objects,
        }

        self._logger.info("Function being executed: {}".format(self.function))
        if self.function in available_functions.keys():
            func = available_functions[self.function]
            func()
        else:
            raise NotImplementedError(
                f"The requested function {self.function} is not implemented."
            )

    @staticmethod
    def _delete_objects(bucket: str, objects_paths: list) -> None:
        """Delete objects recursively.

        Params:
            bucket: name of bucket to perform the delete operation.
            objects_paths: objects to be deleted.
        """
        from lakehouse_engine.core.exec_env import ExecEnv

        for path in objects_paths:
            path = _get_path(bucket, path)

            DBFSFileManager._logger.info(f"Deleting: {path}")

            try:
                delete_operation = DatabricksUtils.get_db_utils(ExecEnv.SESSION).fs.rm(
                    path, True
                )

                if delete_operation:
                    DBFSFileManager._logger.info(f"Deleted: {path}")
                else:
                    DBFSFileManager._logger.info(f"Not able to delete: {path}")
            except Exception as e:
                DBFSFileManager._logger.error(f"Error deleting {path} - {e}")
                raise e

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

        if dry_run:
            response = _dry_run(bucket=source_bucket, object_paths=[source_object])

            self._logger.info("Paths that would be copied:")
            self._logger.info(response)
        else:
            self._copy_objects(
                source_bucket=source_bucket,
                source_object=source_object,
                destination_bucket=destination_bucket,
                destination_object=destination_object,
            )

    @staticmethod
    def _copy_objects(
        source_bucket: str,
        source_object: str,
        destination_bucket: str,
        destination_object: str,
    ) -> None:
        """Copies objects and 'directories'.

        Args:
            source_bucket: name of bucket to perform the copy.
            source_object: object/folder to be copied.
            destination_bucket: name of the target bucket to copy.
            destination_object: target object/folder to copy.
        """
        from lakehouse_engine.core.exec_env import ExecEnv

        copy_from = _get_path(source_bucket, source_object)
        copy_to = _get_path(destination_bucket, destination_object)

        DBFSFileManager._logger.info(f"Copying: {copy_from} to {copy_to}")

        try:
            DatabricksUtils.get_db_utils(ExecEnv.SESSION).fs.cp(
                copy_from, copy_to, True
            )

            DBFSFileManager._logger.info(f"Copied: {copy_from} to {copy_to}")
        except Exception as e:
            DBFSFileManager._logger.error(
                f"Error copying file {copy_from} to {copy_to} - {e}"
            )
            raise e

    def move_objects(self) -> None:
        """Moves objects and 'directories'.

        If dry_run is set to True the function will print a dict with all the
        paths that would be moved based on the given keys.
        """
        source_bucket = self.configs["bucket"]
        source_object = self.configs["source_object"]
        destination_bucket = self.configs["destination_bucket"]
        destination_object = self.configs["destination_object"]
        dry_run = self.configs["dry_run"]

        if dry_run:
            response = _dry_run(bucket=source_bucket, object_paths=[source_object])

            self._logger.info("Paths that would be moved:")
            self._logger.info(response)
        else:
            self._move_objects(
                source_bucket=source_bucket,
                source_object=source_object,
                destination_bucket=destination_bucket,
                destination_object=destination_object,
            )

    @staticmethod
    def _move_objects(
        source_bucket: str,
        source_object: str,
        destination_bucket: str,
        destination_object: str,
    ) -> None:
        """Moves objects and 'directories'.

        Args:
            source_bucket: name of bucket to perform the move.
            source_object: object/folder to be moved.
            destination_bucket: name of the target bucket to move.
            destination_object: target object/folder to move.
        """
        from lakehouse_engine.core.exec_env import ExecEnv

        move_from = _get_path(source_bucket, source_object)
        move_to = _get_path(destination_bucket, destination_object)

        DBFSFileManager._logger.info(f"Moving: {move_from} to {move_to}")

        try:
            DatabricksUtils.get_db_utils(ExecEnv.SESSION).fs.mv(
                move_from, move_to, True
            )

            DBFSFileManager._logger.info(f"Moved: {move_from} to {move_to}")
        except Exception as e:
            DBFSFileManager._logger.error(
                f"Error moving file {move_from} to {move_to} - {e}"
            )
            raise e
