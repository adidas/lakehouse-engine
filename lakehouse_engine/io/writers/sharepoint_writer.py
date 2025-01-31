"""Module to define the behaviour to write to Sharepoint."""

import os
from typing import OrderedDict

from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import OutputSpec
from lakehouse_engine.io.exceptions import (
    EndpointNotFoundException,
    InputNotFoundException,
    NotSupportedException,
    WriteToLocalException,
)
from lakehouse_engine.io.writer import Writer
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.sharepoint_utils import SharepointUtils


class SharepointWriter(Writer):
    """Class to write data to SharePoint.

    This writer is designed specifically for uploading a single file
    to SharePoint. It first writes the data locally before uploading
    it to the specified SharePoint location. Since it handles only
    a single file at a time, any logic for writing multiple files
    must be implemented on the notebook-side.
    """

    def __init__(self, output_spec: OutputSpec, df: DataFrame, data: OrderedDict):
        """Construct FileWriter instances.

        Args:
            output_spec: output specification
            df: dataframe to be written.
            data: list of all dfs generated on previous steps before writer.
        """
        super().__init__(output_spec, df, data)
        self.sharepoint_utils = self._get_sharepoint_utils()
        self._logger = LoggingHandler(__name__).get_logger()

    def write(self) -> None:
        """Upload data to sharepoint."""
        if self._df.isStreaming:
            raise NotSupportedException("Sharepoint writer doesn't support streaming!")
        elif any(
            not getattr(self._output_spec.sharepoint_opts, attr)
            for attr in ["site_name", "drive_name", "local_path"]
        ):
            raise InputNotFoundException(
                f"Please provide all mandatory Sharepoint options. \n"
                f"Expected: site_name, drive_name and local_path. "
                f"Value should not be None.\n"
                f"Provided: site_name={self._output_spec.sharepoint_opts.site_name}, \n"
                f"drive_name={self._output_spec.sharepoint_opts.drive_name}, \n"
                f"local_path={self._output_spec.sharepoint_opts.local_path}"
            )
        elif not self.sharepoint_utils.check_if_endpoint_exists(
            site_name=self._output_spec.sharepoint_opts.site_name,
            drive_name=self._output_spec.sharepoint_opts.drive_name,
            folder_root_path=self._output_spec.sharepoint_opts.folder_relative_path,
        ):
            raise EndpointNotFoundException("The provided endpoint does not exist!")
        else:
            self._write_to_sharepoint_in_batch_mode(self._df)

    def _get_sharepoint_utils(self) -> SharepointUtils:
        sharepoint_utils = SharepointUtils(
            client_id=self._output_spec.sharepoint_opts.client_id,
            tenant_id=self._output_spec.sharepoint_opts.tenant_id,
            local_path=self._output_spec.sharepoint_opts.local_path,
            api_version=self._output_spec.sharepoint_opts.api_version,
            site_name=self._output_spec.sharepoint_opts.site_name,
            drive_name=self._output_spec.sharepoint_opts.drive_name,
            file_name=self._output_spec.sharepoint_opts.file_name,
            folder_relative_path=self._output_spec.sharepoint_opts.folder_relative_path,
            chunk_size=self._output_spec.sharepoint_opts.chunk_size,
            local_options=self._output_spec.sharepoint_opts.local_options,
            secret=self._output_spec.sharepoint_opts.secret,
            conflict_behaviour=self._output_spec.sharepoint_opts.conflict_behaviour,
        )

        return sharepoint_utils

    def _write_to_sharepoint_in_batch_mode(self, df: DataFrame) -> None:
        """Write to Sharepoint in batch mode.

        This method first writes the provided DataFrame to a local file using the
        SharePointUtils `write_to_local_path` method. If the local file is successfully
        written, it then uploads the file to SharePoint using the `write_to_sharepoint`
        method, logging the process and outcome.

        Args:
            df: The DataFrame to write to a local file and subsequently
                upload to SharePoint.
        """
        local_path = self._output_spec.sharepoint_opts.local_path
        file_name = self._output_spec.sharepoint_opts.file_name

        self._logger.info(f"Starting to write the data to the local path: {local_path}")

        try:
            self.sharepoint_utils.write_to_local_path(df)
        except IOError as err:
            self.sharepoint_utils.delete_local_path()
            self._logger.info(f"Deleted the local folder: {local_path}")
            raise WriteToLocalException(
                f"The data was not written on the local path: {local_path}"
            ) from err

        self._logger.info(f"The data was written to the local path: {local_path}")
        file_size = os.path.getsize(local_path)
        self._logger.info(
            f"Uploading the {file_name} ({file_size} bytes) to SharePoint."
        )
        self.sharepoint_utils.write_to_sharepoint()
        self._logger.info(f"The {file_name} was uploaded to SharePoint with success!")
        self.sharepoint_utils.delete_local_path()
        self._logger.info(f"Deleted the local folder: {local_path}")
