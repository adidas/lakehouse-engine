"""Module to define the behaviour to write to Sharepoint."""

import os
from collections import OrderedDict

from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import OutputSpec, SharepointOptions
from lakehouse_engine.io.exceptions import (
    EndpointNotFoundException,
    NotSupportedException,
    WriteToLocalException,
)
from lakehouse_engine.io.writer import Writer
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.sharepoint_utils import SharepointUtils


class SharepointWriter(Writer):
    """Class to write data to Sharepoint.

    This writer is designed specifically for uploading a single file
    to Sharepoint. It first writes the data locally before uploading
    it to the specified Sharepoint location. Since it handles only
    a single file at a time, any logic for writing multiple files
    must be implemented on the notebook-side.
    """

    def __init__(
        self, output_spec: OutputSpec, df: DataFrame, data: OrderedDict | None
    ) -> None:
        """Construct FileWriter instances.

        Args:
            output_spec: output specification
            df: dataframe to be written.
            data: list of all dfs generated on previous steps before writer.
        """
        super().__init__(output_spec, df, data)
        self.opts = self._get_sharepoint_opts()
        self.sharepoint_utils = self._get_sharepoint_utils()
        self._logger = LoggingHandler(__name__).get_logger()

    def _get_sharepoint_opts(self) -> SharepointOptions:
        """Return sharepoint options ensuring they are available."""
        if not self._output_spec.sharepoint_opts:
            raise ValueError("sharepoint_opts must be provided for Sharepoint writer.")
        return self._output_spec.sharepoint_opts

    def write(self) -> None:
        """Upload data to Sharepoint."""
        if self._df.isStreaming:
            raise NotSupportedException("Sharepoint writer doesn't support streaming!")

        self.opts.validate_for_writer()
        if not self.sharepoint_utils.check_if_endpoint_exists(
            folder_root_path=self.opts.folder_relative_path
        ):
            raise EndpointNotFoundException("The provided endpoint does not exist!")

        self._write_to_sharepoint_in_batch_mode(self._df)

    def _get_sharepoint_utils(self) -> SharepointUtils:
        sharepoint_utils = SharepointUtils(
            client_id=self.opts.client_id,
            tenant_id=self.opts.tenant_id,
            local_path=self.opts.local_path,
            api_version=self.opts.api_version,
            site_name=self.opts.site_name,
            drive_name=self.opts.drive_name,
            file_name=self.opts.file_name or "",
            folder_relative_path=self.opts.folder_relative_path,
            chunk_size=self.opts.chunk_size or (5 * 1024 * 1024),
            local_options=self.opts.local_options,
            secret=self.opts.secret or "",
            conflict_behaviour=self.opts.conflict_behaviour or "replace",
        )

        return sharepoint_utils

    def _write_to_sharepoint_in_batch_mode(self, df: DataFrame) -> None:
        """Write to Sharepoint in batch mode.

        This method first writes the provided DataFrame to a local file using the
        SharePointUtils `write_to_local_path` method. If the local file is successfully
        written, it then uploads the file to Sharepoint using the `write_to_sharepoint`
        method, logging the process and outcome.

        Args:
            df: The DataFrame to write to a local file and subsequently
                upload to Sharepoint.
        """
        local_path = self.opts.local_path
        file_name = self.opts.file_name

        self._logger.info(f"Starting to write the data to the local path: {local_path}")

        try:
            self.sharepoint_utils.write_to_local_path(df)
        except OSError as err:
            self.sharepoint_utils.delete_local_path()
            self._logger.info(f"Deleted the local folder: {local_path}")
            raise WriteToLocalException(
                f"The data was not written on the local path: {local_path}"
            ) from err

        self._logger.info(f"The data was written to the local path: {local_path}")
        file_size = os.path.getsize(local_path)
        self._logger.info(
            f"Uploading the {file_name} ({file_size} bytes) to Sharepoint."
        )
        self.sharepoint_utils.write_to_sharepoint()
        self._logger.info(f"The {file_name} was uploaded to Sharepoint with success!")
        self.sharepoint_utils.delete_local_path()
        self._logger.info(f"Deleted the local folder: {local_path}")
