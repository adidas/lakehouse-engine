"""Utilities for sharepoint API operations."""

from __future__ import annotations

import os
import shutil

import msal
import requests
from pyspark.sql import DataFrame
from requests import RequestException
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.exceptions import SharePointAPIError
from lakehouse_engine.utils.logging_handler import LoggingHandler

_logger = LoggingHandler(__name__).get_logger()


class SharepointUtils(object):
    """Class with methods to connect and extract data from SharePoint."""

    def __init__(
        self,
        client_id: str,
        tenant_id: str,
        local_path: str,
        api_version: str,
        site_name: str,
        drive_name: str,
        file_name: str,
        secret: str,
        folder_relative_path: str = None,
        chunk_size: int = 100,
        local_options: dict = None,
        conflict_behaviour: str = "replace",
    ):
        """Instantiate objects of the SharepointUtils class.

        Args:
            client_id: application (client) ID of your Azure AD app.
            tenant_id: tenant ID (directory ID) from Azure AD for authentication.
            local_path: local directory path (Volume) where the files are temporarily
            stored.
            api_version: Graph API version to use.
            site_name: name of the SharePoint site where the files are stored.
            drive_name: name of the document library or drive in SharePoint.
            file_name: name of the file to be stored in sharepoint.
            secret: client secret for authentication.
            folder_relative_path: optional; relative path within the
            drive(drive_name) where the file will be stored.
            chunk_size: Optional; size of file chunks to be uploaded in
            bytes (default is 100 bytes).
            local_options: Optional; additional options for customizing write
            action to local path.
            conflict_behaviour: Optional; defines how conflicts in file uploads are
            handled('replace', 'fail', etc.).

        Returns:
            A SharepointUtils object.
        """
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.local_path = local_path
        self.api_version = api_version
        self.site_name = site_name
        self.drive_name = drive_name
        self.file_name = file_name
        self.secret = secret
        self.folder_relative_path = folder_relative_path
        self.chunk_size = chunk_size
        self.local_options = local_options
        self.conflict_behaviour = conflict_behaviour

        self.token = None

        self._create_app()

    def _get_token(self) -> None:
        """Fetch and store a valid access token for SharePoint API."""
        try:
            self.token = self.app.acquire_token_for_client(
                scopes=[f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/.default"]
            )
        except Exception as err:
            _logger.error(f"Token acquisition error: {err}")

    def _create_app(self) -> None:
        """Create an MSAL (Microsoft Authentication Library) instance.

        This is used to handle authentication and authorization with Azure AD.
        """
        self.app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            authority=f"{ExecEnv.ENGINE_CONFIG.sharepoint_authority}/{self.tenant_id}",
            client_credential=self.secret,
        )

        self._get_token()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=30, min=30, max=150),
        retry=retry_if_exception_type(
            (RequestException, SharePointAPIError)
        ),  # Retry on these exceptions
    )
    def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        headers: dict = None,
        json_options: dict = None,
        data: object = None,
    ) -> requests.Response:
        """Execute API requests to Microsoft Graph API.

        !!! note
            If you try to upload large files sequentially,you may encounter
            a 503 "serviceNotAvailable" error. To mitigate this, consider using
            coalesce in the Acon transform specification. However, be aware that
            increasing the number of partitions also increases the likelihood of
            server throttling

        Args:
            endpoint: The API endpoint to call.
            headers: A dictionary containing the necessary headers.
            json_options: Optional; JSON data to include in the request body.
            method: The HTTP method to use ('GET', 'POST', 'PUT', etc.).
            data: Optional; additional data (e.g., file content) on request body.

        Returns:
            A Response object from the request library.

        Raises:
            SharePointAPIError: If there is an issue with the SharePoint
            API request.
        """
        self._get_token()

        # Required to avoid cicd issue
        if not self.token or "access_token" not in self.token:
            raise SharePointAPIError("Authentication token is missing or invalid.")

        try:
            if "access_token" in self.token:
                response = requests.request(
                    method=method,
                    url=endpoint,
                    headers=(
                        headers
                        if headers
                        else {"Authorization": "Bearer " + self.token["access_token"]}
                    ),
                    json=json_options,
                    data=data,
                )
                return response
        except RequestException as error:
            raise SharePointAPIError(f"{error}")

    def _get_site_id(self, site_name: str) -> str:
        """Get the site ID from the site name.

        Args:
            site_name: The name of the SharePoint site.

        Returns:
            The site ID as a string.
        """
        endpoint = (
            f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/{self.api_version}"
            f"/sites/{ExecEnv.ENGINE_CONFIG.sharepoint_company_domain}:/"
            f"sites/{site_name}"
        )
        response = self._make_request(endpoint=endpoint).json()

        try:
            if not isinstance(response["id"], str):
                raise TypeError("site_id must be a string")
            return response["id"]
        except RequestException as error:
            raise SharePointAPIError(f"{error}")

    def _get_drive_id(self, site_name: str, drive_name: str) -> str:
        """Get the drive ID from the site name and drive name.

        Args:
            site_name: The name of the SharePoint site.
            drive_name: The name of the drive or document library.

        Returns:
            The drive ID as a string.

        Raises:
            ValueError: If the drive is not found.
        """
        site_id = self._get_site_id(site_name)

        endpoint = (
            f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/{self.api_version}"
            f"/sites/{site_id}/drives"
        )

        try:
            response = self._make_request(endpoint=endpoint).json()
            for drive in response["value"]:
                if drive_name in drive["name"] and isinstance(drive["name"], str):
                    return str(drive["id"])
            raise ValueError(f"Requested drive '{drive_name}' could not be found")
        except RequestException as error:
            raise SharePointAPIError(f"{error}")

    def _rename_local_file(self, local_path: str, file_name: str) -> None:
        """Rename a local file that starts with 'part-' to the desired file name.

        Args:
            local_path: The directory where the file is located.
            file_name: The new file name for the local file.
        """
        files_in_dir = os.listdir(local_path)

        part_file = [f for f in files_in_dir if f.startswith("part-")][0]

        try:
            os.rename(
                os.path.join(local_path, part_file), os.path.join(local_path, file_name)
            )
        except IOError as error:
            raise SharePointAPIError(f"{error}")

    def check_if_endpoint_exists(
        self, site_name: str, drive_name: str, folder_root_path: str = None
    ) -> bool:
        """Check if a specified endpoint exists in SharePoint.

        This method checks whether a specific endpoint exists within a SharePoint site.
        If `folder_root_path` is provided, the method checks for the existence of that
        specific folder within the drive.

        Args:
            site_name: Name of the SharePoint site.
            drive_name: Name of the SharePoint drive (document library)
            to search within.
            folder_root_path: Optional; the relative path of the folder within
            the drive. If not provided, the existence of the drive is checked.

        Returns:
            True if the file or folder exists, False otherwise.

        Raises:
            SharePointAPIError: If there's an issue with SharePoint API request.
        """
        try:
            self._get_site_id(site_name)
            drive_id = self._get_drive_id(site_name, drive_name)

            if folder_root_path:
                query_body = (
                    f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}"
                    f"/{self.api_version}/drives/{drive_id}"
                    f"/items/root:/{folder_root_path}"
                )
                self._make_request(endpoint=query_body)
            return True
        except RequestException as error:
            raise SharePointAPIError(f"{error}")

    def check_if_local_path_exists(self, local_path: str) -> None:
        """Check if a specified local path exists.

        This method checks whether a specific local path exists.

        Args:
            local_path: Local path (Volume) where the files are temporarily stored.

        Returns:
            True if the folder exists, False otherwise.

        Raises:
            IOError: If there is an error when reading from the local path.
        """
        try:
            os.listdir(local_path)
        except IOError as error:
            raise SharePointAPIError(f"{error}")

    def write_to_local_path(self, df: DataFrame) -> None:
        """Write a Spark DataFrame to a local path (Volume) in CSV format.

        This method writes the provided Spark DataFrame to a specified local directory,
        saving it in CSV format. The method renames the output file from its default
        "part-*" naming convention to a specified file name.
        The dictionary local_options enables the customisation of the write action.
        The customizable options can be found here:
        https://spark.apache.org/docs/3.5.1/sql-data-sources-csv.html.

        Args:
            df: The Spark DataFrame to write to the local file system.

        Returns:
            None.

        Raises:
            IOError: If there is an issue during the file writing process.
        """
        try:
            df.coalesce(1).write.save(
                path=self.local_path,
                format="csv",
                **self.local_options if self.local_options else {},
            )
            self._rename_local_file(self.local_path, self.file_name)
        except IOError as error:
            raise SharePointAPIError(f"{error}")

    def delete_local_path(self) -> None:
        """Delete a temporary folder.

        This method deletes all the files in a given directory.

        Returns:
            None.

        Raises:
            IOError: If there is an issue during the file writing process.
        """
        try:
            shutil.rmtree(self.local_path)
        except IOError as error:
            raise SharePointAPIError(f"{error}")

    def write_to_sharepoint(self) -> None:
        """Upload a local file to SharePoint in chunks using the Microsoft Graph API.

        This method creates an upload session and uploads a local CSV file to a
        SharePoint document library.
        The file is divided into chunks (based on the `chunk_size` specified)
        to handle large file uploads and send sequentially using the upload URL
        returned from the Graph API.

        The method uses instance attributes such as `api_domain`, `api_version`,
        `site_name`, `drive_name`, `folder_relative_path`, and `file_name` to
        construct the necessary API calls and upload the file to the specified
        location in SharePoint.

        Returns:
            None.

        Raises:
            APIError: If an error occurs during any stage of the upload
            (e.g., failure to create upload session,issues during chunk upload).
        """
        drive_id = self._get_drive_id(
            site_name=self.site_name, drive_name=self.drive_name
        )

        if self.folder_relative_path:
            endpoint = (
                f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}"
                f"/{self.api_version}/drives/{drive_id}/items/root:"
                f"/{self.folder_relative_path}/{self.file_name}.csv:"
                f"/createUploadSession"
            )
        else:
            endpoint = (
                f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}"
                f"/{self.api_version}/drives/{drive_id}/items/root:"
                f"/{self.file_name}.csv:/createUploadSession"
            )

        response = self._make_request(method="POST", endpoint=endpoint)
        response.raise_for_status()
        upload_session = response.json()
        upload_url = upload_session["uploadUrl"]

        upload_file = f"{self.local_path}{self.file_name}"
        stat = os.stat(upload_file)
        size = stat.st_size

        with open(upload_file, "rb") as data:
            start = 0
            while start < size:
                chunk = data.read(self.chunk_size)
                bytes_read = len(chunk)
                upload_range = f"bytes {start}-{start + bytes_read - 1}/{size}"
                headers = {
                    "Content-Length": str(bytes_read),
                    "Content-Range": upload_range,
                }
                response = self._make_request(
                    method="PUT", endpoint=upload_url, headers=headers, data=chunk
                )
                response.raise_for_status()
                start += bytes_read
