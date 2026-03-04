"""Utilities for sharepoint API operations."""

from __future__ import annotations

import os
import shutil
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, cast

import requests
from pyspark.sql import DataFrame
from requests import RequestException
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from lakehouse_engine.core.definitions import SharepointFile
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.exceptions import SharePointAPIError
from lakehouse_engine.utils.logging_handler import LoggingHandler

_logger = LoggingHandler(__name__).get_logger()


class SharepointUtils(object):
    """Class with methods to connect and extract data from Sharepoint."""

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
        chunk_size: int = 5 * 1024 * 1024,  # 5 MB
        local_options: dict = None,
        conflict_behaviour: str = "replace",
        file_pattern: str = None,
        file_type: str = None,
    ):
        """Instantiate objects of the SharepointUtils class.

        Args:
            client_id: application (client) ID of your Azure AD app.
            tenant_id: tenant ID (directory ID) from Azure AD for authentication.
            local_path: local directory path (Volume) where the files are temporarily
            stored.
            api_version: Graph API version to use.
            site_name: name of the Sharepoint site where the files are stored.
            drive_name: name of the document library or drive in Sharepoint.
            file_name: name of the file to be stored in sharepoint.
            secret: client secret for authentication.
            folder_relative_path: optional; relative path within the
            drive(drive_name) where the file will be stored.
            chunk_size: Optional; size of file chunks to be uploaded/downloaded
            in bytes (default is 5 MB).
            local_options: Optional; additional options for customizing write
            action to local path.
            conflict_behaviour: Optional; defines how conflicts in file uploads are
            handled('replace', 'fail', etc.).
            file_pattern: Optional; pattern to match files in Sharepoint (e.g.,
            'data_*').
            file_type: Optional; type of the file to be stored in Sharepoint (e.g.,
            'csv').

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
        self.site_id = None
        self.drive_id = None
        self.token = None
        self.file_pattern = file_pattern
        self.file_type = file_type

        self._create_app()

    def _get_token(self) -> None:
        """Fetch and store a valid access token for Sharepoint API."""
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
        import msal

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
        stream: bool = False,
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
            SharePointAPIError: If there is an issue with the Sharepoint
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
                    stream=stream,
                )
                return response
        except RequestException as error:
            raise SharePointAPIError(f"{error}")

    def _parse_json(self, response: requests.Response, context: str) -> Dict[str, Any]:
        """Parse JSON response and raise on errors.

        Args:
            response: HTTP response object.
            context: Operation context for error logging.

        Returns:
            Parsed JSON as a dictionary.

        Raises:
            HTTPError: If the request fails.
            ValueError: If the response is not valid JSON.
        """
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            _logger.error(
                "HTTP error while %s: %s | body: %s", context, e, response.text[:200]
            )
            raise
        try:
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError(f"Expected dict JSON while {context}")
            return data
        except (requests.JSONDecodeError, ValueError):
            _logger.error(
                "Non-JSON or wrong type while %s. Body preview: %s",
                context,
                response.text[:200],
            )
            raise

    def _get_site_id(self) -> str:
        """Get site ID from site name, with caching.

        Returns:
            Site ID as a string.

        Raises:
            SharepointAPIError: If the request fails.
            RuntimeError: For unexpected errors or missing site ID.
        """
        if self.site_id is not None:
            return self.site_id

        endpoint = (
            f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/{self.api_version}"
            f"/sites/{ExecEnv.ENGINE_CONFIG.sharepoint_company_domain}:/"
            f"sites/{self.site_name}"
        )
        try:
            response = self._make_request(endpoint=endpoint)
            response_data = self._parse_json(
                response, f"getting site id for site '{self.site_name}'"
            )

            self.site_id = response_data.get("id")

            if not self.site_id:
                raise ValueError(
                    f"Site ID not found for site '{self.site_name}' in the API "
                    f"response: {response_data}"
                )

            return self.site_id

        except RequestException as error:
            raise SharePointAPIError(f"{error}")
        except Exception as e:
            raise RuntimeError(
                f"Unexpected error while reading site ID for site '{self.site_name}':"
                f"{e}"
            )

    def _get_drive_id(self) -> str:
        """Get drive ID from site ID and drive name, with caching.

        Returns:
            Drive ID as a string.

        Raises:
            SharepointAPIError: If the request fails.
            ValueError: If no drive is found.
        """
        if self.drive_id is not None:
            return str(self.drive_id)

        site_id = self._get_site_id()

        endpoint = (
            f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/"
            f"{self.api_version}/sites/{site_id}/drives"
        )

        try:
            response = self._make_request(endpoint=endpoint)
            response_data = self._parse_json(response, "listing drives for site")

            drives = response_data.get("value", [])
            if not drives:
                raise ValueError(f"No drives found for site '{self.site_id}'.")

            for drive in drives:
                if self.drive_name.strip().lower() == drive["name"].strip().lower():
                    drive_id = drive["id"]
                    self.drive_id = drive_id
                    return str(drive_id)

            raise ValueError(
                f"Drive '{self.drive_name}' could not be found in site '{site_id}'."
            )

        except RequestException as error:
            raise SharePointAPIError(f"Request error: {error}")

    def check_if_endpoint_exists(
        self, folder_root_path: str = None, raise_error: bool = True
    ) -> bool:
        """Check if a Sharepoint drive or folder exists.

        Args:
            folder_root_path: Optional folder path to check.
            raise_error: Raise error if the folder doesn't exist.

        Returns:
            True if the endpoint exists, False otherwise.

        Raises:
            SharepointAPIError: If the endpoint doesn't exist and raise_error is True.
        """
        try:
            site_id = self._get_site_id()
            drive_id = self._get_drive_id()

            if not folder_root_path:
                return True

            endpoint = (
                f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/"
                f"{self.api_version}/sites/{site_id}/drives/{drive_id}"
                f"/root:/{folder_root_path}"
            )

            response = self._make_request(endpoint=endpoint)
            response.raise_for_status()
            return True

        except requests.HTTPError as error:
            if error.response.status_code == 404:
                _logger.warning(f"Sharepoint path doesn't exist: {folder_root_path}")
                if raise_error:
                    raise SharePointAPIError(
                        f"Path '{folder_root_path}' doesn't exist!"
                    )
                return False
            raise

    def check_if_local_path_exists(self, local_path: str) -> None:
        """Verify that a local path exists.

        Args:
            local_path: Local folder where files are temporarily stored.

        Raises:
            SharePointAPIError: If the path cannot be read.
        """
        try:
            os.listdir(local_path)
        except IOError as error:
            raise SharePointAPIError(f"{error}")

    def save_to_staging_area(self, sp_file: SharepointFile) -> str:
        """Save a Sharepoint file locally (direct write or streaming).

        If the file is under the threshold and already loaded in memory, write its
        content directly.
        Otherwise, download the file via streaming to avoid memory overload.

        Args:
            sp_file: File metadata and content.

        Returns:
            Local file path.

        Raises:
            SharePointAPIError: On download or write failure.
        """
        try:
            if sp_file.content and sp_file.content_size < (500 * 1024 * 1024):
                _logger.info(
                    f"Writing '{sp_file.file_name}' via direct write (under 500MB)."
                )
                return self.write_bytes_to_local_file(sp_file)

            _logger.info(
                f"Writing '{sp_file.file_name}' via streaming (500MB+ or content not"
                f" loaded)."
            )
            return self.download_file_streaming(sp_file)

        except Exception as e:
            raise SharePointAPIError(f"Failed to write '{sp_file.file_name}': {e}")

    def download_file_streaming(self, sp_file: SharepointFile) -> str:
        """Download a large file from Sharepoint in chunks to a local path.

        Uses the configured chunk size to avoid memory overload with large files.

        Args:
            sp_file: File with remote path and name.

        Returns:
            Local file path.

        Raises:
            SharePointAPIError: If the download fails.
        """
        try:
            site_id = self._get_site_id()
            drive_id = self._get_drive_id()
            url = (
                f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/{self.api_version}/"
                f"sites/{site_id}/drives/{drive_id}/root:/{sp_file.file_path}:/content"
            )

            local_file_path = Path(self.local_path) / sp_file.file_name
            local_file_path.parent.mkdir(parents=True, exist_ok=True)

            with self._make_request(endpoint=url, stream=True) as response:
                response.raise_for_status()
                with open(local_file_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        if chunk:
                            file.write(chunk)

            return str(local_file_path)

        except requests.RequestException as error:
            raise SharePointAPIError(f"Failed to stream download: {error}")

    def write_bytes_to_local_file(self, sp_file: SharepointFile) -> str:
        """Write Sharepoint file content (bytes) to a local path.

        Args:
            sp_file: File with content and metadata.

        Returns:
            Local file path.

        Raises:
            ValueError: If content is missing.
            RuntimeError: If writing to disk fails.
        """
        if not sp_file.content:
            raise ValueError(
                f"Cannot write file '{sp_file.file_name}': Content is empty."
            )

        try:
            # Local base path (e.g., Unity Volumes, DBFS, or other mounted storage)
            local_base_path = Path(self.local_path)
            local_base_path.mkdir(parents=True, exist_ok=True)
            file_path = local_base_path / sp_file.file_name
            file_path.write_bytes(sp_file.content)
            return str(file_path)
        except Exception as e:
            raise RuntimeError(
                f"Failed to write file '{sp_file.file_name}' to Unity Volume: {e}"
            )

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
            df.coalesce(1).write.mode("overwrite").save(
                path=self.local_path,
                format="csv",
                **self.local_options if self.local_options else {},
            )
            self._rename_local_file(self.local_path, self.file_name)
        except IOError as error:
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

    def write_to_sharepoint(self) -> None:
        """Upload a local file to Sharepoint in chunks using the Microsoft Graph API.

        This method creates an upload session and uploads a local CSV file to a
        Sharepoint document library.
        The file is divided into chunks (based on the `chunk_size` specified)
        to handle large file uploads and send sequentially using the upload URL
        returned from the Graph API.

        The method uses instance attributes such as `api_domain`, `api_version`,
        `site_name`, `drive_name`, `folder_relative_path`, and `file_name` to
        construct the necessary API calls and upload the file to the specified
        location in Sharepoint.

        Returns:
            None.

        Raises:
            APIError: If an error occurs during any stage of the upload
            (e.g., failure to create upload session,issues during chunk upload).
        """
        drive_id = self._get_drive_id()

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

        upload_file = str(Path(self.local_path) / self.file_name)
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

    def delete_local_path(self) -> None:
        """Delete and recreate the local path used for temporary storage.

        Raises:
            SharePointAPIError: If deletion or recreation fails.
        """
        try:
            local_path = Path(self.local_path)
            if local_path.exists():
                shutil.rmtree(local_path)
            local_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise SharePointAPIError(f"Failed to clear or recreate local path: {e}")

    @contextmanager
    def staging_area(self) -> Generator[str, None, None]:
        """Provide a clean local staging folder for Sharepoint files.

        Yield the local path after ensuring it's empty. Cleans up after use.

        Yield:
            Path to the staging folder as a string.
        """
        self.delete_local_path()
        try:
            yield self.local_path
        finally:
            try:
                self.delete_local_path()
            except Exception as e:
                _logger.warning(f"Failed to clean up local path: {e}")

    def list_items_in_path(self, path: str) -> list[Any]:
        """List items (files/folders) at a Sharepoint path.

        Args:
            path: Relative folder or file path.

        Returns:
            List of items; files include @microsoft.graph.downloadUrl.

        Raises:
            ValueError: If the path is invalid or not found.
        """
        site_id = self._get_site_id()
        drive_id = self._get_drive_id()

        path = path.strip("/")
        if not path:
            resp = self._make_request(
                f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/{self.api_version}/"
                f"sites/{site_id}/drives/{drive_id}/root/children"
            )
            data = self._parse_json(resp, "listing root children")
            return cast(List[dict[str, Any]], data.get("value", []))

        path_parts = path.split("/")

        # start from root children
        resp = self._make_request(
            f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/{self.api_version}/sites/"
            f"{site_id}/drives/{drive_id}/root/children"
        )
        data = self._parse_json(resp, "listing root children")
        items = cast(List[dict[str, Any]], data.get("value", []))

        for component in path_parts:
            current_item = next(
                (item for item in items if item.get("name") == component), None
            )

            if not current_item:
                raise ValueError(f"Path component '{component}' not found in '{path}'.")

            if "folder" in current_item:
                # descend into folder
                resp = self._make_request(
                    f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/{self.api_version}/"
                    f"sites/{site_id}/drives/{drive_id}/items/"
                    f"{current_item['id']}/children"
                )
                data = self._parse_json(resp, f"listing children for '{component}'")
                items = cast(List[dict[str, Any]], data.get("value", []))
            else:
                # it's a file; ensure we have downloadUrl
                if "@microsoft.graph.downloadUrl" not in current_item:
                    resp = self._make_request(
                        f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/"
                        f"{self.api_version}/sites/{site_id}/drives/{drive_id}/"
                        f"items/{current_item['id']}"
                    )
                    current_item = self._parse_json(
                        resp, f"fetching file metadata for item id {current_item['id']}"
                    )
                return [current_item]

        return items

    def get_file_metadata(self, file_path: str) -> SharepointFile:
        """Fetch file metadata and content from Sharepoint.

        Args:
            file_path: Full Sharepoint path (e.g., 'folder/file.csv').

        Returns:
            SharepointFile with metadata and bytes content.

        Raises:
            ValueError: If required metadata is missing or path is invalid.
            requests.HTTPError: On HTTP errors during retrieval.
        """
        site_id = self._get_site_id()
        drive_id = self._get_drive_id()

        file_metadata_url = (
            f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/"
            f"{self.api_version}/sites/{site_id}/drives/{drive_id}/root:/{file_path}"
        )

        # Get metadata
        metadata_response = self._make_request(endpoint=file_metadata_url, method="GET")
        metadata = self._parse_json(
            metadata_response,
            f"fetching metadata for '{file_path}'",
        )

        file_name = metadata.get("name")
        time_created = metadata.get("createdDateTime", "")
        time_modified = metadata.get("lastModifiedDateTime", "")
        download_url = metadata.get("@microsoft.graph.downloadUrl")

        if not file_name or not download_url:
            raise ValueError(
                f"Missing required metadata for '{file_path}': "
                f"name={file_name!r}, "
                f"downloadUrl={'present' if download_url else 'absent'}"
            )

        # Download file content (bytes)
        content_response = self._make_request(endpoint=download_url, method="GET")
        content_response.raise_for_status()
        file_content = content_response.content

        if "/" not in file_path:
            raise ValueError(
                f"Invalid file path: '{file_path}'. Expected a folder/file structure."
            )
        folder = file_path.rsplit("/", 1)[0]

        return SharepointFile(
            file_name=file_name,
            time_created=time_created,
            time_modified=time_modified,
            content=file_content,
            _folder=folder,
        )

    def archive_sharepoint_file(
        self, sp_file: SharepointFile, to_path: str | None, *, move_enabled: bool = True
    ) -> None:
        """Rename (timestamp) and optionally move a Sharepoint file.

        Args:
            sp_file: File to archive.
            to_path: Destination folder (if moving).
            move_enabled: Whether to move after rename.

        Raises:
            SharePointAPIError: If the request fails.
        """
        # If already archived (renamed+moved before), don't repeat
        if getattr(sp_file, "_already_archived", False) and move_enabled and to_path:
            _logger.info(
                "Skipping archive: file already archived -> %s", sp_file.file_name
            )
            return

        try:
            if not getattr(sp_file, "skip_rename", False):
                new_file_name = self._rename_sharepoint_file(sp_file)
                sp_file.file_name = new_file_name
                sp_file.skip_rename = True

            if not move_enabled or not to_path:
                _logger.info(
                    """Archiving disabled or no target folder;
                     Renamed only and left in place: '%s'.""",
                    sp_file.file_path,
                )
                return

            self._move_file_in_sharepoint(sp_file, to_path)
            sp_file._already_archived = True
            _logger.info("Archived '%s' to '%s'.", sp_file.file_name, to_path)

        except requests.RequestException as e:
            _logger.error(
                "Request failed while archiving '%s': %s", sp_file.file_name, e
            )
            raise SharePointAPIError(f"Request failed: {e}")

    def _rename_sharepoint_file(self, sp_file: SharepointFile) -> str:
        """Prefix file name with a timestamp (skip if already renamed).

        Args:
            sp_file: File to rename.

        Returns:
            New file name.

        Raises:
            SharePointAPIError: If the rename request fails.
        """
        try:
            if getattr(sp_file, "skip_rename", False):
                _logger.info(
                    f"Skipping rename for already-prefixed file: {sp_file.file_name}"
                )
                return sp_file.file_name

            _logger.info(f"Renaming file at '{sp_file.file_path}'.")

            site_id = self._get_site_id()
            drive_id = self._get_drive_id()
            current_date_formatted = datetime.now().strftime("%Y%m%d%H%M%S")
            new_file_name = f"{current_date_formatted}_{sp_file.file_name}"

            url_get_file = (
                f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/{self.api_version}/"
                f"sites/{site_id}/drives/{drive_id}/root:/{sp_file.file_path}"
            )
            resp = self._make_request(endpoint=url_get_file, method="GET")
            file_info = self._parse_json(
                resp, f"fetching file info at '{sp_file.file_path}'"
            )
            file_id = file_info.get("id")
            if not file_id:
                raise ValueError(
                    f"File '{sp_file.file_name}' not found in '{sp_file.file_path}'."
                )

            url_rename_file = (
                f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/{self.api_version}/"
                f"sites/{site_id}/drives/{drive_id}/items/{file_id}"
            )
            rename_payload = {"name": new_file_name}
            rename_resp = self._make_request(
                endpoint=url_rename_file, method="PATCH", json_options=rename_payload
            )
            rename_resp.raise_for_status()

            _logger.info(f"File '{sp_file.file_name}' renamed to '{new_file_name}'.")
            sp_file.file_name = new_file_name
            return new_file_name

        except requests.RequestException as e:
            _logger.error(
                f"Request failed while renaming file '{sp_file.file_name}': {e}"
            )
            raise SharePointAPIError(f"Request failed: {e}")

    def _move_file_in_sharepoint(self, sp_file: SharepointFile, to_path: str) -> None:
        """Move a file to another folder in Sharepoint.

        Args:
            sp_file: File to move.
            to_path: Destination path.

        Raises:
            ValueError: If the file ID cannot be resolved.
            SharePointAPIError: If the move request fails.
        """
        try:
            _logger.info(
                f"Moving file '{sp_file.file_name}' from '{sp_file.file_path}' to "
                f"'{to_path}'."
            )

            site_id = self._get_site_id()
            drive_id = self._get_drive_id()

            if not self.check_if_endpoint_exists(
                folder_root_path=to_path, raise_error=False
            ):
                self._create_folder_in_sharepoint(to_path)
                # Create the folder if it doesn't exist; raise_error = false so it
                # doesn't throw error
                _logger.info(f"Created archive folder: {to_path}")

            url_get_file = (
                f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/{self.api_version}/"
                f"sites/{site_id}/drives/{drive_id}/root:/{sp_file.file_path}"
            )

            response = self._make_request(endpoint=url_get_file, method="GET")
            file_info = self._parse_json(
                response,
                f"getting file id for move '{sp_file.file_path}'",
            )

            file_id = file_info.get("id")

            if not file_id:
                raise ValueError(
                    f"File '{sp_file.file_name}' not found in '{sp_file.file_path}'."
                )

            url_move_file = (
                f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/{self.api_version}/"
                f"sites/{site_id}/drives/{drive_id}/items/{file_id}"
            )

            new_parent_reference = {
                "parentReference": {"path": f"/drive/root:/{to_path}"},
                "name": sp_file.file_name,
            }

            response = self._make_request(
                endpoint=url_move_file,
                method="PATCH",
                json_options=new_parent_reference,
            )
            response.raise_for_status()

            _logger.info(
                f"File '{sp_file.file_name}' successfully moved to '{to_path}'."
            )

        except requests.RequestException as e:
            _logger.error(
                f"Request failed while moving file '{sp_file.file_name}': {e}"
            )
            raise SharePointAPIError(f"Request failed: {e}")

    def _create_folder_in_sharepoint(self, folder_path: str) -> None:
        """Create the final folder in a Sharepoint path.

        Args:
            folder_path: Full folder path to create.

        Raises:
            SharePointAPIError: If folder creation fails.
        """
        try:
            site_id = self._get_site_id()
            drive_id = self._get_drive_id()

            parent_path, folder_name = (
                folder_path.rsplit("/", 1) if "/" in folder_path else ("", folder_path)
            )
            parent_path = parent_path.strip("/")  # Clean path just in case

            _logger.info(
                f"Creating folder '{folder_name}' inside '{parent_path or 'root'}'"
            )

            if parent_path:
                endpoint = (
                    f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/{self.api_version}/"
                    f"sites/{site_id}/drives/{drive_id}/root:/{parent_path}:/children"
                )
            else:
                endpoint = (
                    f"{ExecEnv.ENGINE_CONFIG.sharepoint_api_domain}/{self.api_version}/"
                    f"sites/{site_id}/drives/{drive_id}/root/children"
                )

            folder_metadata = {"name": folder_name, "folder": {}}

            response = self._make_request(
                endpoint=endpoint, method="POST", json_options=folder_metadata
            )
            response.raise_for_status()

            _logger.info(f"Folder '{folder_path}' created successfully.")

        except requests.RequestException as e:
            _logger.error(f"Failed to create folder '{folder_path}': {e}")
            raise SharePointAPIError(f"Error creating folder '{folder_path}': {e}")
