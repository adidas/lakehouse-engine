"""Module to define the behaviour to read from Sharepoint."""

import csv
import fnmatch
import time
from functools import reduce
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from lakehouse_engine.core.definitions import InputSpec, SharepointFile
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.exceptions import (
    InvalidSharepointPathException,
    NotSupportedException,
)
from lakehouse_engine.io.reader import Reader
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.sharepoint_utils import SharepointUtils

_LOGGER = LoggingHandler(__name__).get_logger()


class SharepointReader(Reader):
    """Reader implementation for Sharepoint files."""

    def __init__(self, input_spec: InputSpec):
        """Construct SharepointReader instance.

        Args:
            input_spec: InputSpec with Sharepoint parameters.
        """
        super().__init__(input_spec)
        self.opts = self._input_spec.sharepoint_opts
        self.sharepoint_utils = self._get_sharepoint_utils()

        if self.opts.file_name and self.opts.folder_relative_path:
            folder_name = Path(self.opts.folder_relative_path).name
            if "." in folder_name:
                raise InvalidSharepointPathException(
                    f"Invalid path setup: `folder_relative_path` "
                    f"('{self.opts.folder_relative_path}') appears to include a file, "
                    f"but `file_name` ('{self.opts.file_name}') was also provided. "
                    f"Provide either a folder+file_name, or a full file path not both."
                )
            _LOGGER.warning(
                "Using `file_name` with a folder path. "
                "This will read only one file. "
                "To read all files in the folder, set `file_name` to None."
            )
            self.file_path = f"{self.opts.folder_relative_path}/{self.opts.file_name}"
        elif (
            self.opts.folder_relative_path
            and "." in Path(self.opts.folder_relative_path).name
        ):
            self.file_path = self.opts.folder_relative_path  # full path with extension
        else:
            self.file_path = self.opts.folder_relative_path

        if self.opts.file_name and self.opts.file_pattern:
            _LOGGER.warning(
                "`file_name` is provided. `file_pattern` will be ignored and only the "
                "specified file will be read."
            )

        self.pattern = self.opts.file_pattern  # may be None

        # Compute archive base folder from final self.file_path
        archive_base_folder = None
        if self.file_path:
            p = Path(self.file_path)
            archive_base_folder = str(p.parent) if p.suffix else str(p)

        # Set archive folders
        self.success_folder = (
            f"{archive_base_folder}/{self.opts.archive_success_subfolder}"
            if (archive_base_folder and self.opts.archive_success_subfolder)
            else None
        )
        self.error_folder = (
            f"{archive_base_folder}/{self.opts.archive_error_subfolder}"
            if (archive_base_folder and self.opts.archive_error_subfolder)
            else None
        )

    def read(self) -> DataFrame:
        """Read a Sharepoint file using a format-specific reader.

        This method delegates to a reader resolved by file extension or the
        declared `file_type` (e.g., SharepointCsvReader or SharepointExcelReader).

        Returns:
            Spark DataFrame.

        Raises:
            InputNotFoundException: Missing required Sharepoint options.
            NotSupportedException: Streaming requested or reader unsupported.
        """
        self._input_spec.sharepoint_opts.validate_for_reader()

        if self._input_spec.read_type == "streaming":
            raise NotSupportedException(
                "Sharepoint reader doesn't support streaming input."
            )

        return SharepointReaderFactory.get_reader(self._input_spec).read()

    def _get_sharepoint_utils(self) -> SharepointUtils:
        """Build a SharepointUtils instance from input specs.

        Returns:
            SharepointUtils.
        """
        return SharepointUtils(
            client_id=self._input_spec.sharepoint_opts.client_id,
            tenant_id=self._input_spec.sharepoint_opts.tenant_id,
            local_path=self._input_spec.sharepoint_opts.local_path,
            api_version=self._input_spec.sharepoint_opts.api_version,
            site_name=self._input_spec.sharepoint_opts.site_name,
            drive_name=self._input_spec.sharepoint_opts.drive_name,
            file_name=self._input_spec.sharepoint_opts.file_name,
            folder_relative_path=self._input_spec.sharepoint_opts.folder_relative_path,
            chunk_size=self._input_spec.sharepoint_opts.chunk_size,
            local_options=self._input_spec.sharepoint_opts.local_options,
            secret=self._input_spec.sharepoint_opts.secret,
            conflict_behaviour=self._input_spec.sharepoint_opts.conflict_behaviour,
            file_pattern=self._input_spec.sharepoint_opts.file_pattern,
            file_type=self._input_spec.sharepoint_opts.file_type,
        )


class SharepointCsvReader(SharepointReader):
    """Read CSV files from Sharepoint and return Spark DataFrame.

    Supports reading a single file or combining multiple files in a folder.
    Ensures schema consistency and archives processed files.
    """

    def read(self, file_path: str = None, pattern: str = None) -> DataFrame:
        """Read CSV data from Sharepoint.

        Args:
            file_path: Full file or folder path (overrides options if provided).
            pattern: Optional substring filter for folder mode.

        Returns:
            Spark DataFrame.

        Raises:
            ValueError: Invalid/missing path or path not found.
        """
        file_path = file_path or self.file_path
        pattern = pattern or self.pattern

        if not file_path:
            raise ValueError(
                """`file_name` or `folder_relative_path` must be provided via
                sharepoint_opts."""
            )

        # Case 1: file_path includes a file (e.g., folder/file.csv or full path)
        if "." in Path(file_path).name:
            sp_file = self.sharepoint_utils.get_file_metadata(file_path)
            _LOGGER.info(f"Detected single-file read mode for '{file_path}'.")
            return self._load_and_archive_file(sp_file)

        # Case 2: it's a folder — use optional pattern
        if not self.sharepoint_utils.check_if_endpoint_exists(file_path):
            raise ValueError(f"Folder '{file_path}' does not exist in Sharepoint.")

        _LOGGER.info(
            f"Detected folder read mode for '{file_path}' "
            + (
                f"with pattern '{pattern}'."
                if pattern
                else "with no pattern (all files)."
            )
        )
        return self.read_csv_folder(file_path, pattern)

    def _load_and_archive_file(self, sp_file: SharepointFile) -> DataFrame:
        """Download a Sharepoint CSV, stage it locally, load with Spark, and archive it.

        Handles:
        - Writing the CSV to a temporary local path.
        - Reading it as a Spark DataFrame.
        - Archiving goes to the configured success/error subfolders when enabled
        (defaults: "done"/"error").

        Args:
            sp_file: File metadata and content.

        Returns:
            Spark DataFrame.

        Raises:
            ValueError: Empty content.
            Exception: Staging or read failure.
        """
        if self.file_path:
            base_folder = (
                str(Path(self.file_path).parent)
                if "." in Path(self.file_path).name
                else str(Path(self.file_path))
            )
        else:
            base_folder = sp_file._folder if getattr(sp_file, "_folder", None) else None

        success_subfolder = self.opts.archive_success_subfolder or "done"
        error_subfolder = self.opts.archive_error_subfolder or "error"

        success_folder = f"{base_folder}/{success_subfolder}" if base_folder else None
        error_folder = f"{base_folder}/{error_subfolder}" if base_folder else None

        archive_target = error_folder  # default to error unless full read succeeds

        try:
            # IMPORTANT: empty check inside try so finally always runs
            if not sp_file.content:
                raise ValueError(
                    f"File '{getattr(sp_file, 'file_path', None) or self.file_path}' "
                    "is empty or could not be downloaded."
                )

            with self.sharepoint_utils.staging_area() as tmp_dir_raw:
                tmp_dir: Path = Path(tmp_dir_raw)

                sp_file, df = self._load_csv_to_spark(sp_file, tmp_dir)
                archive_target = success_folder  # only mark success after full read

                _LOGGER.info(
                    f"Successfully read '{sp_file.file_path}' into Spark DataFrame."
                )
                df = df.cache()
                df.count()  # Force materialization
                return df

        except Exception as e:
            _LOGGER.error(f"Error processing '{sp_file.file_name}': {e}")
            raise

        finally:
            self.sharepoint_utils.archive_sharepoint_file(
                sp_file=sp_file,
                to_path=archive_target,
                move_enabled=self.opts.archive_enabled,
            )

    def _get_csv_files_in_folder(
        self, folder_path: str, pattern: str = None
    ) -> list[SharepointFile]:
        """List CSV files in a Sharepoint folder, optionally filtered by pattern.

        Args:
            folder_path: Sharepoint folder path.
            pattern: Optional glob/substring pattern.

        Returns:
            List of SharepointFile.
        """
        items = self.sharepoint_utils.list_items_in_path(folder_path)
        files = []

        if pattern:
            _LOGGER.info(
                f"""Filtering Sharepoint files in '{folder_path}' using glob-style
                pattern: '{pattern}'.
                Ensure your pattern uses wildcards (e.g., '*.csv', 'sales_*.csv').
                """
            )

        for item in items:
            file = SharepointFile(
                file_name=item["name"],
                time_created=item.get("createdDateTime", ""),
                time_modified=item.get("lastModifiedDateTime", ""),
                _folder=folder_path,
            )

            if not file.is_csv:
                continue

            if pattern:
                if not fnmatch.fnmatch(file.file_name, pattern):
                    continue

            files.append(file)

        return sorted(files, key=lambda f: f.file_name)

    def _load_csv_to_spark(
        self, sp_file: SharepointFile, tmp_dir: Path
    ) -> tuple[SharepointFile, DataFrame]:
        """Load a staged CSV into Spark and return file + DataFrame.

        Args:
            sp_file: Sharepoint file metadata.
            tmp_dir: Local staging directory.

        Returns:
            (SharepointFile, Spark DataFrame).

        Raises:
            ValueError: Empty or undownloadable file.
        """
        sp_file = self.sharepoint_utils.get_file_metadata(sp_file.file_path)

        local_file = self.sharepoint_utils.save_to_staging_area(sp_file)

        spark_options = self.resolve_spark_csv_options(sp_file.content)

        try:
            _LOGGER.info(f"Starting to read file: {sp_file.file_name}")
            start_time = time.time()
            df = (
                ExecEnv.SESSION.read.format("csv")
                .options(**spark_options)
                .load(str(local_file))
                .cache()
            )
            _LOGGER.info(
                f"""Finished reading file: {sp_file.file_name} in
                {round(time.time() - start_time, 2)} seconds"""
            )
            df.count()  # force materialization

            return sp_file, df

        except Exception as e:
            _LOGGER.error(
                f"Failed to read local copy of Sharepoint file: {local_file}",
                exc_info=True,
            )
            raise ValueError(
                f"Failed to read Sharepoint file: '{sp_file.file_path}'."
            ) from e

    def read_csv_folder(self, folder_path: str, pattern: str = None) -> DataFrame:
        """Read and combine CSVs from a Sharepoint folder.

        If a pattern is provided, only files whose names contain the pattern will be
        read.
        Only archives files to the configured success subfolder if the full read
        and union succeeds.
        Files causing schema mismatches or other read issues are moved to the
        configured error subfolder (when enabled).

        Args:
            folder_path: Sharepoint folder path.
            pattern: Optional substring filter for filenames.

        Returns:
            Combined Spark DataFrame.

        Raises:
            ValueError: No valid files or schema mismatches.
        """
        files = self._get_csv_files_in_folder(folder_path, pattern)
        if not files:
            raise ValueError(f"No CSV files found in folder: {folder_path}")

        valid_files, dfs = [], []
        base_schema = None

        with self.sharepoint_utils.staging_area() as tmp_dir_raw:
            tmp_dir: Path = Path(tmp_dir_raw)

            for file in files:
                try:
                    file_with_content, df = self._validate_and_read_file(
                        file, tmp_dir, base_schema
                    )
                    base_schema = base_schema or df.schema
                    dfs.append(df)
                    valid_files.append(file_with_content)
                except Exception as e:
                    self._handle_file_error(file, folder_path, e)
                    raise

        if not dfs:
            raise ValueError("No valid CSV files could be loaded from folder.")

        combined = reduce(lambda a, b: a.unionByName(b), dfs).cache()
        combined.count()

        for sp_file in valid_files:
            self.sharepoint_utils.archive_sharepoint_file(
                sp_file,
                to_path=(
                    f"{folder_path}/{self.opts.archive_success_subfolder}"
                    if self.opts.archive_success_subfolder
                    else None
                ),
                move_enabled=self.opts.archive_enabled,
            )

        return combined

    def _validate_and_read_file(
        self,
        file: SharepointFile,
        tmp_dir: Path,
        base_schema: Optional[StructType],
    ) -> tuple[SharepointFile, DataFrame]:
        """Validate schema and read CSV file into a Spark DataFrame.

        Args:
            file: Sharepoint file to read.
            tmp_dir: Temporary staging directory.
            base_schema: Schema to validate against.

        Returns:
            (validated SharepointFile, DataFrame).

        Raises:
            ValueError: Schema mismatch.
        """
        file_with_content, df = self._load_csv_to_spark(file, tmp_dir)

        if base_schema and df.schema != base_schema:
            _LOGGER.error(
                f"""Schema mismatch in '{file.file_name}'. Expected: {base_schema},
                Found: {df.schema}"""
            )
            self.sharepoint_utils.archive_sharepoint_file(
                sp_file=file_with_content,
                to_path=self.error_folder,
                move_enabled=self.opts.archive_enabled,
            )
            raise ValueError(f"Schema mismatch in '{file.file_name}'")

        return file_with_content, df

    def _handle_file_error(
        self,
        file: SharepointFile,
        folder_path: str,
        error: Exception,
    ) -> None:
        """Handle file read or processing errors by logging and archiving.

        Logs the error, prevents duplicate archiving, and moves the file
        to the error subfolder when enabled. Falls back gracefully if
        archiving fails.

        Args:
            file: Problematic SharepointFile.
            folder_path: Folder path for fallback archiving.
            error: Exception encountered.
        """
        _LOGGER.error(f"Error processing '{file.file_name}': {error}")
        if not getattr(file, "_already_archived", False):
            file.skip_rename = True
            try:
                self.sharepoint_utils.archive_sharepoint_file(
                    sp_file=file,
                    to_path=self.error_folder,
                    move_enabled=self.opts.archive_enabled,
                )
                file._already_archived = True
            except Exception as archive_error:
                _LOGGER.warning(f"Secondary archiving failed: {archive_error}")
        else:
            _LOGGER.info(
                f"Skipping second archive for '{file.file_name}' (already archived)"
            )

    def detect_delimiter(
        self,
        file_content: bytes,
        provided_delimiter: Optional[str] = None,
        expected_columns: Optional[list] = None,
    ) -> str:
        """Detect the appropriate delimiter for a CSV file.

        If a delimiter is explicitly provided by the user, it will be used directly
        (sniffing is bypassed).
        Otherwise, attempts to auto-detect the delimiter using csv.Sniffer based on the
        first line or expected columns.

        Args:
            file_content: Raw CSV bytes.
            provided_delimiter: Explicit delimiter to use.
            expected_columns: Optional expected header names.

        Returns:
            Final delimiter.

        Raises:
            ValueError: Unable to determine delimiter.
        """
        if provided_delimiter:
            _LOGGER.info(f"User-specified delimiter '{provided_delimiter}' selected.")
            return provided_delimiter

        try:
            text = file_content.decode("utf-8")
            dialect = csv.Sniffer().sniff(text, delimiters=";,|\t")
            detected_delimiter = dialect.delimiter

            _LOGGER.info(
                f"No user-specified delimiter. Auto-detected: '{detected_delimiter}'"
            )

            first_line = text.splitlines()[0].strip()
            actual_column_count = len(first_line.split(detected_delimiter))

            if expected_columns:
                expected_count = len(expected_columns)
                if actual_column_count != expected_count:
                    _LOGGER.warning(
                        f"""Detected delimiter '{detected_delimiter}' resulted in
                        {actual_column_count} columns,
                        but {expected_count} were expected. Consider specifying
                        the delimiter explicitly."""
                    )
            elif actual_column_count <= 1:
                _LOGGER.warning(
                    f"""Detected delimiter '{detected_delimiter}' resulted in only
                    {actual_column_count} column.
                     Consider specifying the delimiter explicitly in
                     'sharepoint_opts.local_options'."""
                )

            return detected_delimiter

        except Exception as e:
            _LOGGER.warning(
                f"Failed to auto-detect delimiter. Defaulting to comma. Reason: {e}"
            )
            return ","

    def resolve_spark_csv_options(self, file_content: bytes) -> dict:
        """Resolve Spark CSV read options by validating or detecting delimiter.

        Args:
            file_content: Raw file bytes.

        Returns:
            Dict of Spark CSV options (includes delimiter).
        """
        local_options = self._input_spec.sharepoint_opts.local_options or {}

        if "sep" in local_options:
            user_delimiter = local_options["sep"]
        elif "delimiter" in local_options:
            user_delimiter = local_options["delimiter"]
        else:
            user_delimiter = None

        expected_columns = local_options.get("expected_columns")

        final_delimiter = self.detect_delimiter(
            file_content=file_content,
            provided_delimiter=user_delimiter,
            expected_columns=expected_columns,
        )

        # Warn if expected column names do not match the header when using the selected
        # delimiter
        if expected_columns:
            try:
                header_line = file_content.decode("utf-8").splitlines()[0].strip()
                actual_columns = [c.strip() for c in header_line.split(final_delimiter)]

                expected_normalized = [str(c).strip().lower() for c in expected_columns]
                actual_normalized = [c.strip().lower() for c in actual_columns]

                if actual_normalized != expected_normalized:
                    _LOGGER.warning(
                        "Expected columns don't match CSV header using delimiter '%s'. "
                        "Expected: %s vs. Actual: %s. The read will proceed; "
                        "consider specifying the correct delimiter or "
                        "updating expected_columns.",
                        final_delimiter,
                        expected_columns,
                        actual_columns,
                    )
            except Exception as e:
                _LOGGER.warning(
                    "Failed to validate expected_columns against CSV header. "
                    "The read will proceed. Reason: %s",
                    e,
                )

        # Safety fallback if detector returned nothing for some reason
        final_delimiter = final_delimiter or ","

        spark_options = dict(local_options)
        spark_options["sep"] = final_delimiter
        # Remove "delimiter" to avoid ambiguity as spark uses "sep"
        spark_options.pop("delimiter", None)

        return spark_options


class SharepointExcelReader(SharepointReader):
    """Read Excel files from Sharepoint (not yet implemented)."""

    def read(self) -> DataFrame:
        """Read Excel files from Sharepoint.

        This method is not yet implemented and currently raises an error.
        Intended for future support of .xlsx file read from Sharepoint folders or files.

        Raises:
            NotImplementedError: Always, since Excel reading is not implemented.
        """
        raise NotImplementedError("Excel reading is not yet implemented.")


class SharepointReaderFactory:
    """Select the correct Sharepoint reader based on file type, file name, folder path.

    Default to using the file path from SharepointUtils instance.
    """

    @staticmethod
    def get_reader(input_spec: InputSpec) -> SharepointReader:
        """Select the appropriate Sharepoint reader based on input specification.

        Resolution order:
        1. Use file extension from `file_name` if provided.
        2. If `folder_relative_path` includes a file with extension, use that.
        3. If neither applies, use `file_type`.

        Args:
            input_spec: InputSpec with Sharepoint options.

        Returns:
            Reader instance for the resolved file type.

        Raises:
            ValueError: If file format is unsupported or cannot be determined.
        """
        opts = input_spec.sharepoint_opts

        # 1. If reading a specific file, use file_name
        if opts.file_name:
            ext = Path(opts.file_name).suffix.lower()

        # 2. If folder_relative_path includes extension, treat it as full path
        elif opts.folder_relative_path and "." in Path(opts.folder_relative_path).name:
            ext = Path(opts.folder_relative_path).suffix.lower()

        # 3. Otherwise, rely on file_type
        elif opts.file_type:
            ext = f".{opts.file_type.lower()}"

        else:
            raise ValueError(
                """Cannot determine file format. Please provide `file_name`,
                 a full file path in `folder_relative_path`, or explicitly set
                 `file_type`."""
            )

        readers = {
            ".csv": SharepointCsvReader,
            ".xlsx": SharepointExcelReader,
        }
        try:
            _LOGGER.info(f"Detected {ext} read mode.")
            return readers[ext](input_spec)
        except KeyError:
            raise ValueError(f"Unsupported file format: {ext}")
