"""Module to define behaviour to read from SFTP."""

import gzip
from datetime import datetime
from io import TextIOWrapper
from logging import Logger
from typing import List
from zipfile import ZipFile

import pandas as pd
from pandas import DataFrame as PandasDataFrame
from pandas.errors import EmptyDataError
from paramiko.sftp_file import SFTPFile
from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import InputSpec, ReadType
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.reader import Reader
from lakehouse_engine.utils.extraction.sftp_extraction_utils import SFTPExtractionUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler


class SFTPReader(Reader):
    """Class to read from SFTP."""

    _logger: Logger = LoggingHandler(__name__).get_logger()

    def __init__(self, input_spec: InputSpec):
        """Construct SFTPReader instances.

        Args:
            input_spec: input specification.
        """
        super().__init__(input_spec)

    def read(self) -> DataFrame:
        """Read SFTP data.

        Returns:
            A dataframe containing the data from SFTP.
        """
        if self._input_spec.read_type == ReadType.BATCH.value:
            options_args = self._input_spec.options if self._input_spec.options else {}

            sftp_files_format = SFTPExtractionUtils.validate_format(
                self._input_spec.sftp_files_format.lower()
            )

            location = SFTPExtractionUtils.validate_location(self._input_spec.location)

            sftp, transport = SFTPExtractionUtils.get_sftp_client(options_args)

            files_list = SFTPExtractionUtils.get_files_list(
                sftp, location, options_args
            )

            dfs: List[PandasDataFrame] = []
            try:
                for filename in files_list:
                    with sftp.open(filename, "r") as sftp_file:
                        try:
                            pdf = self._read_files(
                                filename,
                                sftp_file,
                                options_args.get("args", {}),
                                sftp_files_format,
                            )
                            if options_args.get("file_metadata", None):
                                pdf["filename"] = filename
                                pdf["modification_time"] = datetime.fromtimestamp(
                                    sftp.stat(filename).st_mtime
                                )
                            self._append_files(pdf, dfs)
                        except EmptyDataError:
                            self._logger.info(f"{filename} - Empty or malformed file.")
                if dfs:
                    df = ExecEnv.SESSION.createDataFrame(pd.concat(dfs))
                else:
                    raise ValueError(
                        "No files were found with the specified parameters."
                    )
            finally:
                sftp.close()
                transport.close()
        else:
            raise NotImplementedError(
                "The requested read type supports only BATCH mode."
            )
        return df

    @classmethod
    def _append_files(cls, pdf: PandasDataFrame, dfs: List) -> List:
        """Append to the list dataframes with data.

        Args:
            pdf: a Pandas dataframe containing data from files.
            dfs: a list of Pandas dataframes.

        Returns:
            A list of not empty Pandas dataframes.
        """
        if not pdf.empty:
            dfs.append(pdf)
        return dfs

    @classmethod
    def _read_files(
        cls, filename: str, sftp_file: SFTPFile, option_args: dict, files_format: str
    ) -> PandasDataFrame:
        """Open and decompress files to be extracted from SFTP.

        For zip files, to avoid data type inferred issues
        during the iteration, all data will be read as string.
        Also, empty dataframes will NOT be considered to be processed.
        For the not considered ones, the file names will be logged.

        Args:
            filename: the filename to be read.
            sftp_file: SFTPFile object representing the open file.
            option_args: options from the acon.
            files_format: a string containing the file extension.

        Returns:
            A pandas dataframe with data from the file.
        """
        reader = getattr(pd, f"read_{files_format}")

        if filename.endswith(".gz"):
            with gzip.GzipFile(fileobj=sftp_file, mode="rb") as gz_file:
                pdf = reader(
                    TextIOWrapper(gz_file),  # type: ignore
                    **option_args,
                )
        elif filename.endswith(".zip"):
            with ZipFile(sftp_file, "r") as zf:  # type: ignore
                dfs = [
                    reader(TextIOWrapper(zf.open(f)), **option_args).fillna("")
                    for f in zf.namelist()
                ]
                if not pd.concat(dfs, ignore_index=True).empty:
                    pdf = pd.concat(dfs, ignore_index=True).astype(str)
                else:
                    pdf = pd.DataFrame()
                    cls._logger.info(f"{filename} - Empty or malformed file.")
        else:
            pdf = reader(
                sftp_file,
                **option_args,
            )
        return pdf
