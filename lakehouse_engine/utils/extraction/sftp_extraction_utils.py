"""Utilities module for SFTP extraction processes."""
import stat
from base64 import decodebytes
from datetime import datetime
from enum import Enum
from logging import Logger
from stat import S_ISREG
from typing import Any, List, Set, Tuple, Union

import paramiko as p
from paramiko import Ed25519Key, PKey, RSAKey, Transport
from paramiko.sftp_client import SFTPAttributes, SFTPClient  # type: ignore

from lakehouse_engine.transformers.exceptions import WrongArgumentsException
from lakehouse_engine.utils.logging_handler import LoggingHandler


class SFTPInputFormat(Enum):
    """Formats of algorithm input."""

    CSV = "csv"
    FWF = "fwf"
    JSON = "json"
    XML = "xml"


class SFTPExtractionFilter(Enum):
    """Standardize the types of filters we can have from a SFTP source."""

    file_name_contains = "file_name_contains"
    LATEST_FILE = "latest_file"
    EARLIEST_FILE = "earliest_file"
    GREATER_THAN = "date_time_gt"
    LOWER_THAN = "date_time_lt"


class SFTPExtractionUtils(object):
    """Utils for managing data extraction from particularly relevant SFTP sources."""

    _logger: Logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def get_files_list(
        cls, sftp: SFTPClient, remote_path: str, options_args: dict
    ) -> Set[str]:
        """Get a list of files to be extracted from SFTP.

        The arguments (options_args) to list files are:
        date_time_gt(str):
            Filter the files greater than the string datetime
            formatted as "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS".
        date_time_lt(str):
            Filter the files lower than the string datetime
            formatted as "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS".
        earliest_file(bool):
            Filter the earliest dated file in the directory.
        file_name_contains(str):
            Filter files when match the pattern.
        latest_file(bool):
            Filter the most recent dated file in the directory.
        sub_dir(bool):
            When true, the engine will search files into subdirectories
            of the remote_path.
            It will consider one level below the remote_path.
            When sub_dir is used with latest_file/earliest_file argument,
            the engine will retrieve the latest_file/earliest_file
            for each subdirectory.

        Args:
            sftp: the SFTP client object.
            remote_path: path of files to be filtered.
            options_args: options from the acon.

        Returns:
            A list containing the file names to be passed to Spark.
        """
        all_items, folder_path = cls._get_folder_items(remote_path, sftp, options_args)

        filtered_files: Set[str] = set()

        try:
            for item, folder in zip(all_items, folder_path):
                file_contains = cls._file_has_pattern(item, options_args)
                file_in_interval = cls._file_in_date_interval(item, options_args)
                if file_contains and file_in_interval:
                    filtered_files.add(folder + item.filename)

            if (
                SFTPExtractionFilter.EARLIEST_FILE.value in options_args.keys()
                or SFTPExtractionFilter.LATEST_FILE.value in options_args.keys()
            ):
                filtered_files = cls._get_earliest_latest_file(
                    sftp, options_args, filtered_files, folder_path
                )

        except Exception as e:
            cls._logger.error(f"SFTP list_files EXCEPTION: - {e}")
        return filtered_files

    @classmethod
    def get_sftp_client(
        cls,
        options_args: dict,
    ) -> Tuple[SFTPClient, Transport]:
        """Get the SFTP client.

        The SFTP client is used to open an SFTP session across an open
        SSH Transport and perform remote file operations.

        Args:
            options_args: dictionary containing SFTP connection parameters.
            The Paramiko arguments expected to connect are:
                "hostname": the server to connect to.
                "port": the server port to connect to.
                "username": the username to authenticate as.
                "password": used for password authentication.
                "pkey": optional - an optional public key to use for authentication.
                "passphrase" – optional - options used for decrypting private keys.
                "key_filename" – optional - the filename, or list of filenames,
                    of optional private key(s) and/or certs to try for authentication.
                "timeout" – an optional timeout (in seconds) for the TCP connect.
                "allow_agent" – optional - set to False to disable
                    connecting to the SSH agent.
                "look_for_keys" – optional - set to False to disable searching
                    for discoverable private key files in ~/.ssh/.
                "compress" – optional - set to True to turn on compression.
                "sock" - optional - an open socket or socket-like object
                    to use for communication to the target host.
                "gss_auth" – optional - True if you want to use GSS-API authentication.
                "gss_kex" – optional - Perform GSS-API Key Exchange and
                    user authentication.
                "gss_deleg_creds" – optional - Delegate GSS-API client
                    credentials or not.
                "gss_host" – optional - The targets name in the kerberos database.
                "gss_trust_dns" – optional - Indicates whether or
                    not the DNS is trusted to securely canonicalize the name of the
                    host being connected to (default True).
                "banner_timeout" – an optional timeout (in seconds)
                    to wait for the SSH banner to be presented.
                "auth_timeout" – an optional timeout (in seconds)
                    to wait for an authentication response.
                "disabled_algorithms" – an optional dict passed directly to Transport
                    and its keyword argument of the same name.
                "transport_factory" – an optional callable which is handed a subset of
                    the constructor arguments (primarily those related to the socket,
                    GSS functionality, and algorithm selection) and generates a
                    Transport instance to be used by this client.
                    Defaults to Transport.__init__.

            The parameter to specify the private key is expected to be in RSA format.
            Attempting a connection with a blank host key is not allowed
            unless the argument "add_auto_policy" is explicitly set to True.

        Returns:
            sftp -> a new SFTPClient session object.
            transport -> the Transport for this connection.
        """
        ssh_client = p.SSHClient()
        try:
            if not options_args.get("pkey") and not options_args.get("add_auto_policy"):
                raise WrongArgumentsException(
                    "Get SFTP Client: No host key (pkey) was provided and the "
                    + "add_auto_policy property is false."
                )

            if options_args.get("pkey") and not options_args.get("key_type"):
                raise WrongArgumentsException(
                    "Get SFTP Client: The key_type must be provided when "
                    + "the host key (pkey) is provided."
                )

            if options_args.get("pkey", None) and options_args.get("key_type", None):
                key = cls._get_host_keys(
                    options_args.get("pkey", None), options_args.get("key_type", None)
                )
                ssh_client.get_host_keys().add(
                    hostname=f"[{options_args.get('hostname')}]:"
                    + f"{options_args.get('port')}",
                    keytype="ssh-rsa",
                    key=key,
                )
            elif options_args.get("add_auto_policy", None):
                ssh_client.load_system_host_keys()
                ssh_client.set_missing_host_key_policy(p.WarningPolicy())
            else:
                ssh_client.load_system_host_keys()
                ssh_client.set_missing_host_key_policy(p.RejectPolicy())

            ssh_client.connect(
                hostname=options_args.get("hostname"),
                port=options_args.get("port", 22),
                username=options_args.get("username", None),
                password=options_args.get("password", None),
                key_filename=options_args.get("key_filename", None),
                timeout=options_args.get("timeout", None),
                allow_agent=options_args.get("allow_agent", True),
                look_for_keys=options_args.get("look_for_keys", True),
                compress=options_args.get("compress", False),
                sock=options_args.get("sock", None),
                gss_auth=options_args.get("gss_auth", False),
                gss_kex=options_args.get("gss_kex", False),
                gss_deleg_creds=options_args.get("gss_deleg_creds", False),
                gss_host=options_args.get("gss_host", False),
                banner_timeout=options_args.get("banner_timeout", None),
                auth_timeout=options_args.get("auth_timeout", None),
                gss_trust_dns=options_args.get("gss_trust_dns", None),
                passphrase=options_args.get("passphrase", None),
                disabled_algorithms=options_args.get("disabled_algorithms", None),
                transport_factory=options_args.get("transport_factory", None),
            )

            sftp = ssh_client.open_sftp()
            transport = ssh_client.get_transport()
        except ConnectionError as e:
            cls._logger.error(e)
            raise
        return sftp, transport

    @classmethod
    def validate_format(cls, files_format: str) -> str:
        """Validate the file extension based on the format definitions.

        Args:
            files_format: a string containing the file extension.

        Returns:
            The string validated and formatted.
        """
        formats_allowed = [
            SFTPInputFormat.CSV.value,
            SFTPInputFormat.FWF.value,
            SFTPInputFormat.JSON.value,
            SFTPInputFormat.XML.value,
        ]

        if files_format not in formats_allowed:
            raise WrongArgumentsException(
                f"The formats allowed for SFTP are {formats_allowed}."
            )

        return files_format

    @classmethod
    def validate_location(cls, location: str) -> str:
        """Validate the location. Add "/" in the case it does not exist.

        Args:
            location: file path.

        Returns:
            The location validated.
        """
        return location if location.rfind("/") == len(location) - 1 else location + "/"

    @classmethod
    def _file_has_pattern(cls, item: SFTPAttributes, options_args: dict) -> bool:
        """Check if a file follows the pattern used for filtering.

        Args:
            item: item available in SFTP directory.
            options_args: options from the acon.

        Returns:
            A boolean telling whether the file contains a pattern or not.
        """
        file_to_consider = True

        if SFTPExtractionFilter.file_name_contains.value in options_args.keys():
            if not (
                options_args.get(SFTPExtractionFilter.file_name_contains.value)
                in item.filename
                and (S_ISREG(item.st_mode) or cls._is_compressed(item.filename))
            ):
                file_to_consider = False

        return file_to_consider

    @classmethod
    def _file_in_date_interval(
        cls,
        item: SFTPAttributes,
        options_args: dict,
    ) -> bool:
        """Check if the file is in the expected date interval.

        The logic is applied based on the arguments greater_than and lower_than.
        i.e:
            if greater_than and lower_than have values,
            then it performs a between.
            if only lower_than has values,
            then only values lower than the input value will be retrieved.
            if only greater_than has values,
            then only values greater than the input value will be retrieved.

        Args:
            item: item available in SFTP directory.
            options_args: options from the acon.

        Returns:
            A boolean telling whether the file is in the expected date interval or not.
        """
        file_to_consider = True

        if (
            SFTPExtractionFilter.LOWER_THAN.value in options_args.keys()
            or SFTPExtractionFilter.GREATER_THAN.value in options_args.keys()
            and (S_ISREG(item.st_mode) or cls._is_compressed(item.filename))
        ):
            lower_than = options_args.get(
                SFTPExtractionFilter.LOWER_THAN.value, "9999-12-31"
            )
            greater_than = options_args.get(
                SFTPExtractionFilter.GREATER_THAN.value, "1900-01-01"
            )

            file_date = datetime.fromtimestamp(item.st_mtime)

            if not (
                (
                    lower_than == greater_than
                    and cls._validate_date(greater_than)
                    <= file_date
                    <= cls._validate_date(lower_than)
                )
                or (
                    cls._validate_date(greater_than)
                    < file_date
                    < cls._validate_date(lower_than)
                )
            ):
                file_to_consider = False

        return file_to_consider

    @classmethod
    def _get_earliest_latest_file(
        cls,
        sftp: SFTPClient,
        options_args: dict,
        list_filter_files: Set[str],
        folder_path: List,
    ) -> Set[str]:
        """Get the earliest or latest file of a directory.

        Args:
            sftp: the SFTP client object.
            options_args: options from the acon.
            list_filter_files: set of file names to filter from.
            folder_path: the location of files.

        Returns:
            A set containing the earliest/latest file name.
        """
        list_earl_lat_files: Set[str] = set()

        for folder in folder_path:
            file_date = 0
            file_name = ""
            all_items, file_path = cls._get_folder_items(
                f"{folder}", sftp, options_args
            )
            for item in all_items:
                if (
                    folder + item.filename in list_filter_files
                    and (S_ISREG(item.st_mode) or cls._is_compressed(item.filename))
                    and (
                        options_args.get("earliest_file")
                        and (file_date == 0 or item.st_mtime < file_date)
                    )
                    or (
                        options_args.get("latest_file")
                        and (file_date == 0 or item.st_mtime > file_date)
                    )
                ):
                    file_date = item.st_mtime
                    file_name = folder + item.filename
            list_earl_lat_files.add(file_name)

        return list_earl_lat_files

    @classmethod
    def _get_folder_items(
        cls, remote_path: str, sftp: SFTPClient, options_args: dict
    ) -> Tuple:
        """Get the files and the directory to be processed.

        Args:
            remote_path: root folder path.
            sftp: a SFTPClient session object.
            options_args: options from the acon.

        Returns:
            A tuple with a list of items (file object) and a list of directories.
        """
        sub_dir = options_args.get("sub_dir", False)
        all_items: List[SFTPAttributes] = sftp.listdir_attr(remote_path)
        items: List[SFTPAttributes] = []
        folders: List = []

        for item in all_items:
            is_dir = stat.S_ISDIR(item.st_mode)
            if is_dir and sub_dir and not item.filename.endswith((".gz", ".zip")):
                dirs = sftp.listdir_attr(f"{remote_path}{item.filename}")
                for file in dirs:
                    items.append(file)
                    folders.append(f"{remote_path}{item.filename}/")
            else:
                items.append(item)
                folders.append(remote_path)

        return items, folders

    @classmethod
    def _get_host_keys(cls, pkey: str, key_type: str) -> PKey:
        """Get the pkey that will be added to the server.

        Args:
            pkey: a string with a host key value.
            key_type: the type of key (rsa or ed25519).

        Returns:
            A PKey that will be used to authenticate the connection.
        """
        key: Union[RSAKey, Ed25519Key] = None
        if pkey and key_type.lower() == "rsa":
            b_pkey = bytes(pkey, "UTF-8")
            key = p.RSAKey(data=decodebytes(b_pkey))
        elif pkey and key_type.lower() == "ed25519":
            b_pkey = bytes(pkey, "UTF-8")
            key = p.Ed25519Key(data=decodebytes(b_pkey))

        return key

    @classmethod
    def _is_compressed(cls, filename: str) -> Any:
        """Validate if it is a compressed file.

        Args:
            filename: name of the file to be validated.

        Returns:
            A boolean with the result.
        """
        return filename.endswith((".gz", ".zip"))

    @classmethod
    def _validate_date(cls, date_text: str) -> datetime:
        """Validate the input date format.

        Args:
            date_text: a string with the date or datetime value.
            The expected formats are:
                YYYY-MM-DD and YYYY-MM-DD HH:MM:SS

        Returns:
            The datetime validated and formatted.
        """
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                if date_text is not None:
                    return datetime.strptime(date_text, fmt)
            except ValueError:
                pass
        raise ValueError(
            "Incorrect data format, should be YYYY-MM-DD or YYYY-MM-DD HH:MM:SS."
        )
