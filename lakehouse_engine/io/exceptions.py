"""Package defining all the io custom exceptions."""


class IncrementalFilterInputNotFoundException(Exception):
    """Exception for when the input of an incremental filter is not found.

    This may occur when tables are being loaded in incremental way, taking the increment
    definition out of a specific table, but the table still does not exist, mainly
    because probably it was not loaded for the first time yet.
    """


class WrongIOFormatException(Exception):
    """Exception for when a user provides a wrong I/O format."""


class NotSupportedException(RuntimeError):
    """Exception for when a user provides a not supported operation."""


class InputNotFoundException(Exception):
    """Exception for when a user does not provide a mandatory input."""


class EndpointNotFoundException(Exception):
    """Exception for when the endpoint is not found by the Graph API."""


class LocalPathNotFoundException(Exception):
    """Exception for when a local path is not found."""


class WriteToLocalException(Exception):
    """Exception for when an error occurs when trying to write to the local path."""


class SharePointAPIError(Exception):
    """Custom exception class to handle errors Sharepoint API requests."""


class InvalidSharepointPathException(Exception):
    """Raised when folder path conflicts with file name.

    Happens if both `folder_relative_path` and `file_name` are set, but the folder path
    looks like a file path (last segment has a dot).
    """
