"""Package defining all the io custom exceptions."""


class IncrementalFilterInputNotFoundException(Exception):
    """Exception for when the input of an incremental filter is not found.

    This may occur when tables are being loaded in incremental way, taking the increment
    definition out of a specific table, but the table still does not exist, mainly
    because probably it was not loaded for the first time yet.
    """

    pass


class WrongIOFormatException(Exception):
    """Exception for when a user provides a wrong I/O format."""

    pass


class NotSupportedException(RuntimeError):
    """Exception for when a user provides a not supported operation."""

    pass


class InputNotFoundException(Exception):
    """Exception for when a user does not provide a mandatory input."""

    pass


class EndpointNotFoundException(Exception):
    """Exception for when the endpoint is not found by the Graph API."""

    pass


class LocalPathNotFoundException(Exception):
    """Exception for when a local path is not found."""

    pass


class WriteToLocalException(Exception):
    """Exception for when an error occurs when trying to write to the local path."""

    pass


class SharePointAPIError(Exception):
    """Custom exception class to handle errors SharePoint API requests."""

    pass
