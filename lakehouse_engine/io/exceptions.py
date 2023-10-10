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
