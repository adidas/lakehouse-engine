"""Package defining all the algorithm custom exceptions."""


class ReconciliationFailedException(Exception):
    """Exception for when the reconciliation process fails."""


class NoNewDataException(Exception):
    """Exception for when no new data is available."""


class SensorAlreadyExistsException(Exception):
    """Exception for when a sensor with same sensor id already exists."""


class RestoreTypeNotFoundException(Exception):
    """Exception for when the restore type is not found."""
