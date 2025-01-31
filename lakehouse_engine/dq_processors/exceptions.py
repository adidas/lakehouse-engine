"""Package defining all the DQ custom exceptions."""


class DQValidationsFailedException(Exception):
    """Exception for when the data quality validations fail."""

    pass


class DQCheckpointsResultsException(Exception):
    """Exception for when the checkpoint results parsing fail."""

    pass


class DQSpecMalformedException(Exception):
    """Exception for when the DQSpec is malformed."""

    pass


class DQDuplicateRuleIdException(Exception):
    """Exception for when a duplicated rule id is found."""

    pass
