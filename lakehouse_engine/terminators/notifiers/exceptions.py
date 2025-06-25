"""Package defining all the Notifier custom exceptions."""


class NotifierNotFoundException(Exception):
    """Exception for when the notifier is not found."""

    pass


class NotifierConfigException(Exception):
    """Exception for when the notifier configuration is invalid."""

    pass


class NotifierTemplateNotFoundException(Exception):
    """Exception for when the notifier is not found."""

    pass


class NotifierTemplateConfigException(Exception):
    """Exception for when the notifier config is incorrect."""

    pass
