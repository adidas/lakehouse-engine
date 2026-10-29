"""Package defining all the Notifier custom exceptions."""


class NotifierNotFoundException(Exception):
    """Exception for when the notifier is not found."""


class NotifierConfigException(Exception):
    """Exception for when the notifier configuration is invalid."""


class NotifierTemplateNotFoundException(Exception):
    """Exception for when the notifier is not found."""


class NotifierTemplateConfigException(Exception):
    """Exception for when the notifier config is incorrect."""
