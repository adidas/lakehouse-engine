"""Module for all the transformers exceptions."""


class WrongArgumentsException(Exception):
    """Exception for when a user provides wrong arguments to a transformer."""

    pass


class UnsupportedStreamingTransformerException(Exception):
    """Exception for when a user requests a transformer not supported in streaming."""

    pass
