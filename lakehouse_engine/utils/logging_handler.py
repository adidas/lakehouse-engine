"""Module to configure project logging."""
import logging
import re

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
SENSITIVE_KEYS_REG = [
    {  # Enclosed in ''.
        # Stops replacing when it finds comma and space, space or end of line.
        "regex": r"'(kafka\.ssl\.keystore\.password|kafka\.ssl\.truststore\.password"
        r"|password|secret|credential|credentials|pass|key)'[ ]*:"
        r"[ ]*'.*?(, | |}|$)",
        "replace": "'masked_cred': '******', ",
    },
    {  # Enclosed in "".
        # Stops replacing when it finds comma and space, space or end of line.
        "regex": r'"(kafka\.ssl\.keystore\.password|kafka\.ssl\.truststore\.password'
        r'|password|secret|credential|credentials|pass|key)"[ ]*:'
        r'[ ]*".*?(, | |}|$)',
        "replace": '"masked_cred": "******", ',
    },
    {  # Not enclosed in '' or "".
        # Stops replacing when it finds comma and space, space or end of line.
        "regex": r"(kafka\.ssl\.keystore\.password|kafka\.ssl\.truststore\.password"
        r"|password|secret|credential|credentials|pass|key)[ ]*:"
        r"[ ]*.*?(, | |}|$)",
        "replace": "masked_cred: ******, ",
    },
]


class FilterSensitiveData(logging.Filter):
    """Logging filter to hide sensitive data from being shown in the logs."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        """Hide sensitive information from being shown in the logs.

        Based on the configured regex and replace strings, the content of the log
        records is replaced and then all the records are allowed to be logged
        (return True).

        Args:
            record: the LogRecord event being logged.

        Returns:
            The transformed record to be logged.
        """
        for key_reg in SENSITIVE_KEYS_REG:
            record.msg = re.sub(key_reg["regex"], key_reg["replace"], str(record.msg))
        return True


class LoggingHandler(object):
    """Handle the logging of the lakehouse engine project."""

    def __init__(self, class_name: str):
        """Construct a LoggingHandler instance.

        Args:
            class_name: name of the class to be indicated in the logs.
        """
        self._logger: logging.Logger = logging.getLogger(class_name)
        self._logger.setLevel(logging.DEBUG)
        self._logger.addFilter(FilterSensitiveData())
        lsh = logging.StreamHandler()
        lsh.setLevel(logging.DEBUG)
        lsh.setFormatter(FORMATTER)
        if not self._logger.hasHandlers():
            # avoid keep adding handlers and therefore duplicate messages
            self._logger.addHandler(lsh)

    def get_logger(self) -> logging.Logger:
        """Get the _logger instance variable.

        :return logging.Logger: the logger object.
        """
        return self._logger
