"""Defines terminator behaviour."""
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.logging_handler import LoggingHandler


class SparkTerminator(object):
    """Spark Terminator class."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def terminate_spark(cls) -> None:
        """Terminate spark session."""
        cls._logger.info("Terminating spark session...")
        ExecEnv.SESSION.stop()
