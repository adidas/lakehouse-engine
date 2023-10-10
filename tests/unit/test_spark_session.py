"""Test if a new spark session returns the same object as current session."""

from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.logging_handler import LoggingHandler

LOGGER = LoggingHandler(__name__).get_logger()


def test_spark_session() -> None:
    """Test if a new spark session returns the same object as current session."""
    old_session = ExecEnv.SESSION.getActiveSession()
    ExecEnv.get_or_create()
    new_session = ExecEnv.SESSION.getActiveSession()

    assert old_session is new_session, (
        "Sessions pointing to different objects."
        f"{new_session} is different than {old_session}"
    )

    LOGGER.info(
        f"New session ({new_session}) is the same as previously "
        f"created session ({old_session})."
    )
