"""Unit tests for notification factory module."""

import pytest

from lakehouse_engine.core.definitions import TerminatorSpec
from lakehouse_engine.terminators.notifier_factory import NotifierFactory
from lakehouse_engine.utils.logging_handler import LoggingHandler

LOGGER = LoggingHandler(__name__).get_logger()


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "name": "Error: wrong type of notifier",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "localhost",
                    "port": "1025",
                    "type": "snailmail",
                    "template": "failure_notification_email",
                    "from": "test-email@email.com",
                    "to": ["test-email1@email.com", "test-email2@email.com"],
                    "args": {
                        "exception": "test-exception",
                    },
                },
            ),
            "expected": "The requested notification format snailmail is not supported.",
        },
        {
            "name": "Creation of email",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "localhost",
                    "port": "1025",
                    "type": "email",
                    "template": "failure_notification_email",
                    "from": "test-email@email.com",
                    "to": ["test-email1@email.com", "test-email2@email.com"],
                    "args": {
                        "exception": "test-exception",
                    },
                },
            ),
            "expected": "email",
        },
    ],
)
def test_notification_factory(scenario: dict) -> None:
    """Testing notification factory.

    Args:
        scenario: scenario to test.
    """
    if "Error: " in scenario["name"]:
        with pytest.raises(NotImplementedError) as e:
            notifier = NotifierFactory.get_notifier(scenario["spec"])

        assert scenario["expected"] == str(e.value)
    else:
        notifier = NotifierFactory.get_notifier(scenario["spec"])

        assert notifier.type == scenario["expected"]
