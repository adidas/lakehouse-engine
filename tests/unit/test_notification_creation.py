"""Unit tests for notification creation functions."""

from typing import Any

import pytest

from lakehouse_engine.core.definitions import TerminatorSpec
from lakehouse_engine.terminators.notifier import Notifier
from lakehouse_engine.terminators.notifier_factory import NotifierFactory
from lakehouse_engine.utils.logging_handler import LoggingHandler

LOGGER = LoggingHandler(__name__).get_logger()


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "name": "Email notification creation using a template.",
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
            "expected": """
            Job local in workspace local has
            failed with the exception: test-exception""",
        },
        {
            "name": "Email notification creation using a free form arguments.",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "localhost",
                    "port": "1025",
                    "type": "email",
                    "from": "test-email@email.com",
                    "to": ["test-email1@email.com", "test-email2@email.com"],
                    "mimetype": "plain",
                    "subject": "Test Email",
                    "message": "Test message for the email with templating: {{ msg }}.",
                    "args": {"msg": "anything can go here"},
                },
            ),
            "expected": "Test message for the email with templating: "
            "anything can go here.",
        },
        {
            "name": "Error: missing template",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "localhost",
                    "port": "1025",
                    "type": "email",
                    "template": "missing template",
                    "from": "test-email@email.com",
                    "to": ["test-email1@email.com", "test-email2@email.com"],
                    "args": {
                        "exception": "test-exception",
                    },
                },
            ),
            "expected": "Template missing template does not exist",
        },
        {
            "name": "Error: Malformed acon",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "localhost",
                    "port": "1025",
                    "type": "email",
                    "from": "test-email@email.com",
                    "to": ["test-email1@email.com", "test-email2@email.com"],
                    "args": {
                        "exception": "test-exception",
                    },
                },
            ),
            "expected": "Malformed Notification Definition",
        },
    ],
)
def test_notification_creation(scenario: dict) -> None:
    """Testing notification creation.

    Args:
        scenario: scenario to test.
    """
    notifier = NotifierFactory.get_notifier(scenario["spec"])

    if "Error: " in scenario["name"]:
        with pytest.raises(ValueError) as e:
            notifier.create_notification()
        assert str(e.value) == scenario["expected"]
    else:
        notifier.create_notification()
        assert notifier.notification["message"] == scenario["expected"]


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "name": "Correct Parameters",
            "given_args": {"arg1": "test", "arg2": "test", "arg3": "test"},
            "template_args": ["arg1", "arg2", "arg3"],
            "expected": "",
        },
        {
            "name": "Error: incorrect parameters",
            "given_args": {
                "arg1": "test",
                "arg2": "test",
            },
            "template_args": ["arg1", "arg2", "arg3"],
            "expected": "The following template args have not been set: arg3",
        },
        {
            "name": "Extra parameters",
            "given_args": {
                "arg1": "test",
                "arg2": "test",
                "arg3": "test",
                "arg4": "test",
                "arg5": "test",
                "arg6": "test",
            },
            "template_args": ["arg1", "arg2", "arg3"],
            "expected": "Extra parameters sent to template: arg4, arg5, arg6",
        },
    ],
)
def test_argument_verification(scenario: dict, caplog: Any) -> None:
    """Testing notification template argument validation.

    Args:
        scenario: scenario to test.
        caplog: captured log.
    """
    if "Error" in scenario["name"]:
        with pytest.raises(ValueError) as e:
            Notifier._check_args_are_correct(
                scenario["given_args"], scenario["template_args"]
            )
        assert str(e.value) == scenario["expected"]
    else:
        Notifier._check_args_are_correct(
            scenario["given_args"], scenario["template_args"]
        )
        if scenario["expected"]:
            assert scenario["expected"] in caplog.text
