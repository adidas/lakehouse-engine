"""Unit tests for notification creation functions."""

import pytest

from lakehouse_engine.core.definitions import TerminatorSpec
from lakehouse_engine.terminators.notifier_factory import NotifierFactory
from lakehouse_engine.terminators.notifiers.email_notifier import EmailNotifier
from lakehouse_engine.terminators.notifiers.exceptions import (
    NotifierConfigException,
    NotifierTemplateConfigException,
    NotifierTemplateNotFoundException,
)
from lakehouse_engine.utils.logging_handler import LoggingHandler
from tests.conftest import FEATURE_RESOURCES

LOGGER = LoggingHandler(__name__).get_logger()
TEST_ATTACHEMENTS_PATH = FEATURE_RESOURCES + "/notification/"


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
                    "exception": "test-exception",
                },
            ),
            "expected": """
            Job local in workspace local has
            failed with the exception: test-exception""",
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
                    "exception": "test-exception",
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
                    "exception": "test-exception",
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
        with pytest.raises(
            (
                NotifierTemplateNotFoundException,
                NotifierConfigException,
                NotifierTemplateConfigException,
            )
        ) as e:
            notifier.create_notification()
        assert str(e.value) == scenario["expected"]
    else:
        notifier.create_notification()
        assert notifier.notification["message"] == scenario["expected"]


@pytest.mark.parametrize(
    "scenario",
    [
        TerminatorSpec(
            function="notify",
            args={
                "server": "localhost",
                "port": "1025",
                "type": "email",
                "from": "test-email@email.com",
                "to": ["test-email1@email.com", "test-email2@email.com"],
                "subject": "test-subject",
                "message": "test-message",
            },
        ),
        TerminatorSpec(
            function="notify",
            args={
                "server": "localhost",
                "port": "1025",
                "type": "email",
                "from": "test-email@email.com",
                "cc": ["test-email1@email.com", "test-email2@email.com"],
                "bcc": ["test-email3@email.com", "test-email4@email.com"],
                "mimetype": "html",
                "subject": "test-subject",
                "message": "test-message",
                "attachments": [
                    f"{TEST_ATTACHEMENTS_PATH}test_attachement.txt",
                    f"{TEST_ATTACHEMENTS_PATH}test_image.png",
                ],
            },
        ),
    ],
)
def test_office365_notification_creation(scenario: TerminatorSpec) -> None:
    """Testing Office 365 notification creation."""
    notifier = EmailNotifier(scenario)
    body = notifier._create_graph_api_email_body()
    for recipient, test_recipient in zip(
        body.message.to_recipients, scenario.args.get("to", [])
    ):
        assert recipient.email_address.address == test_recipient
    for recipient, test_recipient in zip(
        body.message.cc_recipients, scenario.args.get("cc", [])
    ):
        assert recipient.email_address.address == test_recipient
    for recipient, test_recipient in zip(
        body.message.bcc_recipients, scenario.args.get("bcc", [])
    ):
        assert recipient.email_address.address == test_recipient

    if body.message.attachments:
        for attachment, test_attachment in zip(
            body.message.attachments, scenario.args.get("attachments")
        ):
            assert attachment.name == test_attachment.split("/")[-1]
            with open(test_attachment, "rb") as file:
                assert attachment.content_bytes == file.read()
