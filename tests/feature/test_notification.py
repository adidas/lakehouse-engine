"""Mail notifications tests."""

import os
import re
import subprocess  # nosec B404
import time
import typing
from signal import SIGKILL

import pytest

from lakehouse_engine.core.definitions import TerminatorSpec
from lakehouse_engine.engine import send_notification
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
            "name": "Email Notification Template",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "localhost",
                    "port": "1025",
                    "type": "email",
                    "template": "failure_notification_email",
                    "from": "test-email@email.com",
                    "cc": ["test-email1@email.com", "test-email2@email.com"],
                    "mimetype": "text/text",
                    "exception": "test-exception",
                },
            ),
            "expected": """
            Job local in workspace local has
            failed with the exception: test-exception""",
        },
        {
            "name": "Email Notification Free Form",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "localhost",
                    "port": "1025",
                    "type": "email",
                    "from": "test-email@email.com",
                    "to": ["test-email1@email.com", "test-email2@email.com"],
                    "mimetype": "text/text",
                    "subject": "Test Email",
                    "message": "Test message for the email.",
                    "attachments": [
                        f"{TEST_ATTACHEMENTS_PATH}test_attachement.txt",
                        f"{TEST_ATTACHEMENTS_PATH}test_image.png",
                    ],
                },
            ),
            "expected": "Test message for the email.",
            "expected_attachments": ["test_attachement.txt", "test_image.png"],
        },
        {
            "name": "Email Notification Free Form",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "localhost",
                    "port": "1025",
                    "type": "email",
                    "from": "test-email@email.com",
                    "to": ["test-email1@email.com", "test-email2@email.com"],
                    "mimetype": "text/html",
                    "subject": "Test Email",
                    "message": """<html><body>Test message.</body></html>""",
                },
            ),
            "expected": "<html><body>Test message.</body></html>",
        },
        {
            "name": "Error: non-existent template",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "localhost",
                    "port": "1025",
                    "type": "email",
                    "template": "missing_template",
                },
            ),
            "expected": "Template missing_template does not exist",
        },
        {
            "name": "Error: malformed definition",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "localhost",
                    "port": "1025",
                    "type": "email",
                    "from": "test-email@email.com",
                    "to": ["test-email1@email.com", "test-email2@email.com"],
                },
            ),
            "expected": "Malformed Notification Definition",
        },
        {
            "name": "Error: Using disallowed smtp server",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "smtp.test.com",
                    "port": "1025",
                    "type": "email",
                    "from": "test-email@email.com",
                    "to": ["test-email1@email.com", "test-email2@email.com"],
                    "mimetype": "text/text",
                    "subject": "Test Email",
                    "message": "Test message for the email.",
                },
            ),
            "expected": "Trying to use disallowed smtp server: "
            "'smtp.test.com'.\n"
            "Disallowed smtp servers: ['smtp.test.com']",
        },
    ],
)
def test_email_notification(scenario: dict) -> None:
    """Testing send email notification with template.

    Args:
        scenario: scenario to test.
    """
    spec: TerminatorSpec = scenario["spec"]
    name = scenario["name"]
    expected_output = scenario["expected"]

    notification_type = spec.args["type"]

    LOGGER.info(f"Executing notification test: {name}")

    if notification_type == "email":
        try:
            port = spec.args["port"]
            server = spec.args["server"]

            p = subprocess.Popen(
                args="python -u -m smtpd -c DebuggingServer -n "
                f"{server}:{port} > email_output",
                shell=True,
                text=True,
                preexec_fn=os.setsid,
            )

            # We sleep so the subprocess has time to start the debug smtp server
            time.sleep(2)

            email_notifier = EmailNotifier(spec)

            if "Error: " in name:
                with pytest.raises(
                    (
                        NotifierTemplateNotFoundException,
                        NotifierConfigException,
                        NotifierTemplateConfigException,
                    )
                ) as e:
                    email_notifier.create_notification()
                    email_notifier.send_notification()
                assert expected_output in str(e.value)
            else:
                email_notifier.create_notification()
                email_notifier.send_notification()
                (
                    email_from,
                    email_to,
                    email_cc,
                    email_bcc,
                    mimetype,
                    subject,
                    message,
                    attachments,
                ) = _parse_email_output()

                assert email_from == spec.args["from"]
                if "to" in spec.args:
                    assert email_to == spec.args["to"]
                if "cc" in spec.args:
                    assert email_cc == spec.args["cc"]
                if "bcc" in spec.args:
                    assert email_bcc == spec.args["bcc"]
                assert mimetype == spec.args["mimetype"]
                assert subject == spec.args["subject"]
                assert message == expected_output
                assert attachments == scenario.get("expected_attachments", [])

            os.killpg(os.getpgid(p.pid), SIGKILL)

        finally:
            os.remove("email_output")


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "name": "Email Notification Template",
            "args": {
                "server": "localhost",
                "port": "1025",
                "type": "email",
                "template": "failure_notification_email",
                "from": "test-email@email.com",
                "to": ["test-email1@email.com", "test-email2@email.com"],
                "cc": ["test-email3@email.com", "test-email4@email.com"],
                "exception": "test-exception",
            },
            "expected": """
            Job local in workspace local has
            failed with the exception: test-exception""",
        },
        {
            "name": "Email Notification Free Form",
            "args": {
                "server": "localhost",
                "port": "1025",
                "type": "email",
                "from": "test-email@email.com",
                "bcc": ["test-email1@email.com", "test-email2@email.com"],
                "mimetype": "text/text",
                "subject": "Test Email",
                "message": "Test message for the email.",
                "attachments": [
                    f"{TEST_ATTACHEMENTS_PATH}test_attachement.txt",
                    f"{TEST_ATTACHEMENTS_PATH}test_image.png",
                ],
            },
            "expected": "Test message for the email.",
            "expected_attachments": ["test_attachement.txt", "test_image.png"],
        },
        {
            "name": "Error: non-existent template",
            "args": {
                "server": "localhost",
                "port": "1025",
                "type": "email",
                "template": "missing_template",
            },
            "expected": "Template missing_template does not exist",
        },
        {
            "name": "Error: Malformed Notification Definition",
            "args": {
                "server": "localhost",
                "port": "1025",
                "type": "email",
                "from": "test-email@email.com",
                "to": ["test-email1@email.com", "test-email2@email.com"],
            },
            "expected": "Malformed Notification Definition",
        },
        {
            "name": "Error: Using disallowed smtp server",
            "args": {
                "server": "smtp.test.com",
                "port": "1025",
                "type": "email",
                "from": "test-email@email.com",
                "to": ["test-email1@email.com", "test-email2@email.com"],
                "mimetype": "plain",
                "subject": "Test Email",
                "message": "Test message for the email.",
            },
            "expected": "Trying to use disallowed smtp server: "
            "'smtp.test.com'.\n"
            "Disallowed smtp servers: ['smtp.test.com']",
        },
    ],
)
def test_email_notification_facade(scenario: dict) -> None:
    """Testing send email notification with template.

    Args:
        scenario: scenario to test.
    """
    args = scenario["args"]
    name = scenario["name"]
    expected_output = scenario["expected"]

    notification_type = args["type"]

    LOGGER.info(f"Executing notification test: {name}")

    if notification_type == "email":
        try:
            port = args["port"]
            server = args["server"]

            p = subprocess.Popen(
                args="python -u -m smtpd -c DebuggingServer -n "
                f"{server}:{port} > email_output",
                shell=True,
                text=True,
                preexec_fn=os.setsid,
            )

            # We sleep so the subprocess has time to start the debug smtp server
            time.sleep(2)

            if "Error: " in name:
                with pytest.raises(
                    (
                        NotifierTemplateNotFoundException,
                        NotifierConfigException,
                        NotifierTemplateConfigException,
                    )
                ) as e:
                    send_notification(args=args)
                assert expected_output in str(e.value)
            else:
                send_notification(args=args)
                (
                    email_from,
                    email_to,
                    email_cc,
                    email_bcc,
                    mimetype,
                    subject,
                    message,
                    attachments,
                ) = _parse_email_output()

                assert email_from == args["from"]
                if "to" in args:
                    assert email_to == args["to"]
                if "cc" in args:
                    assert email_cc == args["cc"]
                if "bcc" in args:
                    assert email_bcc == args["bcc"]
                assert mimetype == args["mimetype"]
                assert subject == args["subject"]
                assert message == expected_output
                assert attachments == scenario.get("expected_attachments", [])

            os.killpg(os.getpgid(p.pid), SIGKILL)

        finally:
            os.remove("email_output")


def _parse_email_output() -> typing.Tuple[str, list, list, list, str, str, str, list]:
    """Parse the mail that was received in the debug smtp server.

    Returns:
        A tuple with the email from, email to, cc, bcc, subject and message.
    """
    mail_content = open("email_output", "r").read()

    email_from = re.search("(?<=From: ).*(?<!')", mail_content).group()
    email_to = re.search("(?<=To: ).*(?<!')", mail_content).group().split(", ")
    email_cc = re.search("(?<=CC: ).*(?<!')", mail_content).group().split(", ")
    email_bcc = re.search("(?<=BCC: ).*(?<!')", mail_content).group().split(", ")
    mimetype = re.search("(?<=Content-Type: ).*(?=; charset)", mail_content).group()
    subject = re.search("(?<=Subject: ).*(?<!')", mail_content).group()
    message = re.findall("(?<=b'').*?(?=b'--=)", mail_content, re.S)[1]
    attachments = re.findall("""(?<=filename=").*(?=")""", mail_content)

    message = message.replace("b'", "").replace("'\n", "\n")[1:-1]

    return (
        email_from,
        email_to,
        email_cc,
        email_bcc,
        mimetype,
        subject,
        message,
        attachments,
    )
