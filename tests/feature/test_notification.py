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
from lakehouse_engine.utils.logging_handler import LoggingHandler

LOGGER = LoggingHandler(__name__).get_logger()


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
            "name": "Email Notification Template",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "localhost",
                    "port": "1025",
                    "type": "email",
                    "template": "opsgenie_notification",
                    "from": "test-email@email.com",
                    "to": ["test-email1@email.com", "test-email2@email.com"],
                    "args": {
                        "data_product": "test-data_product",
                        "alias": "test-alias",
                        "priority": "test-priority",
                        "entity": "test-entity",
                        "tags": "test-tags",
                        "action": "test-action",
                        "description": "test-description",
                        "exception": "test-exception",
                    },
                },
            ),
            "expected": """
                Opsgenie notification:
                    - Data Product: test-data_product
                    - Job name: local
                    - Alias: test-alias
                    - Priority: test-priority
                    - Entity: test-entity
                    - Tags: test-tags
                    - Action: test-action
                    - Description: test-description
                    - Exception: test-exception""",
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
                    "mimetype": "plain",
                    "subject": "Test Email",
                    "message": "Test message for the email.",
                },
            ),
            "expected": "Test message for the email.",
        },
        {
            "name": "Email Notification Free Form with Args",
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
                    "args": {
                        "exception": "test-exception",
                    },
                },
            ),
            "expected": "Malformed Notification Definition",
        },
        {
            "name": "Error: missing template parameters",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "localhost",
                    "port": "1025",
                    "type": "email",
                    "template": "opsgenie_notification",
                    "from": "test-email@email.com",
                    "to": ["test-email1@email.com", "test-email2@email.com"],
                    "args": {"exception": "test-exception"},
                },
            ),
            "expected": "The following template args have not been set: "
            "data_product, alias, priority, entity, tags, action, description",
        },
        {
            "name": "Error: Using disallowed smtp server",
            "spec": TerminatorSpec(
                function="notify",
                args={
                    "server": "smtpgate.emea.adsint.biz",
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
            "expected": "Trying to use disallowed smtp server: "
            "'smtpgate.emea.adsint.biz'.\n"
            "Disallowed smtp servers: ['smtpgate.emea.adsint.biz']",
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
                with pytest.raises(ValueError) as e:
                    email_notifier.create_notification()
                    email_notifier.send_notification()
                assert expected_output in str(e.value)
            else:
                email_notifier.create_notification()
                email_notifier.send_notification()
                email_from, email_to, subject, message = _parse_email_output()

                assert email_from == spec.args["from"]
                assert email_to == spec.args["to"]
                assert subject == spec.args["subject"]
                assert message == expected_output

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
                "args": {
                    "exception": "test-exception",
                },
            },
            "expected": """
            Job local in workspace local has
            failed with the exception: test-exception""",
        },
        {
            "name": "Email Notification Template",
            "args": {
                "server": "localhost",
                "port": "1025",
                "type": "email",
                "template": "opsgenie_notification",
                "from": "test-email@email.com",
                "to": ["test-email1@email.com", "test-email2@email.com"],
                "args": {
                    "data_product": "test-data_product",
                    "alias": "test-alias",
                    "priority": "test-priority",
                    "entity": "test-entity",
                    "tags": "test-tags",
                    "action": "test-action",
                    "description": "test-description",
                    "exception": "test-exception",
                },
            },
            "expected": """
                Opsgenie notification:
                    - Data Product: test-data_product
                    - Job name: local
                    - Alias: test-alias
                    - Priority: test-priority
                    - Entity: test-entity
                    - Tags: test-tags
                    - Action: test-action
                    - Description: test-description
                    - Exception: test-exception""",
        },
        {
            "name": "Email Notification Free Form",
            "args": {
                "server": "localhost",
                "port": "1025",
                "type": "email",
                "from": "test-email@email.com",
                "to": ["test-email1@email.com", "test-email2@email.com"],
                "mimetype": "plain",
                "subject": "Test Email",
                "message": "Test message for the email.",
            },
            "expected": "Test message for the email.",
        },
        {
            "name": "Email Notification Free Form with Args",
            "args": {
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
            "expected": "Test message for the email with templating: "
            "anything can go here.",
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
                "args": {
                    "exception": "test-exception",
                },
            },
            "expected": "Malformed Notification Definition",
        },
        {
            "name": "Error: missing template parameters",
            "args": {
                "server": "localhost",
                "port": "1025",
                "type": "email",
                "template": "opsgenie_notification",
                "from": "test-email@email.com",
                "to": ["test-email1@email.com", "test-email2@email.com"],
                "args": {"exception": "test-exception"},
            },
            "expected": "The following template args have not been set: "
            "data_product, alias, priority, entity, tags, action, description",
        },
        {
            "name": "Error: Using disallowed smtp server",
            "args": {
                "server": "smtpgate.emea.adsint.biz",
                "port": "1025",
                "type": "email",
                "from": "test-email@email.com",
                "to": ["test-email1@email.com", "test-email2@email.com"],
                "mimetype": "plain",
                "subject": "Test Email",
                "message": "Test message for the email with templating: {{ msg }}.",
                "args": {"msg": "anything can go here"},
            },
            "expected": "Trying to use disallowed smtp server: "
            "'smtpgate.emea.adsint.biz'.\n"
            "Disallowed smtp servers: ['smtpgate.emea.adsint.biz']",
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
                with pytest.raises(ValueError) as e:
                    send_notification(args=args)
                assert expected_output in str(e.value)
            else:
                send_notification(args=args)
                email_from, email_to, subject, message = _parse_email_output()

                assert email_from == args["from"]
                assert email_to == args["to"]
                assert subject == args["subject"]
                assert message == expected_output

            os.killpg(os.getpgid(p.pid), SIGKILL)

        finally:
            os.remove("email_output")


def _parse_email_output() -> typing.Tuple[str, list, str, str]:
    """Parse the mail that was received in the debug smtp server.

    Returns:
        A tuple with the email from, email to, subject and message.
    """
    mail_content = open("email_output", "r").read()

    email_from = re.search("(?<=From: ).*(?<!')", mail_content).group()
    email_to = re.search("(?<=To: ).*(?<!')", mail_content).group().split(", ")
    subject = re.search("(?<=Subject: ).*(?<!')", mail_content).group()
    message = re.findall("(?<=b'').*?(?=b'--=)", mail_content, re.S)[1]

    message = message.replace("b'", "").replace("'\n", "\n")[1:-1]

    return email_from, email_to, subject, message
