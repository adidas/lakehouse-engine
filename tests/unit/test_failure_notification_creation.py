"""Unit tests for the creation of failure notifications."""

import re
import time

import pytest

from lakehouse_engine.core.definitions import TerminatorSpec
from lakehouse_engine.terminators.notifier_factory import NotifierFactory
from lakehouse_engine.utils.logging_handler import LoggingHandler
from tests.utils.smtp_server import SMTPServer

LOGGER = LoggingHandler(__name__).get_logger()


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "name": "Email notification creation using a template.",
            "spec": [
                TerminatorSpec(
                    function="notify",
                    args={
                        "server": "localhost",
                        "port": "1025",
                        "type": "email",
                        "template": "failure_notification_email",
                        "from": "test-email@email.com",
                        "to": ["test-email1@email.com", "test-email2@email.com"],
                        "on_failure": True,
                    },
                ),
            ],
            "server": "localhost",
            "port": "1025",
            "expected": """
            Job local in workspace local has
            failed with the exception: Test exception""",
        },
    ],
)
def test_failure_notification_creation(scenario: dict) -> None:
    """Testing notification creation.

    Args:
        scenario: scenario to test.
    """
    expected_output = scenario["expected"]

    try:
        port = scenario["port"]
        server = scenario["server"]

        smtp_server = SMTPServer(server, port)
        smtp_server.start()

        # We sleep so the subprocess has time to start the debug smtp server
        time.sleep(2)

        NotifierFactory.generate_failure_notification(
            scenario["spec"], ValueError("Test exception")
        )

        message = _parse_email_output(smtp_server.get_last_message().as_string())

        assert message == expected_output

    finally:
        smtp_server.stop()


def _parse_email_output(mail_content: str) -> str:
    """Parse the mail that was received in the debug smtp server.

    The regex is fetching the data between the encoding's field 'bit' and
    the next boundary of the email.
    Example notification content:
        Content-Type: multipart/mixed; boundary="===============1362798268250904879=="
        MIME-Version: 1.0
        From: test-email@email.com
        To: test-email1@email.com, test-email2@email.com
        CC:
        BCC:
        Subject: Service Failure
        Importance: normal
        X-Peer: ('::1', 49472, 0, 0)
        X-MailFrom: test-email@email.com
        X-RcptTo: test-email1@email.com, test-email2@email.com

        --===============1362798268250904879==
        Content-Type: text/text; charset="us-ascii"
        MIME-Version: 1.0
        Content-Transfer-Encoding: 7bit


                    Job local in workspace local has
                    failed with the exception: Test exception
        --===============1362798268250904879==--

    Args:
        mail_content: The content of the email to parse.

    Returns:
        The parsed email message.
    """
    message = re.search("(?<=bit\n).*?(?=--=)", mail_content, re.S).group()[1:-1]

    return str(message)
