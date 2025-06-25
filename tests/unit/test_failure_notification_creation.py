"""Unit tests for the creation of failure notifications."""

import os
import re
import subprocess  # nosec B404
import time
from signal import SIGKILL

import pytest

from lakehouse_engine.core.definitions import TerminatorSpec
from lakehouse_engine.terminators.notifier_factory import NotifierFactory
from lakehouse_engine.utils.logging_handler import LoggingHandler

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

        p = subprocess.Popen(
            args="python -u -m smtpd -c DebuggingServer -n "
            f"{server}:{port} > email_output",
            shell=True,
            text=True,
            preexec_fn=os.setsid,
        )

        # We sleep so the subprocess has time to start the debug smtp server
        time.sleep(2)

        NotifierFactory.generate_failure_notification(
            scenario["spec"], ValueError("Test exception")
        )

        os.killpg(os.getpgid(p.pid), SIGKILL)

        message = _parse_email_output()

        assert message == expected_output

    finally:
        os.remove("email_output")


def _parse_email_output() -> str:
    """Parse the mail that was received in the debug smtp server."""
    mail_content = open("email_output", "r").read()

    message = re.findall("(?<=b'').*?(?=b'--=)", mail_content, re.S)[1]

    message = message.replace("b'", "").replace("'\n", "\n")[1:-1]

    return str(message)
