"""A simple SMTP server for testing purposes."""

from logging import Logger
from typing import Any

from aiosmtpd import controller
from aiosmtpd.handlers import Message

from lakehouse_engine.utils.logging_handler import LoggingHandler


class SMTPHandler(Message):
    """Custom handler to capture emails during testing."""

    def __init__(self) -> None:
        """Initialize the SMTP handler."""
        super().__init__()
        self.messages: list = []

    def handle_message(self, message: Any) -> None:
        """Handle incoming messages and store them for verification.

        Args:
            message: The incoming email message.

        Returns:
            A string indicating the result of the message handling.
        """
        self.messages.append(message)


class SMTPServer:
    """Test SMTP server for unit testing."""

    _LOGGER: Logger = LoggingHandler(__name__).get_logger()

    def __init__(self, host: str, port: int) -> None:
        """Initialize the SMTP server.

        Args:
            host: The hostname of the SMTP server.
            port: The port number of the SMTP server.
        """
        self.host = host
        self.port = port
        self.handler = SMTPHandler()
        self.controller: controller.Controller | None = None

    def start(self) -> None:
        """Start the SMTP server."""
        self.controller = controller.Controller(
            self.handler, hostname=self.host, port=self.port
        )
        self.controller.start()
        self._LOGGER.info(f"Test SMTP server started on {self.host}:{self.port}")

    def stop(self) -> None:
        """Stop the SMTP server."""
        if self.controller:
            self.controller.stop()
            self._LOGGER.info("Test SMTP server stopped")

    def get_messages(self) -> list:
        """Get all captured messages."""
        return self.handler.messages

    def clear_messages(self) -> None:
        """Clear all captured messages."""
        self.handler.messages.clear()

    def get_last_message(self) -> Any:
        """Get the last received message."""
        return self.handler.messages[-1] if self.handler.messages else None
