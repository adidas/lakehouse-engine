"""Module to hold utilities Mocks tests."""

from __future__ import annotations

from types import TracebackType
from typing import Any, Self
from unittest.mock import MagicMock


class MockRESTResponse:
    """Mock Rest Responses for tests."""

    def __init__(
        self,
        status_code: int,
        json_data: dict[str, Any] | None = None,
        content: bytes = b"",
    ) -> None:
        """Construct MockRESTResponse instances.

        :param status_code: status code.
        :param json_data: json response.
        :param content: raw response content.
        """
        self.status_code: int = status_code
        self.json_data: dict[str, Any] | None = json_data
        self.content: bytes = content
        self.text: str = content.decode("utf-8", errors="ignore") if content else ""
        self.raise_for_status: MagicMock = MagicMock()

    def json(self) -> dict[str, Any] | None:
        """Get json response.

        :return dict: json response.
        """
        return self.json_data

    def __enter__(self) -> Self:
        """Allow use as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Context manager exit."""
        return
