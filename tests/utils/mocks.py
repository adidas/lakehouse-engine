"""Module to hold utilities Mocks tests."""

from __future__ import annotations

from typing import Any, Optional
from unittest.mock import MagicMock


class MockRESTResponse:
    """Mock Rest Responses for tests."""

    def __init__(
        self,
        status_code: int,
        json_data: Optional[dict[str, Any]] = None,
        content: bytes = b"",
    ) -> None:
        """Construct MockRESTResponse instances.

        :param status_code: status code.
        :param json_data: json response.
        :param content: raw response content.
        """
        self.status_code: int = status_code
        self.json_data: Optional[dict[str, Any]] = json_data
        self.content: bytes = content
        self.text: str = content.decode("utf-8", errors="ignore") if content else ""
        self.raise_for_status: MagicMock = MagicMock()

    def json(self) -> Optional[dict[str, Any]]:
        """Get json response.

        :return dict: json response.
        """
        return self.json_data

    def __enter__(self) -> MockRESTResponse:
        """Allow use as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        """Context manager exit."""
        return None
