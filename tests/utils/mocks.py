"""Module to hold utilities Mocks tests."""

from unittest.mock import MagicMock


class MockRESTResponse:
    """Mock Rest Responses for tests."""

    def __init__(self, status_code: int, json_data: dict = None):
        """Construct MockRESTResponse instances.

        :param status_code: status code.
        :param json_data: json response.
        """
        self.status_code = status_code
        self.json_data = json_data
        self.raise_for_status = MagicMock()

    def json(self) -> dict:
        """Get json response.

        :return dict: json response.
        """
        return self.json_data
