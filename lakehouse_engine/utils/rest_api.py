"""Module to handle REST API operations."""

import time
from enum import Enum

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lakehouse_engine.utils.logging_handler import LoggingHandler

LOG = LoggingHandler(__name__).get_logger()
DEFAULT_CONTENT_TYPE = "application/json"


class RestMethods(Enum):
    """Methods for REST API calls."""

    POST = "POST"
    PUT = "PUT"
    ALLOWED_METHODS = ["POST", "PUT"]


class RestStatusCodes(Enum):
    """REST Status Code."""

    RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
    OK_STATUS_CODES = [200]


class RESTApiException(requests.RequestException):
    """Class representing any possible REST API Exception."""

    def __init__(self, message: str) -> None:
        """Construct RESTApiException instances.

        Args:
            message: message to display on exception event.
        """
        super().__init__(message)


def get_basic_auth(username: str, password: str) -> requests.auth.HTTPBasicAuth:
    """Get the basic authentication object to authenticate REST requests.

    Args:
        username: username.
        password: password.

    Returns:
        requests.auth.HTTPBasicAuth: the HTTPBasicAuth object.
    """
    return requests.auth.HTTPBasicAuth(username, password)


def get_configured_session(
    sleep_seconds: float = 0.2,
    total_retries: int = 5,
    backoff_factor: int = 2,
    retry_status_codes: list = None,
    allowed_methods: list = None,
    protocol: str = "https://",
) -> requests.Session:
    """Get a configured requests Session with exponential backoff.

    Args:
        sleep_seconds: seconds to sleep before each request to avoid rate limits.
        total_retries: number of times to retry.
        backoff_factor: factor for the exponential backoff.
        retry_status_codes: list of status code that triggers a retry.
        allowed_methods: http methods that are allowed for retry.
        protocol: http:// or https://.

    Returns
        requests.Session: the configured session.
    """
    retry_status_codes = (
        retry_status_codes
        if retry_status_codes
        else RestStatusCodes.RETRY_STATUS_CODES.value
    )
    allowed_methods = (
        allowed_methods if allowed_methods else RestMethods.ALLOWED_METHODS.value
    )
    time.sleep(sleep_seconds)
    session = requests.Session()
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=retry_status_codes,
        allowed_methods=allowed_methods,
    )
    session.mount(protocol, HTTPAdapter(max_retries=retries))
    return session


def execute_api_request(
    method: str,
    url: str,
    headers: dict = None,
    basic_auth_dict: dict = None,
    json: dict = None,
    files: dict = None,
    sleep_seconds: float = 0.2,
) -> requests.Response:
    """Execute a REST API request.

    Args:
        method: REST method (e.g., POST or PUT).
        url: url of the api.
        headers: request headers.
        basic_auth_dict: basic http authentication details
            (e.g., {"username": "x", "password": "y"}).
        json: json payload to send in the request.
        files: files payload to send in the request.
        sleep_seconds: for how many seconds to sleep to avoid error 429.

    Returns:
        response from the HTTP request.
    """
    basic_auth: requests.auth.HTTPBasicAuth = None
    if basic_auth_dict:
        basic_auth = get_basic_auth(
            basic_auth_dict["username"], basic_auth_dict["password"]
        )

    return get_configured_session(sleep_seconds=sleep_seconds).request(
        method=method,
        url=url,
        headers=headers,
        auth=basic_auth,
        json=json,
        files=files,
    )
