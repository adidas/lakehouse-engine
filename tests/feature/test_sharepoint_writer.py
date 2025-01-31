"""Test Sharepoint utils."""

import json
from typing import Any, List
from unittest.mock import MagicMock, patch

import pytest

from lakehouse_engine.engine import load_data
from lakehouse_engine.io.exceptions import (
    EndpointNotFoundException,
    InputNotFoundException,
    NotSupportedException,
)
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.local_storage import LocalStorage
from tests.utils.mocks import MockRESTResponse

"""
Tests for SharePoint-related utilities and functionality.

This test suite validates the behavior of the SharePoint writer, ensuring
that it handles various scenarios correctly. The tests cover validation of
mandatory inputs, unsupported operations, endpoint existence checks, and
successful writing to SharePoint.

Scenarios tested:
- Attempting to use streaming with the SharePoint writer raises a
  `NotSupportedException`.
- Missing mandatory options (`site_name`, `drive_name`, `local_path`) raises
  an `InputNotFoundException`.
- Providing an invalid endpoint raises an `EndpointNotFoundException`.
- Successful writing to SharePoint and associated log validation.

Mocks:
- `SharepointWriter._get_sharepoint_utils` is patched to simulate the behavior
  of the SharePoint utilities without making actual external calls.
- Mock REST responses simulate SharePoint API interactions for success cases.

Dependencies:
- Uses pytest for parameterized testing of different scenarios.
- Relies on a local storage utility for preparing test data and file operations.
"""

TEST_NAME = "sharepoint"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_NAME}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_NAME}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_NAME}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_NAME}"

TEST_SCENARIOS_EXCEPTIONS = [
    [
        "streaming_exception",
        "Sharepoint writer doesn't support streaming!",
    ],
    [
        "drive_exception",
        "Please provide all mandatory Sharepoint options. \n"
        "Expected: site_name, drive_name and local_path. "
        "Value should not be None.\n"
        "Provided: site_name=mock_site, \n"
        "drive_name=, \n"
        "local_path=mock_path",
    ],
    [
        "site_exception",
        "Please provide all mandatory Sharepoint options. \n"
        "Expected: site_name, drive_name and local_path. "
        "Value should not be None.\n"
        "Provided: site_name=, \n"
        "drive_name=mock_drive, \n"
        "local_path=mock_path",
    ],
    [
        "local_path_exception",
        "Please provide all mandatory Sharepoint options. \n"
        "Expected: site_name, drive_name and local_path. "
        "Value should not be None.\n"
        "Provided: site_name=mock_site, \n"
        "drive_name=mock_drive, \n"
        "local_path=",
    ],
    ["endpoint_exception", "The provided endpoint does not exist!"],
]

TEST_SCENARIOS_WRITER = [
    [
        "writer",
        "write_to_local_success",
        f"Deleted the local folder: {TEST_LAKEHOUSE_OUT}/writer/data",
    ],
]


@pytest.mark.parametrize("scenario", TEST_SCENARIOS_EXCEPTIONS)
@patch(
    "lakehouse_engine.io.writers.sharepoint_writer.SharepointWriter._get_sharepoint_utils"  # noqa
)
def test_sharepoint_writer_exceptions(
    mock_get_sharepoint_utils: MagicMock, scenario: List[str]
) -> None:
    """Test writing to Sharepoint from csv source.

    Args:
        scenario: scenario to test.
        mock_get_sharepoint_utils: patch sharepoint_utils.
    """
    mock_sharepoint_utils = MagicMock()
    mock_get_sharepoint_utils.return_value = mock_sharepoint_utils

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/file_source.csv",
        f"{TEST_LAKEHOUSE_IN}/data/",
    )

    if scenario[1] == "streaming_exception":
        with pytest.raises(NotSupportedException, match=scenario[2]):
            load_data(
                f"file://{TEST_RESOURCES}/{scenario[0]}/acons/streaming_exception.json"
            )
    elif scenario[1] == "site_exception":
        with pytest.raises(InputNotFoundException, match=scenario[2]):
            load_data(
                f"file://{TEST_RESOURCES}/{scenario[0]}/acons/site_exception.json"
            )
    elif scenario[1] == "drive_exception":
        with pytest.raises(InputNotFoundException, match=scenario[2]):
            load_data(
                f"file://{TEST_RESOURCES}/{scenario[0]}/acons/drive_exception.json"
            )
    elif scenario[1] == "local_path_exception":
        with pytest.raises(InputNotFoundException, match=scenario[2]):
            load_data(
                f"file://{TEST_RESOURCES}/{scenario[0]}/acons/local_path_exception.json"
            )
    elif scenario[1] == "endpoint_exception":
        mock_sharepoint_utils.check_if_endpoint_exists.return_value = False
        with pytest.raises(EndpointNotFoundException, match=scenario[2]):
            load_data(
                f"file://{TEST_RESOURCES}/{scenario[0]}/acons/endpoint_exception.json"
            )


@pytest.mark.parametrize("scenario", TEST_SCENARIOS_WRITER)
@patch(
    "lakehouse_engine.utils.sharepoint_utils.SharepointUtils.check_if_endpoint_exists",
    return_value=True,  # noqa
)
@patch("lakehouse_engine.utils.sharepoint_utils.SharepointUtils._create_app")  # noqa
@patch("lakehouse_engine.utils.sharepoint_utils.SharepointUtils._get_token")  # noqa
@patch(
    "lakehouse_engine.utils.sharepoint_utils.SharepointUtils._make_request",
    side_effect=[
        MockRESTResponse(
            status_code=200,
            json_data=json.loads(
                open(f"{TEST_RESOURCES}/writer/mocks/get_site_id.json").read()
            ),
        ),
        MockRESTResponse(
            status_code=200,
            json_data=json.loads(
                open(f"{TEST_RESOURCES}/writer/mocks/get_drive_id.json").read()
            ),
        ),
        MockRESTResponse(
            status_code=200,
            json_data=json.loads(
                open(f"{TEST_RESOURCES}/writer/mocks/create_upload_session.json").read()
            ),
        ),
        MockRESTResponse(status_code=200),  # final upload to sharepoint
    ],
)  # noqa
def test_sharepoint_writer(
    _: Any, __: Any, ___: Any, _make_requests: Any, scenario: List[str], caplog: Any
) -> None:
    """Test writing to Sharepoint from csv source.

    Args:
        scenario: scenario to test.
        caplog: fetch logs.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/{scenario[0]}/data/file_source.csv",
        f"{TEST_LAKEHOUSE_IN}/data/",
    )

    if scenario[0] == "writer" and scenario[1] == "write_to_local_success":
        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/{scenario[0]}/data/file_source.csv",
            f"{TEST_LAKEHOUSE_IN}/data/",
        )

        load_data(
            f"file://{TEST_RESOURCES}/{scenario[0]}/acons/write_to_local_success.json"
        )

        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/{scenario[0]}/data/file_source.csv",
            f"{TEST_LAKEHOUSE_CONTROL}/{scenario[0]}/data/",
        )

        assert scenario[2] in caplog.text
