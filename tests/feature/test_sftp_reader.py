"""Test SFTP reader.

Note: there is a limitation with the SFTP server/client which serves all files with
the same access and modified time, so we use the biggest dates to cover those
scenarios. Moreover, we also cover scenarios were no files are expected to be found,
due to the date filters.
"""
import gzip
import io
import os
from copy import deepcopy
from io import TextIOWrapper
from typing import Generator
from zipfile import ZipFile

import pandas as pd
import pytest
from paramiko import Transport
from paramiko.sftp_client import SFTPClient
from pytest_sftpserver.consts import (  # type: ignore
    SERVER_KEY_PRIVATE,
    SERVER_KEY_PUBLIC,
)
from pytest_sftpserver.sftp.server import SFTPServer  # type: ignore

from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.engine import load_data
from lakehouse_engine.utils.logging_handler import LoggingHandler
from tests.conftest import FEATURE_RESOURCES, LAKEHOUSE_FEATURE_OUT
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_PATH = "sftp_reader"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"
LOCAL_PATH = f"{TEST_RESOURCES}/data/"
LOGGER = LoggingHandler(__name__).get_logger()
FILES = os.listdir(LOCAL_PATH)


@pytest.fixture(scope="module")
def sftp_client(sftpserver: SFTPServer) -> Generator:
    """Create the sftp client to perform the tests.

    Args:
        sftpserver: a local SFTP-Server provided by the plugin pytest-sftpserver.
    """
    conn_cred = {"username": "a", "password": "b"}
    transport = Transport((sftpserver.host, sftpserver.port))
    transport.connect(
        hostkey=None,
        **conn_cred,
        pkey=None,
        gss_host=None,
        gss_auth=False,
        gss_kex=False,
        gss_deleg_creds=True,
        gss_trust_dns=True,
    )
    client = SFTPClient.from_transport(transport)
    yield client
    client.close()
    transport.close()


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "sftp_csv",
            "test_name": "between_dates",
            "sftp_files_format": "csv",
            "file_name": "file",
            "file_extension": ".csv",
        },
        {
            "scenario_name": "sftp_csv",
            "test_name": "between_dates_fail",
            "sftp_files_format": "csv",
            "file_name": "file",
            "file_extension": ".csv",
        },
    ],
)
def test_sftp_reader_csv(
    sftp_client: SFTPClient,
    sftpserver: SFTPServer,
    scenario: dict,
    remote_location: dict,
) -> None:
    """Test loads from sftp source - csv type.

    This tests covers a connection using keys and tests a scenario between dates.

    Args:
        sftp_client: sftp client used to perform tests.
        sftpserver: a local SFTP-Server created by pytest_sftpserver.
        scenario: scenario being tested.
        remote_location: serve files on remote location.
    """
    LOGGER.info(f"Starting Scenario {scenario['scenario_name']}")
    with sftpserver.serve_content(deepcopy(remote_location)):
        rename_remote_files(sftp_client)

        option_params = {
            "hostname": sftpserver.host,
            "username": "dummy_user",
            "password": "dummy_password",
            "port": sftpserver.port,
            "key_type": "RSA",
            "pkey": LocalStorage.read_file(SERVER_KEY_PUBLIC).split()[1],
            "key_filename": SERVER_KEY_PRIVATE,
            "date_time_gt": "2022-01-01",
            "date_time_lt": "9999-12-31"
            if "fail" not in scenario["test_name"]
            else "2021-01-01",
            "file_name_contains": f"e{scenario['file_extension']}",
            "args": {"sep": "|"},
        }

        acon = _get_test_acon(scenario, option_params)

        if "fail" not in scenario["test_name"]:
            _execute_and_validate(acon, scenario)
        else:
            with pytest.raises(
                ValueError, match="No files were found with the specified parameters."
            ):
                _execute_and_validate(acon, scenario)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "sftp_fwf",
            "test_name": "earliest_file",
            "sftp_files_format": "fwf",
            "file_name": "file5",
            "file_extension": ".txt",
        }
    ],
)
def test_sftp_reader_fwf(
    sftp_client: SFTPClient,
    sftpserver: SFTPServer,
    scenario: dict,
    remote_location: dict,
) -> None:
    """Test loads from sftp source - fwf type.

    This test covers a connection using add auto policy and tests
    earliest file and additional args.

    Args:
        sftp_client: sftp client used to perform tests.
        sftpserver: a local SFTP-Server created by pytest_sftpserver.
        scenario: scenario being tested.
        remote_location: serve files on remote location.
    """
    LOGGER.info(f"Starting Scenario {scenario['scenario_name']}")
    with sftpserver.serve_content(deepcopy(remote_location)):
        rename_remote_files(sftp_client)

        option_params = {
            "hostname": sftpserver.host,
            "username": "dummy_user",
            "password": "dummy_password",
            "port": sftpserver.port,
            "add_auto_policy": True,
            "earliest_file": True,
            "file_name_contains": scenario["file_extension"],
            "args": {"index_col": False, "names": ["value"]},
        }

        acon = _get_test_acon(scenario, option_params)

        _execute_and_validate(acon, scenario)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "sftp_gz_file",
            "test_name": "compressed_gz_file",
            "sftp_files_format": "csv",
            "file_name": "file6.compress",
            "file_extension": ".gz",
        },
    ],
)
def test_sftp_reader_gz_file(
    sftp_client: SFTPClient,
    sftpserver: SFTPServer,
    scenario: dict,
    remote_location: dict,
) -> None:
    """Test loads from sftp source - compressed gz type.

    This tests covers a connection using keys and tests a scenario of
    extracting a compressed gz file.

    Args:
        sftp_client: sftp client used to perform tests.
        sftpserver: a local SFTP-Server created by pytest_sftpserver.
        scenario: scenario being tested.
        remote_location: serve files on remote location.
    """
    LOGGER.info(f"Starting Scenario {scenario['scenario_name']}")
    with sftpserver.serve_content(deepcopy(remote_location)):
        rename_remote_files(sftp_client)

        option_params = {
            "hostname": sftpserver.host,
            "username": "dummy_user",
            "password": "dummy_password",
            "port": sftpserver.port,
            "key_type": "RSA",
            "pkey": LocalStorage.read_file(SERVER_KEY_PUBLIC).split()[1],
            "file_name_contains": "file6",
            "args": {"sep": "|"},
        }

        acon = _get_test_acon(scenario, option_params)

        _execute_and_validate(acon, scenario)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "sftp_json",
            "test_name": "greater_than",
            "sftp_files_format": "json",
            "file_name": "file3",
            "file_extension": ".json",
        }
    ],
)
def test_sftp_reader_json(
    sftp_client: SFTPClient,
    sftpserver: SFTPServer,
    scenario: dict,
    remote_location: dict,
) -> None:
    """Test loads from sftp source - json type.

    This tests covers a connection with add auto policy and tests date time
    greater than specified date and additional args.

    Args:
        sftp_client: sftp client used to perform tests.
        sftpserver: a local SFTP-Server created by pytest_sftpserver.
        scenario: scenario being tested.
        remote_location: serve files on remote location.
    """
    LOGGER.info(f"Starting Scenario {scenario['scenario_name']}")
    with sftpserver.serve_content(deepcopy(remote_location)):
        rename_remote_files(sftp_client)

        option_params = {
            "hostname": sftpserver.host,
            "username": "dummy_user",
            "password": "dummy_password",
            "port": sftpserver.port,
            "add_auto_policy": True,
            "date_time_gt": "2022-01-01",
            "file_name_contains": scenario["file_extension"],
            "args": {"lines": True, "orient": "columns"},
        }

        acon = _get_test_acon(scenario, option_params)

        _execute_and_validate(acon, scenario)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "sftp_mult_files",
            "test_name": "file_name_contains",
            "sftp_files_format": "csv",
            "file_name": "*",
            "file_extension": ".csv",
        }
    ],
)
def test_sftp_reader_mult_files(
    sftp_client: SFTPClient,
    sftpserver: SFTPServer,
    scenario: dict,
    remote_location: dict,
) -> None:
    """Test loads from sftp source - multiple files.

    This test covers a connection with add auto policy and tests file
    contains with additional args.

    Args:
        sftp_client: sftp client used to perform tests.
        sftpserver: a local SFTP-Server created by pytest_sftpserver.
        scenario: scenario being tested.
        remote_location: serve files on remote location.
    """
    LOGGER.info(f"Starting Scenario {scenario['scenario_name']}")

    with sftpserver.serve_content(deepcopy(remote_location)):
        rename_remote_files(sftp_client)

        option_params = {
            "hostname": sftpserver.host,
            "username": "dummy_user",
            "password": "dummy_password",
            "port": sftpserver.port,
            "add_auto_policy": True,
            "file_name_contains": scenario["file_extension"],
            "args": {"sep": "|"},
        }

        acon = _get_test_acon(scenario, option_params)

        _execute_and_validate(acon, scenario)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "sftp_xml",
            "test_name": "lower_than",
            "sftp_files_format": "xml",
            "file_name": "file4",
            "file_extension": ".xml",
        },
        {
            "scenario_name": "sftp_xml",
            "test_name": "lower_than_fails",
            "sftp_files_format": "xml",
            "file_name": "file4",
            "file_extension": ".xml",
        },
    ],
)
def test_sftp_reader_xml(
    sftp_client: SFTPClient,
    sftpserver: SFTPServer,
    scenario: dict,
    remote_location: dict,
) -> None:
    """Test loads from sftp source - xml type.

    This test covers a connection with add auto policy and date time
    lower than specified date.

    Args:
        sftp_client: sftp client used to perform tests.
        sftpserver: a local SFTP-Server created by pytest_sftpserver.
        scenario: scenario being tested.
        remote_location: serve files on remote location.
    """
    LOGGER.info(f"Starting Scenario {scenario['scenario_name']}")
    with sftpserver.serve_content(deepcopy(remote_location)):
        rename_remote_files(sftp_client)

        option_params = {
            "hostname": sftpserver.host,
            "username": "dummy_user",
            "password": "dummy_password",
            "port": sftpserver.port,
            "add_auto_policy": True,
            "date_time_lt": "9999-12-31"
            if "fail" not in scenario["test_name"]
            else "2022-01-01",
            "file_name_contains": scenario["file_extension"],
        }

        acon = _get_test_acon(scenario, option_params)

        if "fail" not in scenario["test_name"]:
            _execute_and_validate(acon, scenario)
        else:
            with pytest.raises(
                ValueError, match="No files were found with the specified parameters."
            ):
                _execute_and_validate(acon, scenario)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "scenario_name": "sftp_zip_file",
            "test_name": "compressed_zip_file",
            "sftp_files_format": "csv",
            "file_name": "file7",
            "file_extension": ".zip",
        },
    ],
)
def test_sftp_reader_zip_file(
    sftp_client: SFTPClient,
    sftpserver: SFTPServer,
    scenario: dict,
    remote_location: dict,
) -> None:
    """Test loads from sftp source - compressed zip type.

    This tests covers a connection using keys and tests a scenario of
    extracting a compressed zip file.

    Args:
        sftp_client: sftp client used to perform tests.
        sftpserver: a local SFTP-Server created by pytest_sftpserver.
        scenario: scenario being tested.
        remote_location: serve files on remote location.
    """
    LOGGER.info(f"Starting Scenario {scenario['scenario_name']}")
    with sftpserver.serve_content(deepcopy(remote_location)):
        rename_remote_files(sftp_client)

        option_params = {
            "hostname": sftpserver.host,
            "username": "dummy_user",
            "password": "dummy_password",
            "port": sftpserver.port,
            "key_type": "RSA",
            "pkey": LocalStorage.read_file(SERVER_KEY_PUBLIC).split()[1],
            "sub_dir": True,
            "file_name_contains": "file7",
            "args": {"sep": "|"},
        }

        acon = _get_test_acon(scenario, option_params)

        _execute_and_validate(acon, scenario)


def test_sftp_server_available(sftpserver: SFTPServer) -> None:
    """Test availability of sftp server.

    Args:
        sftpserver: a local SFTP-Server created by pytest_sftpserver.
    """
    assert isinstance(sftpserver, SFTPServer)
    assert sftpserver.is_alive()
    assert str(sftpserver.port) in sftpserver.url


def _execute_and_validate(
    acon: dict,
    scenario: dict,
) -> None:
    """Execute the load and compare data of result and control.

    Args:
        acon: acon dict to be tested.
        scenario: scenario to be tested.
    """
    load_data(acon=acon)
    result = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario['scenario_name']}/{scenario['test_name']}/data"
    )

    if scenario["scenario_name"] == "sftp_fwf":
        control = (
            ExecEnv.SESSION.read.format("text")
            .option("lineSep", "\n")
            .load(
                f"{TEST_RESOURCES}/data/{scenario['file_name']}"
                f"{scenario['file_extension']}"
            )
        )
    elif scenario["scenario_name"] == "sftp_json":
        control = DataframeHelpers.read_from_file(
            f"{TEST_RESOURCES}/data/{scenario['file_name']}"
            f"{scenario['file_extension']}",
            file_format="json",
        )
    elif scenario["scenario_name"] == "sftp_xml":
        control = (
            ExecEnv.SESSION.read.format("xml")
            .option("rowTag", "row")
            .load(
                f"{TEST_RESOURCES}/data/{scenario['file_name']}"
                f"{scenario['file_extension']}"
            )
        )
    elif scenario["scenario_name"] == "sftp_zip_file":
        with ZipFile(
            f"{TEST_RESOURCES}/data/{scenario['file_name']}"
            f"{scenario['file_extension']}",
            "r",
        ) as zf:
            file = pd.read_csv(TextIOWrapper(zf.open(zf.namelist()[0])), sep="|")
        control = ExecEnv.SESSION.createDataFrame(file)
    else:
        control = DataframeHelpers.read_from_file(
            f"{TEST_RESOURCES}/data/{scenario['file_name']}"
            f"{scenario['file_extension']}"
        )

    assert not DataframeHelpers.has_diff(result, control)


def _get_test_acon(
    scenario: dict,
    option_params: dict,
) -> dict:
    """Creates a test ACON with the desired logic for the algorithm.

    Args:
        scenario: the scenario being tested.
        option_params: option params for the scenario being tested.

    Returns:
        dict: the ACON for the algorithm configuration.
    """
    return {
        "input_specs": [
            {
                "spec_id": "sftp_source",
                "read_type": "batch",
                "data_format": "sftp",
                "sftp_files_format": scenario["sftp_files_format"],
                "location": "remote_location",
                "options": option_params,
            }
        ],
        "output_specs": [
            {
                "spec_id": "sftp_bronze",
                "input_id": "sftp_source",
                "write_type": "overwrite",
                "data_format": "csv",
                "options": {"header": True, "delimiter": "|", "inferSchema": True},
                "location": f"file:///{TEST_LAKEHOUSE_OUT}/{scenario['scenario_name']}/"
                f"{scenario['test_name']}/data",
            }
        ],
    }


@pytest.fixture(scope="module")
def remote_location() -> dict:
    """Get files to serve on a remote sftp location.

    For creating compressed file in the remote location,
    it is necessary to read, decompress, cast it to bytes
    and then send it to the location.
    For regular files, only file read is necessary.

    Returns:
        A dict with the files for the remote location configured.
    """
    remote_location: dict = {"remote_location": {}}

    for file in FILES:
        if file.endswith(".gz"):
            file_name = file.rsplit(".", 1)[0]
            with gzip.GzipFile(f"{LOCAL_PATH}{file}", "rb") as compressed_file:
                file_data_string = compressed_file.read().decode()
                file_bytes = gzip.compress(file_data_string.encode("utf-8"))
            remote_location["remote_location"][f"{file_name}"] = file_bytes
        elif file.endswith(".zip"):
            file_name = file.rsplit(".", 1)[0]
            with ZipFile(f"{LOCAL_PATH}{file}", "r") as f:
                with f.open(f"{file_name}.csv") as zfile:
                    data = zfile.read().decode()

            bytesfile = io.BytesIO()
            with ZipFile(bytesfile, mode="w") as zf:
                zf.writestr(f"{file_name}.csv", data)
                zf.close()
                file_bytes = bytesfile.getvalue()
            remote_location["remote_location"].update({"sub_dir": {}})
            remote_location["remote_location"]["sub_dir"][f"{file_name}"] = file_bytes
        else:
            file_name = file.split(".")[0]
            remote_location["remote_location"][f"{file_name}"] = LocalStorage.read_file(
                f"{LOCAL_PATH}{file}"
            )
    return remote_location


def rename_remote_files(sftp_client: SFTPClient) -> None:
    """Rename files served remotely in SFTP."""
    for file in FILES:
        file_name = file.rsplit(".", 1)[0]
        try:
            sftp_client.rename(
                f"/remote_location/{file_name}",
                f"/remote_location/{file}",
            )
        except IOError:
            pass
        try:
            sftp_client.rename(
                f"/remote_location/sub_dir/{file_name}",
                f"/remote_location/sub_dir/{file}",
            )
        except IOError:
            pass
