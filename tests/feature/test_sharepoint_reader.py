"""Test Sharepoint reader."""

import json
from pathlib import Path
from typing import Any, Dict, List, Set
from unittest.mock import Mock, patch

import pytest

from lakehouse_engine.core.definitions import SharepointFile
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.engine import load_data
from tests.conftest import FEATURE_RESOURCES
from tests.utils.local_storage import LocalStorage
from tests.utils.mocks import MockRESTResponse

TEST_NAME = "sharepoint"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_NAME}"

TEST_SCENARIOS_READER_SUCCESS: List[List[str]] = [
    ["reader", "read_single_csv_success"],
    ["reader", "read_single_csv_full_path_success"],
    ["reader", "read_folder_csv_success"],
    ["reader", "read_folder_csv_pattern_success"],
    ["reader", "read_single_csv_archive_enabled_success"],
    ["reader", "read_folder_csv_archive_enabled_success"],
    ["reader", "read_single_csv_archive_default_enabled_success"],
    ["reader", "read_single_csv_archive_success_subfolder_override_success"],
    ["reader", "read_folder_csv_archive_success_subfolder_override_success"],
]

TEST_SCENARIOS_READER_FAILURES: List[List[str]] = [
    [
        "reader",
        "read_folder_csv_one_file_schema_mismatch_should_archive_error",
        r"Schema mismatch",
    ],
    ["reader", "read_single_csv_empty_file_should_archive_error", r"is empty"],
    [
        "reader",
        "read_folder_csv_no_csv_files_should_fail",
        r"No CSV files found in folder: sp_test",
    ],
    [
        "reader",
        "read_folder_csv_pattern_matches_no_files_should_fail",
        r"No CSV files found in folder: sp_test",
    ],
    [
        "reader",
        "read_folder_csv_one_file_schema_mismatch_"
        "custom_error_subfolder_should_archive_error",
        r"Schema mismatch",
    ],
    [
        "reader",
        "read_single_csv_download_error_should_archive_error",
        r"Download failed",
    ],
    [
        "reader",
        "read_single_csv_spark_load_fails_should_archive_error",
        r"Failed to read Sharepoint file",
    ],
]


TEST_SCENARIOS_READER_EXCEPTIONS: List[List[str]] = [
    [
        "reader",
        "read_single_csv_full_path_with_file_name_should_fail",
        "When `folder_relative_path` points to a file, `file_name` must be None.",
    ],
    [
        "reader",
        "read_folder_path_does_not_exist_should_fail",
        "Folder 'missing_folder' does not exist in Sharepoint.",
    ],
    [
        "reader",
        "read_file_name_and_file_pattern_conflict_should_fail",
        "Conflicting options: provide either `file_name` or `file_pattern`",
    ],
    [
        "reader",
        "read_file_name_unsupported_extension_should_fail",
        "`file_name` must end with one of",
    ],
    [
        "reader",
        "read_folder_relative_path_looks_like_file_unsupported_extension_should_fail",
        "`folder_relative_path` appears to be a file path but does not end with one of",
    ],
    [
        "reader",
        "read_unsupported_file_type_should_fail",
        "`file_type` must be one of",
    ],
    [
        "reader",
        "read_single_csv_full_path_with_file_pattern_should_fail",
        "When `folder_relative_path` points to a file, `file_pattern` must be None.",
    ],
    [
        "reader",
        "read_single_csv_full_path_with_file_type_should_fail",
        "When `folder_relative_path` points to a file, `file_type` must be None",
    ],
]

# Helper functions


def _read_bytes(path_value: str) -> bytes:
    """Read a test file as bytes."""
    return Path(path_value).read_bytes()


def _get_output_path_by_scenario() -> Dict[str, str]:
    """Return the delta output location for each success scenario."""
    return {
        "read_single_csv_success": (
            "/app/tests/lakehouse/out/feature/sharepoint/reader/delta/"
        ),
        "read_single_csv_full_path_success": (
            "/app/tests/lakehouse/out/feature/sharepoint/reader/delta_full_path/"
        ),
        "read_folder_csv_success": (
            "/app/tests/lakehouse/out/feature/sharepoint/reader/delta_folder/"
        ),
        "read_folder_csv_pattern_success": (
            "/app/tests/lakehouse/out/feature/sharepoint/reader/delta_folder_pattern/"
        ),
        "read_single_csv_archive_enabled_success": (
            "/app/tests/lakehouse/out/feature/sharepoint/"
            "reader/delta_single_archive_enabled/"
        ),
        "read_folder_csv_archive_enabled_success": (
            "/app/tests/lakehouse/out/feature/sharepoint/"
            "reader/delta_folder_archive_enabled/"
        ),
        "read_single_csv_archive_default_enabled_success": (
            "/app/tests/lakehouse/out/feature/sharepoint/"
            "reader/delta_single_archive_default_enabled/"
        ),
        "read_single_csv_archive_success_subfolder_override_success": (
            "/app/tests/lakehouse/out/feature/sharepoint/"
            "reader/delta_single_archive_success_subfolder_override/"
        ),
        "read_folder_csv_archive_success_subfolder_override_success": (
            "/app/tests/lakehouse/out/feature/sharepoint/"
            "reader/delta_folder_archive_success_subfolder_override/"
        ),
    }


def _setup_sharepoint_reader_mocks_for_success(
    scenario_name: str,
    mock_list_items_in_path: Mock,
    mock_get_file_metadata: Mock,
) -> None:
    """Configure SharePoint mocks used by Sharepoint reader success scenarios.

    Args:
        scenario_name: Test scenario identifier.
        mock_list_items_in_path: Mock for SharepointUtils.list_items_in_path.
        mock_get_file_metadata: Mock for SharepointUtils.get_file_metadata.
    """
    is_folder_read_scenario = scenario_name.startswith("read_folder_")

    if is_folder_read_scenario:
        mock_list_items_in_path.return_value = [
            {"name": "sample_1.csv", "createdDateTime": "", "lastModifiedDateTime": ""},
            {"name": "sample_2.csv", "createdDateTime": "", "lastModifiedDateTime": ""},
            {"name": "other.csv", "createdDateTime": "", "lastModifiedDateTime": ""},
            {"name": "ignore.txt", "createdDateTime": "", "lastModifiedDateTime": ""},
        ]

        file_bytes_by_path: Dict[str, bytes] = {
            "sp_test/sample_1.csv": _read_bytes(
                f"{TEST_RESOURCES}/reader/data/sample_1.csv"
            ),
            "sp_test/sample_2.csv": _read_bytes(
                f"{TEST_RESOURCES}/reader/data/sample_2.csv"
            ),
            "sp_test/other.csv": _read_bytes(f"{TEST_RESOURCES}/reader/data/other.csv"),
        }

        def get_file_metadata_side_effect_for_folder(file_path: str) -> SharepointFile:
            """Side effect function for `get_file_metadata` mock in folder scenarios."""
            return SharepointFile(
                file_name=file_path.split("/")[-1],
                time_created="",
                time_modified="",
                content=file_bytes_by_path[file_path],
                _folder=file_path.rsplit("/", 1)[0],
            )

        mock_get_file_metadata.side_effect = get_file_metadata_side_effect_for_folder
        return

    content = _read_bytes(f"{TEST_RESOURCES}/reader/data/sample_1.csv")

    def get_file_metadata_side_effect_for_single_file(file_path: str) -> SharepointFile:
        """Side effect function for `get_file_metadata` mock in single file scenarios.

        Args:
            file_path: The path of the file for which metadata is being requested.

        Returns:
            A SharepointFile object with the content set to the bytes read from the
            test file.
        """
        folder = file_path.rsplit("/", 1)[0] if "/" in file_path else "sp_test"
        return SharepointFile(
            file_name=file_path.split("/")[-1],
            time_created="",
            time_modified="",
            content=content,
            _folder=folder,
        )

    mock_get_file_metadata.side_effect = get_file_metadata_side_effect_for_single_file


def _assert_archive_calls_for_success(
    scenario_name: str,
    mock_archive_sharepoint_file: Mock,
) -> None:
    """Assert archive behavior for Sharepoint reader success scenarios.

    Args:
        scenario_name: Test scenario identifier.
        mock_archive_sharepoint_file: Mock for SharepointUtils.archive_sharepoint_file.
    """
    is_folder_read_scenario = scenario_name.startswith("read_folder_")

    folder_expected_calls_by_scenario: Dict[str, int] = {
        "read_folder_csv_success": 3,
        "read_folder_csv_pattern_success": 2,
        "read_folder_csv_archive_enabled_success": 3,
        "read_folder_csv_archive_success_subfolder_override_success": 3,
    }

    folder_archive_enabled_scenarios: Set[str] = {
        "read_folder_csv_archive_enabled_success",
        "read_folder_csv_archive_success_subfolder_override_success",
    }

    single_file_archive_enabled_scenarios: Set[str] = {
        "read_single_csv_archive_enabled_success",
        "read_single_csv_archive_default_enabled_success",
        "read_single_csv_archive_success_subfolder_override_success",
    }

    success_subfolder_by_scenario: Dict[str, str] = {
        "read_single_csv_archive_success_subfolder_override_success": "processed",
        "read_folder_csv_archive_success_subfolder_override_success": "processed",
    }

    expected_success_subfolder = success_subfolder_by_scenario.get(
        scenario_name, "done"
    )

    if is_folder_read_scenario:
        expected_calls = folder_expected_calls_by_scenario[scenario_name]
        assert mock_archive_sharepoint_file.call_count == expected_calls

        expected_move_enabled = scenario_name in folder_archive_enabled_scenarios
        for call in mock_archive_sharepoint_file.call_args_list:
            assert call.kwargs["move_enabled"] is expected_move_enabled
            if expected_move_enabled:
                to_path = call.kwargs["to_path"]
                assert to_path is not None
                assert to_path.endswith(f"/{expected_success_subfolder}")
        return

    mock_archive_sharepoint_file.assert_called_once()
    expected_move_enabled = scenario_name in single_file_archive_enabled_scenarios
    assert (
        mock_archive_sharepoint_file.call_args.kwargs["move_enabled"]
        is expected_move_enabled
    )

    if expected_move_enabled:
        to_path = mock_archive_sharepoint_file.call_args.kwargs["to_path"]
        assert to_path is not None
        assert to_path.endswith(f"/{expected_success_subfolder}")


def _assert_sharepoint_reader_success_output(
    scenario_name: str,
    output_path: str,
) -> None:
    """Assert the delta output produced by Sharepoint reader success scenarios.

    Args:
        scenario_name: Test scenario identifier.
        output_path: Delta output location for the scenario.
    """
    data_frame = ExecEnv.SESSION.read.format("delta").load(output_path)
    assert data_frame.columns == ["col_a", "col_b"]

    if scenario_name in {
        "read_folder_csv_success",
        "read_folder_csv_archive_enabled_success",
        "read_folder_csv_archive_success_subfolder_override_success",
    }:
        assert data_frame.count() == 3
        rows = [row.asDict() for row in data_frame.orderBy("col_a").collect()]
        assert rows == [
            {"col_a": 1, "col_b": 2},
            {"col_a": 3, "col_b": 4},
            {"col_a": 999, "col_b": 999},
        ]
    elif scenario_name == "read_folder_csv_pattern_success":
        assert data_frame.count() == 2
        rows = [row.asDict() for row in data_frame.orderBy("col_a").collect()]
        assert rows == [
            {"col_a": 1, "col_b": 2},
            {"col_a": 3, "col_b": 4},
        ]


@patch(
    "lakehouse_engine.utils.sharepoint_utils.SharepointUtils.archive_sharepoint_file"
)
@patch("lakehouse_engine.utils.sharepoint_utils.SharepointUtils.get_file_metadata")
@patch("lakehouse_engine.utils.sharepoint_utils.SharepointUtils.list_items_in_path")
@patch(
    "lakehouse_engine.utils.sharepoint_utils.SharepointUtils.check_if_endpoint_exists",
    return_value=True,
)
@patch("lakehouse_engine.utils.sharepoint_utils.SharepointUtils._create_app")
@patch("lakehouse_engine.utils.sharepoint_utils.SharepointUtils._get_token")
@patch("lakehouse_engine.utils.sharepoint_utils.SharepointUtils._make_request")
@pytest.mark.parametrize("scenario", TEST_SCENARIOS_READER_SUCCESS)
def test_sharepoint_reader_success(
    mock_make_request: Any,
    mock_get_token: Any,
    mock_create_app: Any,
    mock_check_if_endpoint_exists: Any,
    mock_list_items_in_path: Any,
    mock_get_file_metadata: Any,
    mock_archive_sharepoint_file: Any,
    scenario: List[str],
) -> None:
    """Test Sharepoint reader happy paths (single file, full path, folder)."""
    scenario_name = scenario[1]

    output_path_by_scenario = _get_output_path_by_scenario()

    mock_archive_sharepoint_file.return_value = None
    mock_make_request.return_value = None

    _setup_sharepoint_reader_mocks_for_success(
        scenario_name=scenario_name,
        mock_list_items_in_path=mock_list_items_in_path,
        mock_get_file_metadata=mock_get_file_metadata,
    )

    output_path = output_path_by_scenario[scenario_name]
    LocalStorage.clean_folder(output_path)

    load_data(f"file://{TEST_RESOURCES}/{scenario[0]}/acons/{scenario_name}.json")

    _assert_archive_calls_for_success(
        scenario_name=scenario_name,
        mock_archive_sharepoint_file=mock_archive_sharepoint_file,
    )

    _assert_sharepoint_reader_success_output(
        scenario_name=scenario_name,
        output_path=output_path,
    )


@patch(
    "lakehouse_engine.utils.sharepoint_utils.SharepointUtils.archive_sharepoint_file"
)
@patch("lakehouse_engine.utils.sharepoint_utils.SharepointUtils.get_file_metadata")
@patch("lakehouse_engine.utils.sharepoint_utils.SharepointUtils.list_items_in_path")
@patch(
    "lakehouse_engine.utils.sharepoint_utils.SharepointUtils.check_if_endpoint_exists",
    return_value=True,
)
@patch("lakehouse_engine.utils.sharepoint_utils.SharepointUtils._create_app")
@patch("lakehouse_engine.utils.sharepoint_utils.SharepointUtils._get_token")
@patch("lakehouse_engine.utils.sharepoint_utils.SharepointUtils._make_request")
@pytest.mark.parametrize("scenario", TEST_SCENARIOS_READER_FAILURES)
def test_sharepoint_reader_failures(
    mock_make_request: Any,
    mock_get_token: Any,
    mock_create_app: Any,
    mock_check_if_endpoint_exists: Any,
    mock_list_items_in_path: Any,
    mock_get_file_metadata: Any,
    mock_archive_sharepoint_file: Any,
    scenario: List[str],
    tmp_path: Path,
) -> None:
    """Test Sharepoint reader runtime failure scenarios.

    This test covers failures that happen during file processing (for example schema
    mismatches, empty files, or folder contents that result in non readable CSVs).
    These are different from `test_sharepoint_reader_exceptions`, which validates
    fail-fast configuration errors (invalid option combinations, unsupported file
    types) that should raise before any file processing starts.
    For runtime failures where archiving is enabled, the reader should move the
    problematic file(s) to the configured error subfolder (default: "error").
    The assertions at the end verify:
    - the job failed with the expected error message
    - archiving was invoked with `move_enabled=True`
    - the archive target folder matches the expected error subfolder
    - the archived file is one of the files involved in the scenario
    """
    scenario_name = scenario[1]
    expected_error_regex = scenario[2]

    mock_archive_sharepoint_file.return_value = None
    mock_make_request.return_value = None

    should_assert_no_archive_calls = False
    expected_error_subfolder = "error"
    allowed_file_names: Set[str] = set()

    should_patch_spark_load = False

    # Scenario-specific mocking + expectations (no load_data here)
    if "schema_mismatch" in scenario_name:
        expected_error_subfolder = (
            "failed" if "custom_error_subfolder" in scenario_name else "error"
        )
        allowed_file_names = {"sample_1.csv", "bad_schema.csv"}

        mock_list_items_in_path.return_value = [
            {"name": "sample_1.csv", "createdDateTime": "", "lastModifiedDateTime": ""},
            {
                "name": "bad_schema.csv",
                "createdDateTime": "",
                "lastModifiedDateTime": "",
            },
        ]

        file_bytes_by_path: Dict[str, bytes] = {
            "sp_test/sample_1.csv": _read_bytes(
                f"{TEST_RESOURCES}/reader/data/sample_1.csv"
            ),
            "sp_test/bad_schema.csv": _read_bytes(
                f"{TEST_RESOURCES}/reader/data/bad_schema.csv"
            ),
        }

        def get_file_metadata_side_effect(file_path: str) -> SharepointFile:
            return SharepointFile(
                file_name=file_path.split("/")[-1],
                time_created="",
                time_modified="",
                content=file_bytes_by_path[file_path],
                _folder=file_path.rsplit("/", 1)[0],
            )

        mock_get_file_metadata.side_effect = get_file_metadata_side_effect

    elif scenario_name == "read_single_csv_empty_file_should_archive_error":
        allowed_file_names = {"empty.csv"}

        def get_file_metadata_side_effect(file_path: str) -> SharepointFile:
            return SharepointFile(
                file_name="empty.csv",
                time_created="",
                time_modified="",
                content=b"",
                _folder="sp_test",
            )

        mock_get_file_metadata.side_effect = get_file_metadata_side_effect

    elif scenario_name == "read_folder_csv_no_csv_files_should_fail":
        should_assert_no_archive_calls = True
        mock_list_items_in_path.return_value = [
            {"name": "ignore.txt", "createdDateTime": "", "lastModifiedDateTime": ""},
            {"name": "readme.md", "createdDateTime": "", "lastModifiedDateTime": ""},
        ]

    elif scenario_name == "read_folder_csv_pattern_matches_no_files_should_fail":
        should_assert_no_archive_calls = True
        mock_list_items_in_path.return_value = [
            {"name": "sample_1.csv", "createdDateTime": "", "lastModifiedDateTime": ""},
            {"name": "sample_2.csv", "createdDateTime": "", "lastModifiedDateTime": ""},
            {"name": "other.csv", "createdDateTime": "", "lastModifiedDateTime": ""},
        ]

    elif scenario_name == "read_single_csv_download_error_should_archive_error":
        allowed_file_names = {"sample_1.csv"}

        first_sharepoint_file = SharepointFile(
            file_name="sample_1.csv",
            time_created="",
            time_modified="",
            content=b"not-empty",
            _folder="sp_test",
        )

        mock_get_file_metadata.side_effect = [
            first_sharepoint_file,
            ValueError("Download failed"),
        ]
    elif scenario_name == "read_single_csv_spark_load_fails_should_archive_error":
        should_patch_spark_load = True
        allowed_file_names = {"sample_1.csv"}

        sp_file_first = SharepointFile(
            file_name="sample_1.csv",
            time_created="",
            time_modified="",
            content=b"col_a,col_b\n1,2\n",
            _folder="sp_test",
        )

        sp_file_second = SharepointFile(
            file_name="sample_1.csv",
            time_created="",
            time_modified="",
            content=b"col_a,col_b\n1,2\n",
            _folder="sp_test",
        )

        mock_get_file_metadata.side_effect = [sp_file_first, sp_file_second]

    else:
        raise ValueError(f"Unhandled failure scenario: {scenario_name}")

    # Execute + assert error (exactly once per scenario)
    acon_path = f"file://{TEST_RESOURCES}/{scenario[0]}/acons/{scenario_name}.json"

    if should_patch_spark_load:
        fake_local_file: Path = tmp_path / "fake.csv"
        fake_local_file.write_text("dummy")
        with (
            patch(
                "lakehouse_engine.utils.sharepoint_utils."
                "SharepointUtils.save_to_staging_area",
                return_value=str(fake_local_file),
            ),
            patch(
                "pyspark.sql.readwriter.DataFrameReader.load",
                side_effect=Exception("Spark load failed"),
            ),
        ):
            with pytest.raises(ValueError, match=expected_error_regex):
                load_data(acon_path)
    else:
        with pytest.raises(ValueError, match=expected_error_regex):
            load_data(acon_path)

    # For scenarios that fail before reading any CSV file (folder contains no CSVs, or
    # the pattern filters everything out), there is no concrete CSV file to archive.
    # We assert no archive attempts are made.
    if should_assert_no_archive_calls:
        assert mock_archive_sharepoint_file.call_count == 0
        assert mock_get_file_metadata.call_count == 0
        return

    # For processing-time failures, the reader should attempt to archive the failing
    # file(s) into the configured error subfolder (default: "error").
    # We assert at least one archive call targeted that error folder with move enabled,
    # and that the archived file belongs to this scenario.
    error_calls = [
        c
        for c in mock_archive_sharepoint_file.call_args_list
        if (c.kwargs.get("to_path") or "").endswith(f"/{expected_error_subfolder}")
    ]
    assert len(error_calls) >= 1

    for c in error_calls:
        assert c.kwargs["move_enabled"] is True
        sp_file = c.kwargs.get("sp_file")
        assert sp_file is not None
        assert sp_file.file_name in allowed_file_names


@pytest.mark.parametrize("scenario", TEST_SCENARIOS_READER_EXCEPTIONS)
@patch("lakehouse_engine.utils.sharepoint_utils.SharepointUtils._create_app")
@patch(
    "lakehouse_engine.utils.sharepoint_utils.SharepointUtils._get_token",
    return_value="fake-token",
)
@patch(
    "lakehouse_engine.utils.sharepoint_utils.SharepointUtils._make_request",
    side_effect=[
        # site id
        MockRESTResponse(
            status_code=200,
            json_data=json.loads(
                open(f"{TEST_RESOURCES}/reader/mocks/get_site_id.json").read()
            ),
        ),
        # drive id
        MockRESTResponse(
            status_code=200,
            json_data=json.loads(
                open(f"{TEST_RESOURCES}/reader/mocks/get_drive_id.json").read()
            ),
        ),
    ],
)
@patch(
    "lakehouse_engine.utils.sharepoint_utils.SharepointUtils.check_if_endpoint_exists",
    return_value=True,
)
def test_sharepoint_reader_exceptions(
    mock_check_if_endpoint_exists: Any,
    mock_make_request: Any,
    mock_get_token: Any,
    mock_create_app: Any,
    scenario: List[str],
) -> None:
    """Test Sharepoint reader invalid configs that must fail fast."""
    scenario_name = scenario[1]

    if scenario_name == "read_folder_path_does_not_exist_should_fail":
        mock_check_if_endpoint_exists.return_value = False

    with pytest.raises(ValueError, match=scenario[2]):
        load_data(f"file://{TEST_RESOURCES}/{scenario[0]}/acons/{scenario_name}.json")
