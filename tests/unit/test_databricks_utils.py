"""Unit tests for DatabricksUtils in lakehouse_engine.utils.databricks_utils."""

import sys
import types
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

from lakehouse_engine.utils.databricks_utils import DatabricksUtils

CONTEXT_KEYS = {
    "runId": "76890",
    "jobId": "657890",
    "jobName": "sadp-template-dummy_job",
    "workspaceId": "213245431",
    "usagePolicyId": "4567890",
}
CONTROL_DATA = {
    "run_id": "76890",
    "job_id": "657890",
    "job_name": "sadp-template-dummy_job",
    "workspace_id": "213245431",
    "policy_id": "4567890",
    "dp_name": "sadp-template",
    "environment": "dev",
}


@dataclass
class FileInfoFixture:
    """Minimal DBUtils FileInfo fixture for unit tests."""

    path: str
    name: str


def test_get_usage_context_for_serverless() -> None:
    """Test for get_usage_context_for_serverless method in DatabricksUtils."""
    # Create a fake module and function
    fake_module = types.ModuleType("dbruntime.databricks_repl_context")
    fake_module.get_context = MagicMock(  # type: ignore[attr-defined]
        return_value=MagicMock()
    )
    sys.modules["dbruntime"] = types.ModuleType("dbruntime")
    sys.modules["dbruntime.databricks_repl_context"] = fake_module

    mock_context = MagicMock(**CONTEXT_KEYS)

    # Patch get_context to return our mock context
    with patch(
        "dbruntime.databricks_repl_context.get_context", return_value=mock_context
    ):

        with patch(
            "lakehouse_engine.core.exec_env.ExecEnv.get_environment", return_value="dev"
        ):
            usage_stats: dict = {}
            DatabricksUtils.get_usage_context_for_serverless(usage_stats)

            assert (
                usage_stats == CONTROL_DATA
            ), f"Expected usage_stats to be {CONTROL_DATA}, but got {usage_stats}"

    # Clean up after test
    del sys.modules["dbruntime.databricks_repl_context"]
    del sys.modules["dbruntime"]


def test_dbutils_entry_name_and_path() -> None:
    """Dutils ls helpers normalize FileInfo-like entries."""
    entry = FileInfoFixture(path="/path/schema-1", name="schema-1")

    assert DatabricksUtils.get_dbutils_entry_name(entry) == "schema-1"
    assert DatabricksUtils.get_dbutils_entry_path(entry) == "/path/schema-1"


def test_dbutils_path_exists_true_and_false() -> None:
    """Dutils path helper returns True only when ls succeeds."""
    ok_dbutils = MagicMock()
    ok_dbutils.fs.ls.return_value = [
        FileInfoFixture(path="/some/path/file1", name="file1")
    ]
    assert DatabricksUtils.check_dbutils_path_exists(ok_dbutils, "/some/path") is True

    bad_dbutils = MagicMock()
    bad_dbutils.fs.ls.side_effect = Exception("not found")
    assert DatabricksUtils.check_dbutils_path_exists(bad_dbutils, "/missing") is False
