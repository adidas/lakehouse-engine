"""Module to configure the test environment."""

from typing import Any, Generator
from unittest.mock import patch

import pytest

from lakehouse_engine.core.exec_env import ExecEnv
from tests.utils.exec_env_helpers import ExecEnvHelpers
from tests.utils.local_storage import LocalStorage

RESOURCES = "/app/tests/resources/"
FEATURE_RESOURCES = RESOURCES + "feature"
UNIT_RESOURCES = RESOURCES + "unit"
LAKEHOUSE = "/app/tests/lakehouse/"
LAKEHOUSE_FEATURE_IN = LAKEHOUSE + "in/feature"
LAKEHOUSE_FEATURE_CONTROL = LAKEHOUSE + "control/feature"
LAKEHOUSE_FEATURE_OUT = LAKEHOUSE + "out/feature"
LAKEHOUSE_FEATURE_LOGS = LAKEHOUSE + "logs/lakehouse-engine-logs"


def pytest_configure(config: Any) -> None:
    """Register custom markers for Spark Connect compatibility."""
    config.addinivalue_line(
        "markers",
        "local_only: marks tests that only work with local Spark",
    )
    config.addinivalue_line(
        "markers", "connect_only: marks tests that only work with Spark connect"
    )


@pytest.fixture(scope="session", autouse=True)
def patch_databricks_utils_job_info() -> Generator:
    """Patch DatabricksUtils.get_databricks_job_information to return local values."""
    with patch(
        "lakehouse_engine.utils.databricks_utils."
        "DatabricksUtils.get_databricks_job_information",
        return_value=("local", "local"),
    ):
        yield


def pytest_addoption(parser: Any) -> Any:
    """Setting extra options for pytest command."""
    parser.addoption(
        "--spark_driver_memory",
        action="store",
        help="memory limit for the spark driver (default 2g)",
    )
    parser.addoption(
        "--spark_mode",
        action="store",
        default="local",
        choices=["local", "connect"],
        help="spark mode: 'local' for local spark, 'connect' for spark connect (default: local)",  # noqa: E501
    )


@pytest.fixture(scope="session", autouse=True)
def spark_driver_memory(request: Any) -> Any:
    """Fetching the value of spark_driver_memory parameter."""
    return request.config.getoption(name="--spark_driver_memory")


@pytest.fixture(scope="session", autouse=True)
def spark_mode(request: Any) -> Any:
    """Fetching the value of spark_mode parameter."""
    return request.config.getoption(name="--spark_mode")


@pytest.fixture(scope="session", autouse=True)
def prepare_exec_env(spark_driver_memory: str, spark_mode: str) -> None:
    """Prepare the execution environment before any test is executed."""
    # remove previous test lakehouse data
    LocalStorage.clean_folder(LAKEHOUSE)
    ExecEnv.set_default_engine_config("tests.configs")
    ExecEnvHelpers.prepare_exec_env(spark_driver_memory, spark_mode)
    ExecEnv.SESSION.sql(f"CREATE DATABASE IF NOT EXISTS test_db LOCATION '{LAKEHOUSE}'")


@pytest.fixture(autouse=True)
def before_each_test() -> Generator:
    """Reset default spark session configs."""
    yield
    ExecEnvHelpers.reset_default_spark_session_configs()


@pytest.fixture(scope="session", autouse=True)
def test_session_closure(request: Any) -> None:
    """Finalizing resources."""

    def finalizer() -> None:
        """Close spark session."""
        ExecEnv.SESSION.stop()

    request.addfinalizer(finalizer)


def pytest_collection_modifyitems(config: Any, items: list) -> None:
    """Automatically skip tests incompatible with current spark_mode."""
    spark_mode = config.getoption("--spark_mode")

    if spark_mode == "connect":
        skip_local = pytest.mark.skip(
            reason="Test requires local Spark mode (streaming/RDD operations)"
        )
        for item in items:
            if "local_only" in item.keywords:
                item.add_marker(skip_local)
                continue

    elif spark_mode == "local":
        skip_connect = pytest.mark.skip(reason="Test is Spark Connect only")
        for item in items:
            if "connect_only" in item.keywords:
                item.add_marker(skip_connect)
