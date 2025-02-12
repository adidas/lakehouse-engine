"""Module to configure the test environment."""

from typing import Any, Generator

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


def pytest_addoption(parser: Any) -> Any:
    """Setting extra options for pytest command."""
    parser.addoption(
        "--spark_driver_memory",
        action="store",
        help="memory limit for the spark driver (default 2g)",
    )


@pytest.fixture(scope="session", autouse=True)
def spark_driver_memory(request: Any) -> Any:
    """Fetching the value of spark_driver_memory parameter."""
    return request.config.getoption(name="--spark_driver_memory")


@pytest.fixture(scope="session", autouse=True)
def prepare_exec_env(spark_driver_memory: str) -> None:
    """Prepare the execution environment before any test is executed."""
    # remove previous test lakehouse data
    LocalStorage.clean_folder(LAKEHOUSE)
    ExecEnv.set_default_engine_config("tests.configs")
    ExecEnvHelpers.prepare_exec_env(spark_driver_memory)
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
