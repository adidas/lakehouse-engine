"""Unit tests for overwritten the default configs."""

from lakehouse_engine.core import exec_env
from lakehouse_engine.utils.logging_handler import LoggingHandler
from tests.conftest import UNIT_RESOURCES

LOGGER = LoggingHandler(__name__).get_logger()

TEST_PATH = "custom_configs"
TEST_RESOURCES = f"{UNIT_RESOURCES}/{TEST_PATH}"


def test_custom_config() -> None:
    """Testing using a custom configuration."""
    default_configs = exec_env.ExecEnv.ENGINE_CONFIG.notif_disallowed_email_servers
    LOGGER.info(f"Default disallowed email server: {default_configs}")

    # Testing custom configurations using a dictionary
    exec_env.ExecEnv.set_default_engine_config(
        custom_configs_dict={"notif_disallowed_email_servers": ["dummy.server.test"]},
    )
    dict_custom_configs = exec_env.ExecEnv.ENGINE_CONFIG.notif_disallowed_email_servers
    LOGGER.info(
        f"Custom disallowed email server using dictionary: {dict_custom_configs}"
    )
    assert default_configs != dict_custom_configs

    # Testing custom configurations using a file
    exec_env.ExecEnv.set_default_engine_config(
        custom_configs_file_path=f"{TEST_RESOURCES}/custom_engine_config.yaml",
    )
    file_custom_configs = exec_env.ExecEnv.ENGINE_CONFIG.notif_disallowed_email_servers
    LOGGER.info(
        f"Custom disallowed email server using configuration file: "
        f"{file_custom_configs}"
    )
    assert default_configs != file_custom_configs

    # Resetting to the default configurations
    exec_env.ExecEnv.set_default_engine_config(package="tests.configs")
    reset_configs = exec_env.ExecEnv.ENGINE_CONFIG.notif_disallowed_email_servers
    LOGGER.info(f"Reset disallowed email server: {reset_configs}")
    assert default_configs == reset_configs
