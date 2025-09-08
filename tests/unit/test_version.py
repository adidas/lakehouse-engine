"""Test if the correct version of the lib is being read."""

import re

from lakehouse_engine.utils.configs.config_utils import ConfigUtils


def test_version() -> None:
    """Test if ConfigUtils is reading the correct version from pyproject.toml."""
    configUtils = ConfigUtils()

    current_version = re.search(
        r"(?<=version = \").*(?=\")", open("pyproject.toml").read()
    ).group()
    assert current_version == configUtils.get_engine_version()
