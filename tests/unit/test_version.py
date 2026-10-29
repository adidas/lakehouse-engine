"""Test if the correct version of the lib is being read."""

import re

from lakehouse_engine.utils.configs.config_utils import ConfigUtils


def test_version() -> None:
    """Test if ConfigUtils is reading the correct version from pyproject.toml."""
    configUtils = ConfigUtils()

    with open("pyproject.toml", "r") as f:
        content = f.read()
        version_match = re.search(r"(?<=version = \").*(?=\")", content)
        assert version_match
        current_version = version_match.group()
        assert current_version == configUtils.get_engine_version()
