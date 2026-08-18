"""Tests for Sharepoint reader options validation."""

import tempfile
from typing import Any

import pytest

from lakehouse_engine.core.definitions import SharepointOptions


def _build_sharepoint_options(**overrides: Any) -> SharepointOptions:
    """Create baseline SharepointOptions for reader validation tests."""
    base: dict[str, Any] = {
        "client_id": "client",
        "tenant_id": "tenant",
        "site_name": "site",
        "drive_name": "drive",
        "secret": "secret",
        "local_path": f"{tempfile.gettempdir()}/sp/",
        "folder_relative_path": "incoming",
    }
    base.update(overrides)
    return SharepointOptions(**base)


def test_skip_rename_requires_archive_disabled() -> None:
    """It should reject skip_rename when archive move is enabled."""
    opts = _build_sharepoint_options(skip_rename=True, archive_enabled=True)

    with pytest.raises(
        ValueError,
        match=r"`skip_rename=True` is only supported when `archive_enabled=False`.",
    ):
        opts.validate_for_reader()


def test_skip_rename_allowed_when_archive_disabled() -> None:
    """It should allow skip_rename when archive move is disabled."""
    opts = _build_sharepoint_options(skip_rename=True, archive_enabled=False)

    opts.validate_for_reader()
