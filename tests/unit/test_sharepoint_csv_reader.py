"""Test Sharepoint CSV reader.

Unit tests for delimiter detection and Spark CSV option resolution in
`SharepointCsvReader`.
"""

from __future__ import annotations

from typing import Any, Dict, cast

from lakehouse_engine.io.readers.sharepoint_reader import SharepointCsvReader


class DummySharepointOptions:
    """Minimal Sharepoint options stub used to build a `SharepointCsvReader`.

    Args:
        local_options: Dictionary of local CSV read options (for example, header,
            delimiter, sep).
    """

    def __init__(self, local_options: Dict[str, Any]) -> None:
        """Initialize the dummy options with the provided local options."""
        self.local_options = local_options


class DummyInputSpec:
    """Minimal input spec stub that exposes `sharepoint_opts` as expected by the reader.

    Args:
        sharepoint_options: Instance containing `local_options`.
    """

    def __init__(self, sharepoint_options: DummySharepointOptions) -> None:
        """Initialize the dummy input spec with the provided Sharepoint options."""
        self.sharepoint_opts = sharepoint_options


def create_csv_reader(local_options: Dict[str, Any]) -> SharepointCsvReader:
    """Create a `SharepointCsvReader` instance without calling its constructor.

    Args:
        local_options: Dictionary of local CSV read options.

    Returns:
        SharepointCsvReader: A partially-initialized reader instance.
    """
    csv_reader: SharepointCsvReader = SharepointCsvReader.__new__(SharepointCsvReader)
    csv_reader._input_spec = cast(
        Any, DummyInputSpec(DummySharepointOptions(local_options))
    )
    return csv_reader


def test_detect_delimiter_uses_user_provided_delimiter() -> None:
    """It should always return the explicitly provided delimiter."""
    csv_reader: SharepointCsvReader = SharepointCsvReader.__new__(SharepointCsvReader)
    detected: str = csv_reader.detect_delimiter(
        file_content=b"column_a;column_b\n1;2\n",
        provided_delimiter="|",
        expected_columns=None,
    )
    assert detected == "|"


def test_detect_delimiter_autodetects_semicolon() -> None:
    """It should infer the delimiter from the file content when none is provided."""
    csv_reader: SharepointCsvReader = SharepointCsvReader.__new__(SharepointCsvReader)
    detected: str = csv_reader.detect_delimiter(
        file_content=b"column_a;column_b\n1;2\n",
        provided_delimiter=None,
        expected_columns=None,
    )
    assert detected == ";"


def test_detect_delimiter_defaults_to_comma_on_decode_error() -> None:
    """It should fall back to comma when content cannot be decoded for sniffing."""
    csv_reader: SharepointCsvReader = SharepointCsvReader.__new__(SharepointCsvReader)
    detected: str = csv_reader.detect_delimiter(
        file_content=b"\xff\xfe",
        provided_delimiter=None,
        expected_columns=None,
    )
    assert detected == ","


def test_resolve_csv_options_prefers_sep_over_delimiter() -> None:
    """`sep` should take precedence over `delimiter`, and `delimiter` should be removed.

    Args:
        None
    Returns:
        None
    """
    csv_reader: SharepointCsvReader = create_csv_reader(
        {"sep": "|", "delimiter": ",", "header": True}
    )
    spark_options: Dict[str, Any] = csv_reader.resolve_spark_csv_options(
        b"column_a,column_b\n1,2\n"
    )
    assert spark_options["sep"] == "|"
    assert "delimiter" not in spark_options


def test_resolve_spark_csv_options_uses_delimiter_when_sep_missing() -> None:
    """If `sep` is missing, `delimiter` should be mapped into `sep` and removed."""
    csv_reader: SharepointCsvReader = create_csv_reader(
        {"delimiter": ";", "header": True}
    )
    spark_options: Dict[str, Any] = csv_reader.resolve_spark_csv_options(
        b"column_a,column_b\n1,2\n"
    )
    assert spark_options["sep"] == ";"
    assert "delimiter" not in spark_options


def test_resolve_spark_csv_options_autodetects_when_no_delimiter_provided() -> None:
    """If neither `sep` nor `delimiter` is provided, it should autodetect from content.

    Args:
        None
    Returns:
        None
    """
    csv_reader: SharepointCsvReader = create_csv_reader({"header": True})
    spark_options: Dict[str, Any] = csv_reader.resolve_spark_csv_options(
        b"column_a|column_b\n1|2\n"
    )
    assert spark_options["sep"] == "|"


def test_resolve_spark_csv_options_warns_when_expected_columns_names_mismatch(
    caplog: Any,
) -> None:
    """Warn when expected column names do not match the header.

    Args:
        caplog: Pytest log capture fixture.

    Returns:
        None.
    """
    csv_reader: SharepointCsvReader = create_csv_reader(
        {
            "header": True,
            "expected_columns": ["col_a", "col_b"],
        }
    )

    # Header uses semicolon, delimiter should be detected as ';', but names mismatch.
    file_content: bytes = b"wrong_a;wrong_b\n1;2\n"

    with caplog.at_level("WARNING"):
        csv_reader.resolve_spark_csv_options(file_content)

    assert "Expected columns don't match CSV header" in caplog.text


def test_resolve_spark_csv_options_warns_when_expected_columns_validation_fails(
    caplog: Any,
) -> None:
    """Warn when validation against the header cannot be performed.

    Args:
        caplog: Pytest log capture fixture.

    Returns:
        None.
    """
    csv_reader: SharepointCsvReader = create_csv_reader(
        {
            "header": True,
            "expected_columns": ["col_a", "col_b"],
        }
    )

    # Force decode failure inside the expected_columns validation block.
    file_content: bytes = b"\xff\xfe"

    with caplog.at_level("WARNING"):
        csv_reader.resolve_spark_csv_options(file_content)

    assert "Failed to validate expected_columns against CSV header" in caplog.text
