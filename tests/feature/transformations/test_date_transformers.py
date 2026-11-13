"""Test Date Transformers."""

from datetime import date

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.errors import DateTimeException
from pyspark.sql.types import DateType

from lakehouse_engine.core.definitions import OutputFormat, TransformerSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.engine import load_data
from lakehouse_engine.transformers.transformer_factory import TransformerFactory
from lakehouse_engine.utils.configs.config_utils import ConfigUtils
from lakehouse_engine.utils.schema_utils import SchemaUtils
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_PATH = "transformations/date_transformers"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.mark.parametrize(
    ("source_format", "valid_date", "invalid_date"),
    [
        (None, "2024-02-29", "2024-02-30"),
        ("dd-MM-yyyy", "29-02-2024", "30-02-2024"),
    ],
)
@pytest.mark.parametrize("fail_safe", [True, False])
def test_convert_to_date_fail_safe(
    source_format: str | None, valid_date: str, invalid_date: str, fail_safe: bool
) -> None:
    """Malformed dates become null in fail-safe mode and raise otherwise."""
    spark = ExecEnv.SESSION

    source = spark.createDataFrame(
        [(valid_date, invalid_date), ("not a date", valid_date)],
        ["first_date", "second_date"],
    )
    transformer = TransformerFactory.get_transformer(
        TransformerSpec(
            function="convert_to_date",
            args={
                "cols": ["first_date", "second_date"],
                "source_format": source_format,
                "fail_safe": fail_safe,
            },
        )
    )
    result = source.transform(transformer)

    assert isinstance(result.schema["first_date"].dataType, DateType)
    assert isinstance(result.schema["second_date"].dataType, DateType)
    if fail_safe:
        assert set(result.collect()) == {
            (date(2024, 2, 29), None),
            (None, date(2024, 2, 29)),
        }
    else:
        with pytest.raises(
            (Py4JJavaError, DateTimeException),
            match="CAST_INVALID_INPUT|CANNOT_PARSE_TIMESTAMP",
        ):
            result.collect()


@pytest.mark.parametrize(
    "scenario",
    ["streaming"],
)
def test_date_transformers(scenario: str) -> None:
    """Test date transformers.

    Args:
        scenario: scenario to test.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/source/*.csv",
        f"{TEST_LAKEHOUSE_IN}/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/*schema.json",
        f"{TEST_LAKEHOUSE_IN}/",
    )
    acon = ConfigUtils.get_acon(f"file://{TEST_RESOURCES}/{scenario}.json")
    load_data(acon=acon)

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/control/*.csv",
        f"{TEST_LAKEHOUSE_CONTROL}/data/",
    )

    result_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_OUT}/{scenario}/data",
        file_format=OutputFormat.DELTAFILES.value,
    ).drop("curr_date")

    control_df = DataframeHelpers.read_from_file(
        f"{TEST_LAKEHOUSE_CONTROL}/data",
        schema=SchemaUtils.from_file_to_dict(
            f"file://{TEST_LAKEHOUSE_IN}/control_schema.json"
        ),
    )

    assert not DataframeHelpers.has_diff(result_df, control_df)
