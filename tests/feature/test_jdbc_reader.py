"""Test jdbc reader."""
from typing import List

import pytest
from pyspark.sql.utils import IllegalArgumentException

from lakehouse_engine.engine import load_data
from lakehouse_engine.transformers.exceptions import WrongArgumentsException
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_NAME = "jdbc_reader"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_NAME}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_NAME}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_NAME}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_NAME}"
"""Same as spark, we provide two different ways to run jdbc reader.
    We can use the jdbc() function, passing inside all the arguments
        needed for Spark to work and we can even combine this with additional
        options passed trough .options().
    Other way is using .format("jdbc") and pass all necessary arguments
        through .options().
    It's important to say by choosing jdbc() we can also add options() to the execution.

JDBC Function Scenario - Description:
    correct_arguments - we are providing jdbc_args and options by passing arguments
        in a correct way.
    wrong_arguments - we are providing jdbc_args and options, but wrong arguments are
        filled to validate if spark reports the error messages properly.
JDBC Format Scenario - Description:
    correct_arguments - we are providing options to .format(jdbc) by passing arguments
        in a correct way.
    wrong_arguments - we are providing options to .format(jdbc), but wrong arguments are
        filled to validate if spark reports the error messages properly.
    predicates - predicates on spark read works on jdbc() function only, but if you
        mistake and pass to .format(jdbc) as a option, spark won't show any error, so
        we decided to add a validation and raise the error, this scenario validates it.
"""
TEST_SCENARIOS = [
    ["jdbc_function", "correct_arguments"],
    ["jdbc_function", "wrong_arguments"],
    ["jdbc_format", "correct_arguments"],
    ["jdbc_format", "wrong_arguments"],
    ["jdbc_format", "predicates"],
]


@pytest.mark.parametrize("scenario", TEST_SCENARIOS)
def test_jdbc_reader(scenario: List[str]) -> None:
    """Test loads from jdbc source.

    Args:
        scenario: scenario to test.
    """
    if scenario[0] == "jdbc_format" and scenario[1] == "wrong_arguments":
        with pytest.raises(IllegalArgumentException, match="Option.*is required."):
            load_data(
                f"file://{TEST_RESOURCES}/{scenario[0]}/{scenario[1]}/batch_init.json"
            )

    elif scenario[0] == "jdbc_format" and scenario[1] == "predicates":
        with pytest.raises(
            WrongArgumentsException, match="Predicates can only be used with jdbc_args."
        ):
            load_data(
                f"file://{TEST_RESOURCES}/{scenario[0]}/{scenario[1]}/batch_init.json"
            )

    elif scenario[0] == "jdbc_function" and scenario[1] == "wrong_arguments":
        with pytest.raises(
            TypeError, match=r"jdbc\(\) got an unexpected keyword argument.*"
        ):
            load_data(
                f"file://{TEST_RESOURCES}/{scenario[0]}/{scenario[1]}/batch_init.json"
            )
    else:
        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/{scenario[0]}/{scenario[1]}/data/source/part-01.csv",
            f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/{scenario[1]}/data/",
        )

        source_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_IN}/{scenario[0]}/{scenario[1]}/data"
        )
        DataframeHelpers.write_into_jdbc_table(
            source_df,
            f"jdbc:sqlite:{TEST_LAKEHOUSE_IN}/{scenario[0]}/{scenario[1]}/tests.db",
            f"{scenario[0]}",
        )

        load_data(
            f"file://{TEST_RESOURCES}/{scenario[0]}/{scenario[1]}/batch_init.json"
        )

        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/{scenario[0]}/{scenario[1]}/data/control/part-01.csv",
            f"{TEST_LAKEHOUSE_CONTROL}/{scenario[0]}/{scenario[1]}/data/",
        )

        result_df = DataframeHelpers.read_from_table(f"test_db.{scenario[0]}_table")
        control_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_CONTROL}/{scenario[0]}/{scenario[1]}/data"
        )

        assert not DataframeHelpers.has_diff(result_df, control_df)
