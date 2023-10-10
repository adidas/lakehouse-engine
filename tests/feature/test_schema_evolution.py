"""Test schema evolution on delta loads."""

from typing import Generator

import pytest
from pyspark.sql.utils import AnalysisException

from lakehouse_engine.core.definitions import InputFormat
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.engine import load_data
from lakehouse_engine.utils.schema_utils import SchemaUtils
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.dataframe_helpers import DataframeHelpers
from tests.utils.local_storage import LocalStorage

TEST_PATH = "schema_evolution"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"


@pytest.fixture(autouse=True)
def prepare_tests() -> Generator:
    """Run setup and cleanup steps before/after each test scenario."""
    # Test setup
    yield
    # Test cleanup
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_IN}")
    LocalStorage.clean_folder(f"{TEST_LAKEHOUSE_OUT}")


@pytest.mark.parametrize(
    "scenario",
    [
        [
            "auto_merge_enabled_add_column",
            "part-02",
            "batch_delta_enabled",
            "control_schema_add_column",
        ],
        [
            "auto_merge_disabled_add_column",
            "part-02",
            "batch_delta_disabled",
            "control_schema_add_column",
        ],
        [
            "auto_merge_enabled_remove_column",
            "part-03",
            "batch_delta_enabled",
            "control_schema",
        ],
        [
            "auto_merge_disabled_remove_column",
            "part-03",
            "batch_delta_disabled",
            "control_schema",
            "customer",
        ],
        [
            "auto_merge_enabled_cast_column",
            "part-04",
            "batch_delta_enabled",
            "control_schema",
        ],
        [
            "auto_merge_disabled_cast_column",
            "part-04",
            "batch_delta_disabled",
            "control_schema",
        ],
        [
            "auto_merge_enabled_rename_column_file",
            "part-05",
            "batch_delta_enabled",
            "control_schema_rename",
        ],
        [
            "auto_merge_disabled_rename_column_file",
            "part-05",
            "batch_delta_disabled",
            "control_schema_rename",
            "request",
        ],
        [
            "auto_merge_enabled_rename_column_transform",
            "part-06",
            "batch_delta_enabled",
            "control_schema",
        ],
        [
            "auto_merge_disabled_rename_column_transform",
            "part-06",
            "batch_delta_disabled_rename",
            "control_schema",
            "ARTICLE",
        ],
    ],
)
def test_schema_evolution_delta_load(scenario: str) -> None:
    """Test schema evolution on delta loads.

    Args:
        scenario: scenario to test.
        auto_merge_enabled_add_column - it performs the merge successfully and
        the new column is added to the schema (older rows assume null value
        for this column)
        auto_merge_disabled_add_column - it performs the merge successfully
        but the new column is ignored (is not added to the final schema).
        auto_merge_enabled_remove_column - it performs the merge successfully,
        the column is not removed from the final schema and the new rows assume
        the value null for this column.
        auto_merge_disabled_remove_column - purposely checks that the delta
        load fails when a column is removed.
        auto_merge_enabled_cast_column - it performs the merge successfully
        but the column type does not change automatically in the final schema.
        auto_merge_disabled_cast_column - it performs the merge successfully
        but the column type does not change automatically in the final schema.
        auto_merge_enabled_rename_column_file - it performs the merge
        successfully but assumes the renamed column as a new column (the
        column is renamed in the source schema only).
        auto_merge_disabled_rename_column_file - purposely checks that the
        delta load fails when a column is renamed (the column is renamed in
        the source schema only).
        auto_merge_enabled_rename_column_transform - it performs the merge
        successfully but ignores the renaming transformation specified in
        the acon.
        auto_merge_disabled_rename_column_transform - checks the behavior
        of the delta load when a column is renamed to lowercase,
        based on a transformation specified in the acon, without spark
        case-sensitive property.
    Scenario Properties:
        [scenario name, input file, acon file, control schema file,
        error message excerpt (optional)]
    """
    _create_table("schema_evolution_delta_load", "delta_load")

    # initial load
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/delta_load/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/delta_load/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/delta_load/schema/source/source_part-01_schema.json",
        f"{TEST_LAKEHOUSE_IN}/delta_load/",
    )
    load_data(
        f"file://{TEST_RESOURCES}/delta_load/batch_init_"
        f"{'enabled' if 'enabled' in scenario[0] else 'disabled'}.json"
    )

    initial_schema = DataframeHelpers.read_from_table(
        "test_db.schema_evolution_delta_load"
    ).schema

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/delta_load/data/source/{scenario[1]}.csv",
        f"{TEST_LAKEHOUSE_IN}/delta_load/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/delta_load/schema/source/source_{scenario[1]}_schema.json",
        f"{TEST_LAKEHOUSE_IN}/delta_load/source_delta_schema.json",
    )

    # tests with schema auto merge enabled
    if (
        "enabled" in scenario[0]
        or scenario[0] == "auto_merge_disabled_rename_column_transform"
    ):
        load_data(f"file://{TEST_RESOURCES}/delta_load/{scenario[2]}.json")

        result_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_OUT}/delta_load/data",
            file_format=InputFormat.DELTAFILES.value,
        )
        schema_after_merge = DataframeHelpers.read_from_table(
            "test_db.schema_evolution_delta_load"
        ).schema

        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/delta_load/data/control/{scenario[1]}.csv",
            f"{TEST_LAKEHOUSE_CONTROL}/delta_load/data/",
        )
        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/delta_load/schema/control/{scenario[3]}.json",
            f"{TEST_LAKEHOUSE_CONTROL}/delta_load/",
        )
        control_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_CONTROL}/delta_load/data/{scenario[1]}.csv",
            schema=SchemaUtils.from_file_to_dict(
                f"file://{TEST_LAKEHOUSE_CONTROL}/delta_load/{scenario[3]}.json"
            ),
        )

        # for the cast and rename tests, based on the transformations
        # specified in the acon file, the schema changes are ignored
        if scenario[0] == "auto_merge_enabled_cast_column" or scenario[0] == (
            "auto_merge_enabled_rename_column_transform"
        ):
            assert initial_schema == schema_after_merge
        else:
            assert not DataframeHelpers.has_diff(result_df, control_df)

    # tests with schema auto merge disabled
    elif "disabled" in scenario[0]:
        # for "add column" and "cast column" tests the merge runs successfully
        # but the schema changes are ignored
        if "add" in scenario[0] or "cast" in scenario[0]:
            load_data(f"file://{TEST_RESOURCES}/delta_load/{scenario[2]}.json")

            result_df = DataframeHelpers.read_from_file(
                f"{TEST_LAKEHOUSE_OUT}/delta_load/data",
                file_format=InputFormat.DELTAFILES.value,
            )

            if scenario[0] == "auto_merge_disabled_add_column":
                assert "new_column" not in result_df.columns
            else:
                assert not isinstance(result_df["code"], str)
        # for the removing column tests, the merge throws an error
        else:
            with pytest.raises(
                AnalysisException,
                match=f".*cannot resolve {scenario[4]} in UPDATE clause.*",
            ):
                load_data(f"file://{TEST_RESOURCES}/delta_load/{scenario[2]}.json")


@pytest.mark.parametrize(
    "scenario",
    [
        [
            "auto_merge_enabled_add_column",
            "part-02",
            "batch_append_enabled",
            "control_schema_add_column",
        ],
        [
            "auto_merge_disabled_add_column",
            "part-02",
            "batch_append_disabled",
            "control_schema_add_column",
            "A schema mismatch detected when writing to the Delta table",
        ],
        [
            "auto_merge_enabled_remove_column",
            "part-03",
            "batch_append_enabled",
            "control_schema",
        ],
        [
            "auto_merge_disabled_remove_column",
            "part-03",
            "batch_append_disabled",
            "control_schema",
        ],
        [
            "auto_merge_enabled_cast_column",
            "part-04",
            "batch_append_enabled_cast",
            "control_schema",
            "Failed to merge incompatible data types",
        ],
        [
            "auto_merge_disabled_cast_column",
            "part-04",
            "batch_append_disabled",
            "control_schema",
        ],
        [
            "auto_merge_enabled_rename_column_file",
            "part-05",
            "batch_append_enabled",
            "control_schema_rename",
        ],
        [
            "auto_merge_disabled_rename_column_file",
            "part-05",
            "batch_append_disabled",
            "control_schema_rename",
            "A schema mismatch detected",
        ],
        [
            "auto_merge_enabled_rename_column_transform",
            "part-06",
            "batch_append_enabled",
            "control_schema",
        ],
        [
            "auto_merge_disabled_rename_column_transform",
            "part-06",
            "batch_append_disabled",
            "control_schema",
        ],
    ],
)
def test_schema_evolution_append_load(scenario: str) -> None:
    """Test schema evolution on append loads.

    Args:
        scenario: scenario to test.
        auto_merge_enabled_add_column - it performs the append load successfully
        and the new column is added to the schema (older rows assume null value
        for this column)
        auto_merge_disabled_add_column - purposely checks that the append load
        fails when a new column is added.
        auto_merge_enabled_remove_column - it performs the append load
        successfully, the column is not removed from the final schema and the
        new rows assume the value null for this column.
        auto_merge_disabled_remove_column - it performs the append load
        successfully, the column is not removed from the final schema and the
        new rows assume the value null for this column.
        auto_merge_enabled_cast_column - purposely checks that the append load
        fails when a cast transformation is added to the acon file.
        auto_merge_disabled_cast_column - purposely checks that the append load
        fails when a cast transformation is added to the acon file.
        auto_merge_enabled_rename_column_file - purposely checks that the
        append load fails when a column is renamed (the column is renamed in
        the source schema only).
        auto_merge_disabled_rename_column_file - purposely checks that the
        append load fails when a column is renamed (the column is renamed in
        the source schema only).
        auto_merge_enabled_rename_column_transform - it performs the append load
        successfully but ignores the renaming transformation specified in
        the acon.
        auto_merge_disabled_rename_column_transform - it performs the append load
        successfully but ignores the renaming transformation specified in
        the acon.
    Scenario Properties:
        [scenario name, input file, acon file, control schema file,
        error message excerpt (optional)]
    """
    _create_table("schema_evolution_append_load", "append_load")

    # initial load
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/append_load/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/append_load/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/append_load/schema/source/source_part-01_schema.json",
        f"{TEST_LAKEHOUSE_IN}/append_load/",
    )
    load_data(
        f"file://{TEST_RESOURCES}/append_load/batch_init_"
        f"{'enabled' if 'enabled' in scenario[0] else 'disabled'}.json"
    )

    initial_schema = DataframeHelpers.read_from_table(
        "test_db.schema_evolution_append_load"
    ).schema

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/append_load/data/source/{scenario[1]}.csv",
        f"{TEST_LAKEHOUSE_IN}/append_load/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/append_load/schema/source/source_{scenario[1]}_schema.json",
        f"{TEST_LAKEHOUSE_IN}/append_load/source_append_schema.json",
    )

    # tests with schema auto merge enabled
    if "enabled" in scenario[0]:
        # for the cast column test, the append throws an error
        if "cast" in scenario[0]:
            with pytest.raises(AnalysisException, match=f".*{scenario[4]}*"):
                load_data(f"file://{TEST_RESOURCES}/append_load/{scenario[2]}.json")
        else:
            load_data(f"file://{TEST_RESOURCES}/append_load/{scenario[2]}.json")

            result_df = DataframeHelpers.read_from_file(
                f"{TEST_LAKEHOUSE_OUT}/append_load/data",
                file_format=InputFormat.DELTAFILES.value,
            )
            schema_after_append = DataframeHelpers.read_from_table(
                "test_db.schema_evolution_append_load"
            ).schema

            LocalStorage.copy_file(
                f"{TEST_RESOURCES}/append_load/data/control/{scenario[1]}.csv",
                f"{TEST_LAKEHOUSE_CONTROL}/append_load/data/",
            )
            LocalStorage.copy_file(
                f"{TEST_RESOURCES}/append_load/schema/control/{scenario[3]}.json",
                f"{TEST_LAKEHOUSE_CONTROL}/append_load/",
            )
            control_df = DataframeHelpers.read_from_file(
                f"{TEST_LAKEHOUSE_CONTROL}/append_load/data/{scenario[1]}.csv",
                schema=SchemaUtils.from_file_to_dict(
                    f"file://{TEST_LAKEHOUSE_CONTROL}/append_load/{scenario[3]}.json"
                ),
            )

            # for rename test, based on the transformation specified in the
            # acon file, the schema change is ignored
            if scenario[0] == "auto_merge_enabled_rename_column_transform":
                assert initial_schema == schema_after_append
            else:
                assert not DataframeHelpers.has_diff(result_df, control_df)

    # tests with schema auto merge disabled
    elif "disabled" in scenario[0]:
        # for the renaming or adding column tests, the append throws an error
        if "rename_column_file" in scenario[0] or "add" in scenario[0]:
            with pytest.raises(AnalysisException, match=f".*{scenario[4]}*"):
                load_data(f"file://{TEST_RESOURCES}/append_load/{scenario[2]}.json")
        else:
            load_data(f"file://{TEST_RESOURCES}/append_load/{scenario[2]}.json")

            result_df = DataframeHelpers.read_from_file(
                f"{TEST_LAKEHOUSE_OUT}/append_load/data",
                file_format=InputFormat.DELTAFILES.value,
            )

            schema_after_append = DataframeHelpers.read_from_table(
                "test_db.schema_evolution_append_load"
            ).schema

            assert initial_schema == schema_after_append


@pytest.mark.parametrize(
    "scenario",
    [
        [
            "auto_merge_enabled",
            "part-02",
            "batch_merge_enabled",
            "control_schema_merge_enabled",
        ],
        [
            "auto_merge_disabled",
            "part-02",
            "batch_merge_disabled",
            "",
            "Failed to merge",
        ],
        [
            "overwrite_schema",
            "part-02",
            "batch_overwrite",
            "control_schema_overwrite",
        ],
    ],
)
def test_schema_evolution_full_load(scenario: str) -> None:
    """Test schema evolution on full loads.

    Args:
        scenario: scenario to test.
        auto_merge_enabled - overwrites the data in the table but does not
        overwrite the schema (assumes the new column, keeps the removed
        column, ignores renaming and cast transformations)
        auto_merge_disabled - throws a mismatch schema error.
        overwrite_schema - overwrites the data and the schema of the table.
    Scenario Properties:
        [scenario name, input file, acon file, control schema file,
        error message excerpt (optional)]
    """
    _create_table("schema_evolution_full_load", "full_load")

    # initial load
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/full_load/data/source/part-01.csv",
        f"{TEST_LAKEHOUSE_IN}/full_load/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/full_load/schema/source/source_part-01_schema.json",
        f"{TEST_LAKEHOUSE_IN}/full_load/source_schema.json",
    )
    load_data(f"file://{TEST_RESOURCES}/full_load/batch_init.json")

    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/full_load/data/source/{scenario[1]}.csv",
        f"{TEST_LAKEHOUSE_IN}/full_load/data/",
    )
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/full_load/schema/source/source_{scenario[1]}_schema.json",
        f"{TEST_LAKEHOUSE_IN}/full_load/source_schema.json",
    )

    if scenario[0] == "auto_merge_disabled":
        with pytest.raises(AnalysisException, match=f".*{scenario[4]}*"):
            load_data(f"file://{TEST_RESOURCES}/full_load/{scenario[2]}.json")
    else:
        load_data(f"file://{TEST_RESOURCES}/full_load/{scenario[2]}.json")

        final_schema = SchemaUtils.from_table_schema(
            "test_db.schema_evolution_full_load"
        )

        result_df = DataframeHelpers.read_from_file(
            f"{TEST_LAKEHOUSE_OUT}/full_load/data",
            file_format=InputFormat.DELTAFILES.value,
        )
        LocalStorage.copy_file(
            f"{TEST_RESOURCES}/full_load/schema/control/{scenario[3]}.json",
            f"{TEST_LAKEHOUSE_CONTROL}/full_load/",
        )

        control_schema = SchemaUtils.from_file(
            f"file://{TEST_LAKEHOUSE_CONTROL}/full_load/{scenario[3]}.json"
        )

        assert final_schema == control_schema
        # with the rename transformation specified in acon, both the original
        # and the renamed field (ARTICLE and article) are not considered in
        # the final schema
        assert ("article", "ARTICLE") not in result_df.columns


def _create_table(table_name: str, location: str) -> None:
    """Create test table."""
    ExecEnv.SESSION.sql(f"DROP TABLE IF EXISTS test_db.{table_name}")
    ExecEnv.SESSION.sql(
        f"""
        CREATE TABLE IF NOT EXISTS test_db.{table_name} (
            actrequest_timestamp string,
            request string,
            datapakid int,
            partno int,
            record int,
            salesorder int,
            item int,
            recordmode string,
            date int,
            customer string,
            ARTICLE string,
            amount int,
            code int
        )
        USING delta
        LOCATION '{TEST_LAKEHOUSE_OUT}/{location}/data'
        """
    )
