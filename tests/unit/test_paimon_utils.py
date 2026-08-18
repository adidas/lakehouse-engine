"""Unit tests for PaimonUtils."""

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import types as T

from lakehouse_engine.core.definitions import OutputFormat, OutputSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.databricks_utils import DatabricksUtils
from lakehouse_engine.utils.paimon_utils import PaimonUtils


def _make_output_spec(**overrides: Any) -> OutputSpec:
    """Build a minimal OutputSpec for tests, overriding selected attributes."""
    defaults: dict = {
        "spec_id": "out_paimon",
        "input_id": "in_paimon",
        "write_type": "overwrite",
        "data_format": OutputFormat.PAIMON.value,
        "location": "/paimon/test_table",
    }
    defaults.update(overrides)
    return OutputSpec(**defaults)


@pytest.fixture(scope="module")
def sample_df() -> Any:
    """Provide a small DataFrame with a representative schema."""
    schema = T.StructType(
        [
            T.StructField("id", T.LongType(), False),
            T.StructField("name", T.StringType(), True),
            T.StructField("amount", T.DecimalType(10, 2), True),
            T.StructField(
                "tags",
                T.ArrayType(T.StringType(), containsNull=False),
                True,
            ),
            T.StructField(
                "attrs",
                T.MapType(T.StringType(), T.IntegerType(), valueContainsNull=True),
                True,
            ),
            T.StructField(
                "nested",
                T.StructType(
                    [
                        T.StructField("inner_a", T.IntegerType(), True),
                        T.StructField("inner_b", T.BooleanType(), False),
                    ]
                ),
                True,
            ),
        ]
    )
    return ExecEnv.SESSION.createDataFrame([], schema)


# ---------------------------------------------------------------------------
# Type conversions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spark_type, nullable, expected",
    [
        (T.BooleanType(), True, "BOOLEAN"),
        (T.IntegerType(), True, "INT"),
        (T.LongType(), False, "BIGINT NOT NULL"),
        (T.StringType(), True, "STRING"),
        (T.BinaryType(), True, "BYTES"),
        (T.DateType(), True, "DATE"),
        (T.TimestampType(), True, "TIMESTAMP(6) WITH LOCAL TIME ZONE"),
        (T.DecimalType(12, 4), True, "DECIMAL(12, 4)"),
        (T.FloatType(), False, "FLOAT NOT NULL"),
        (T.DoubleType(), True, "DOUBLE"),
        (T.ByteType(), True, "TINYINT"),
        (T.ShortType(), True, "SMALLINT"),
    ],
)
def test_spark_type_to_paimon_simple(
    spark_type: T.DataType, nullable: bool, expected: str
) -> None:
    """Each simple Spark type is mapped to its Paimon literal."""
    assert PaimonUtils.spark_type_to_paimon(spark_type, nullable) == expected


def test_spark_type_to_paimon_timestamp_ntz() -> None:
    """Type TimestampNTZType is converted to TIMESTAMP(6) without LTZ suffix."""
    timestamp_ntz = getattr(T, "TimestampNTZType", None)
    if timestamp_ntz is None:
        pytest.skip("TimestampNTZType not available in this Spark version.")
    assert PaimonUtils.spark_type_to_paimon(timestamp_ntz(), True) == "TIMESTAMP(6)"


def test_spark_type_to_paimon_array_map_struct() -> None:
    """Complex Spark types are rendered with nested nullability."""
    array_type = T.ArrayType(T.StringType(), containsNull=False)
    assert (
        PaimonUtils.spark_type_to_paimon(array_type, True) == "ARRAY<STRING NOT NULL>"
    )

    map_type = T.MapType(T.StringType(), T.IntegerType(), valueContainsNull=False)
    assert (
        PaimonUtils.spark_type_to_paimon(map_type, True)
        == "MAP<STRING NOT NULL, INT NOT NULL>"
    )

    struct_type = T.StructType(
        [
            T.StructField("a", T.IntegerType(), True),
            T.StructField("b", T.StringType(), False),
        ]
    )
    assert (
        PaimonUtils.spark_type_to_paimon(struct_type, True)
        == "ROW<`a` INT, `b` STRING NOT NULL>"
    )


def test_spark_type_to_paimon_unsupported_type() -> None:
    """Unsupported Spark types raise NotImplementedError."""

    class _UnknownType(T.DataType):
        pass

    with pytest.raises(NotImplementedError):
        PaimonUtils.spark_type_to_paimon(_UnknownType(), True)


# ---------------------------------------------------------------------------
# Schema building
# ---------------------------------------------------------------------------


def test_build_schema_json_basic(sample_df: Any) -> None:
    """Schema JSON contains all expected metadata and field ids."""
    output_spec = _make_output_spec(
        partitions=["name"],
        options={"paimon_options": {"primary-key": ["id"], "bucket": 4}},
    )
    schema = json.loads(PaimonUtils.build_schema_json(sample_df, output_spec))

    assert schema["version"] == PaimonUtils.SCHEMA_VERSION
    assert schema["id"] == 0
    assert schema["partitionKeys"] == ["name"]
    assert schema["primaryKeys"] == ["id"]
    # primary-key/partition keys should be popped from options
    assert schema["options"] == {"bucket": "4"}
    assert schema["comment"] == ""
    assert schema["highestFieldId"] == len(sample_df.schema.fields) - 1
    assert [f["name"] for f in schema["fields"]] == [
        sf.name for sf in sample_df.schema.fields
    ]
    assert [f["id"] for f in schema["fields"]] == list(
        range(len(sample_df.schema.fields))
    )
    # spot-check that nullability is encoded on the field types
    id_field = next(f for f in schema["fields"] if f["name"] == "id")
    assert id_field["type"] == "BIGINT NOT NULL"


def test_build_schema_json_partition_option_overrides_spec(sample_df: Any) -> None:
    """An explicit ``partition`` option takes precedence over output_spec.partitions."""
    output_spec = _make_output_spec(
        partitions=["name"],
        options={"paimon_options": {"partition": "id,name"}},
    )
    schema = json.loads(PaimonUtils.build_schema_json(sample_df, output_spec))
    assert schema["partitionKeys"] == ["id", "name"]
    assert schema["primaryKeys"] == []
    assert schema["options"] == {}


def test_build_schema_json_no_options_no_partitions(sample_df: Any) -> None:
    """When options/partitions are absent, defaults are empty collections."""
    schema = json.loads(PaimonUtils.build_schema_json(sample_df, _make_output_spec()))
    assert schema["partitionKeys"] == []
    assert schema["primaryKeys"] == []
    assert schema["options"] == {}


# ---------------------------------------------------------------------------
# Helpers around option parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, []),
        ("", []),
        ("id", ["id"]),
        ("id, name ,age", ["id", "name", "age"]),
        (["id", " name "], ["id", "name"]),
        (("id", "name", ""), ["id", "name"]),
    ],
)
def test_extract_string_list(value: Any, expected: list) -> None:
    """String/list/tuple inputs are normalized into a clean list of strings."""
    assert PaimonUtils._extract_string_list(value) == expected


# ---------------------------------------------------------------------------
# ensure_table_exists orchestration
# ---------------------------------------------------------------------------


def test_ensure_table_exists_uses_direct_write(sample_df: Any) -> None:
    """When direct schema writing succeeds, flow finishes without errors."""
    spec = _make_output_spec(location="/paimon/table/")
    with patch.object(
        PaimonUtils, "_write_schema_file", return_value=True
    ) as write_mock:
        PaimonUtils.ensure_table_exists(MagicMock(), sample_df, spec)
    # trailing slash must be stripped before being passed downstream
    args, _ = write_mock.call_args
    assert args[-1] == "/paimon/table"


def test_ensure_table_exists_propagates_direct_write_error(sample_df: Any) -> None:
    """When direct write fails, the error is propagated."""
    spec = _make_output_spec()
    with patch.object(
        PaimonUtils, "_write_schema_file", side_effect=RuntimeError("nope")
    ):
        with pytest.raises(RuntimeError, match="nope"):
            PaimonUtils.ensure_table_exists(MagicMock(), sample_df, spec)


def test_ensure_table_exists_raises_without_direct_file_access(
    sample_df: Any,
) -> None:
    """When dbutils is unavailable, flow raises instead of falling back to SQL."""
    spec = _make_output_spec()
    with patch.object(PaimonUtils, "_write_schema_file", return_value=False):
        with pytest.raises(RuntimeError, match="requires direct file access"):
            PaimonUtils.ensure_table_exists(MagicMock(), sample_df, spec)


# ---------------------------------------------------------------------------
# _write_schema_file behaviour
# ---------------------------------------------------------------------------


def test_write_schema_file_returns_false_when_dbutils_missing(sample_df: Any) -> None:
    """If dbutils cannot be obtained, the method returns False without raising."""
    spec = _make_output_spec()
    with patch(
        "lakehouse_engine.utils.paimon_utils.DatabricksUtils.get_db_utils",
        side_effect=Exception("no dbutils"),
    ):
        assert (
            PaimonUtils._write_schema_file(
                MagicMock(), sample_df, spec, "/paimon/table"
            )
            is False
        )


def test_write_schema_file_skips_when_latest_schema_is_equal(sample_df: Any) -> None:
    """If latest schema already matches the input, no new version is created."""
    spec = _make_output_spec()
    dbutils = MagicMock()
    schema_entry = MagicMock()
    schema_entry.name = "schema-2"
    schema_entry.path = "/paimon/table/schema/schema-2"
    existing_schema = PaimonUtils.build_schema_dict(sample_df, spec)
    dbutils.fs.ls.return_value = [schema_entry]
    dbutils.fs.head.return_value = json.dumps(existing_schema)

    with patch(
        "lakehouse_engine.utils.paimon_utils.DatabricksUtils.get_db_utils",
        return_value=dbutils,
    ):
        result = PaimonUtils._write_schema_file(
            MagicMock(), sample_df, spec, "/paimon/table"
        )
    assert result is True
    dbutils.fs.put.assert_not_called()


def test_write_schema_file_writes_schema_zero_when_missing(sample_df: Any) -> None:
    """If schema directory is missing, schema-0 is written."""
    spec = _make_output_spec(options={"paimon_options": {"primary-key": "id"}})
    dbutils = MagicMock()
    dbutils.fs.ls.side_effect = Exception("not found")
    with patch(
        "lakehouse_engine.utils.paimon_utils.DatabricksUtils.get_db_utils",
        return_value=dbutils,
    ):
        result = PaimonUtils._write_schema_file(
            MagicMock(), sample_df, spec, "/paimon/table"
        )
    assert result is True
    dbutils.fs.put.assert_called_once()
    call_args = dbutils.fs.put.call_args[0]
    assert call_args[0] == "/paimon/table/schema/schema-0"
    payload = json.loads(call_args[1])
    assert payload["primaryKeys"] == ["id"]
    assert call_args[2] is False


def test_write_schema_file_writes_next_version_when_schema_changes(
    sample_df: Any,
) -> None:
    """When schema changes are detected, next schema version is written."""
    spec = _make_output_spec()
    old_df = sample_df.select("id", "name")
    existing_schema = PaimonUtils.build_schema_dict(old_df, spec)

    dbutils = MagicMock()
    schema_entry = MagicMock()
    schema_entry.name = "schema-4"
    schema_entry.path = "/paimon/table/schema/schema-4"
    dbutils.fs.ls.return_value = [schema_entry]
    dbutils.fs.head.return_value = json.dumps(existing_schema)
    with patch(
        "lakehouse_engine.utils.paimon_utils.DatabricksUtils.get_db_utils",
        return_value=dbutils,
    ):
        result = PaimonUtils._write_schema_file(
            MagicMock(), sample_df, spec, "/paimon/table"
        )
    assert result is True
    dbutils.fs.put.assert_called_once()
    assert dbutils.fs.put.call_args[0][0] == "/paimon/table/schema/schema-5"


def test_write_schema_file_handles_concurrent_creation(sample_df: Any) -> None:
    """If put fails but matching schema was created concurrently, it succeeds."""
    spec = _make_output_spec()
    dbutils = MagicMock()
    desired_schema = PaimonUtils.build_schema_dict(sample_df, spec)
    existing_schema = PaimonUtils.build_schema_dict(sample_df.select("id"), spec)

    with (
        patch(
            "lakehouse_engine.utils.paimon_utils.DatabricksUtils.get_db_utils",
            return_value=dbutils,
        ),
        patch.object(
            PaimonUtils,
            "_get_latest_schema_version",
            return_value=(1, "/paimon/table/schema/schema-1"),
        ),
        patch.object(
            PaimonUtils,
            "_read_schema_file",
            side_effect=[existing_schema, existing_schema, desired_schema],
        ),
        patch.object(DatabricksUtils, "check_dbutils_path_exists", return_value=True),
    ):
        dbutils.fs.put.side_effect = Exception("conflict")
        result = PaimonUtils._write_schema_file(
            MagicMock(), sample_df, spec, "/paimon/table"
        )
    assert result is True


def test_write_schema_file_raises_when_put_fails_and_file_absent(
    sample_df: Any,
) -> None:
    """If put fails and the file is still absent, the original error propagates."""
    spec = _make_output_spec()
    dbutils = MagicMock()
    dbutils.fs.ls.side_effect = Exception("not found")
    dbutils.fs.put.side_effect = RuntimeError("boom")
    with (
        patch(
            "lakehouse_engine.utils.paimon_utils.DatabricksUtils.get_db_utils",
            return_value=dbutils,
        ),
        patch.object(DatabricksUtils, "check_dbutils_path_exists", return_value=False),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            PaimonUtils._write_schema_file(
                MagicMock(), sample_df, spec, "/paimon/table"
            )


def test_write_schema_file_raises_when_column_type_changes(sample_df: Any) -> None:
    """Changing existing column type is rejected."""
    spec = _make_output_spec()
    dbutils = MagicMock()
    schema_entry = MagicMock()
    schema_entry.name = "schema-0"
    schema_entry.path = "/paimon/table/schema/schema-0"
    existing_schema = PaimonUtils.build_schema_dict(sample_df, spec)
    for field in existing_schema["fields"]:
        if field["name"] == "name":
            field["type"] = "INT"
            break
    dbutils.fs.ls.return_value = [schema_entry]
    dbutils.fs.head.return_value = json.dumps(existing_schema)

    with patch(
        "lakehouse_engine.utils.paimon_utils.DatabricksUtils.get_db_utils",
        return_value=dbutils,
    ):
        with pytest.raises(ValueError, match="changing existing column type"):
            PaimonUtils._write_schema_file(
                MagicMock(), sample_df, spec, "/paimon/table"
            )


def test_write_schema_file_preserves_missing_existing_columns(sample_df: Any) -> None:
    """Columns absent from incoming DataFrame are preserved from existing schema."""
    spec = _make_output_spec()
    dbutils = MagicMock()
    schema_entry = MagicMock()
    schema_entry.name = "schema-0"
    schema_entry.path = "/paimon/table/schema/schema-0"
    existing_schema = PaimonUtils.build_schema_dict(sample_df, spec)
    existing_schema["fields"].append(
        {"id": 999, "name": "legacy_column", "type": "STRING"}
    )
    existing_schema["highestFieldId"] = 999
    dbutils.fs.ls.return_value = [schema_entry]
    dbutils.fs.head.return_value = json.dumps(existing_schema)

    with patch(
        "lakehouse_engine.utils.paimon_utils.DatabricksUtils.get_db_utils",
        return_value=dbutils,
    ):
        result = PaimonUtils._write_schema_file(
            MagicMock(), sample_df, spec, "/paimon/table"
        )
    assert result is True
    dbutils.fs.put.assert_not_called()


def test_write_schema_file_relaxes_new_required_field_from_latest_schema() -> None:
    """A required field added in latest version is relaxed to nullable."""
    spec = _make_output_spec()
    evolving_df = ExecEnv.SESSION.createDataFrame(
        [],
        T.StructType(
            [
                T.StructField("id", T.LongType(), False),
                T.StructField("test_column", T.StringType(), False),
            ]
        ),
    )
    previous_schema = {
        "fields": [
            {"id": 0, "name": "id", "type": "BIGINT NOT NULL"},
        ]
    }
    latest_schema = {
        "fields": [
            {"id": 0, "name": "id", "type": "BIGINT NOT NULL"},
            {"id": 1, "name": "test_column", "type": "STRING NOT NULL"},
        ],
        "partitionKeys": [],
        "primaryKeys": [],
        "options": {},
    }

    dbutils = MagicMock()
    with (
        patch(
            "lakehouse_engine.utils.paimon_utils.DatabricksUtils.get_db_utils",
            return_value=dbutils,
        ),
        patch.object(
            PaimonUtils,
            "_get_latest_schema_version",
            return_value=(3, "/paimon/table/schema/schema-3"),
        ),
        patch.object(
            PaimonUtils,
            "_read_schema_file",
            side_effect=[latest_schema, previous_schema],
        ),
    ):
        result = PaimonUtils._write_schema_file(
            MagicMock(), evolving_df, spec, "/paimon/table"
        )
    assert result is True
    assert dbutils.fs.put.call_args[0][0] == "/paimon/table/schema/schema-4"
    written = json.loads(dbutils.fs.put.call_args[0][1])
    fields_by_name = {field["name"]: field for field in written["fields"]}
    assert fields_by_name["test_column"]["type"] == "STRING"


def test_build_schema_dict_preserves_existing_ids(sample_df: Any) -> None:
    """Existing field ids are reused and new columns receive new ids."""
    spec = _make_output_spec()
    existing = {
        "fields": [
            {"id": 3, "name": "id", "type": "BIGINT NOT NULL"},
            {"id": 8, "name": "name", "type": "STRING"},
        ]
    }
    schema = PaimonUtils.build_schema_dict(sample_df, spec, existing)
    fields_by_name = {field["name"]: field for field in schema["fields"]}
    assert fields_by_name["id"]["id"] == 3
    assert fields_by_name["name"]["id"] == 8
    added_ids = [
        field["id"] for field in schema["fields"] if field["name"] not in {"id", "name"}
    ]
    assert min(added_ids) == 9


def test_build_schema_dict_adds_new_non_nullable_columns_as_nullable(
    sample_df: Any,
) -> None:
    """Added required columns are materialized as nullable on evolved tables."""
    spec = _make_output_spec()
    existing = {
        "fields": [
            {"id": 1, "name": "name", "type": "STRING"},
        ]
    }
    schema = PaimonUtils.build_schema_dict(sample_df, spec, existing)
    fields_by_name = {field["name"]: field for field in schema["fields"]}
    # "id" is non-nullable in sample_df; for evolution it must remain nullable.
    assert fields_by_name["id"]["type"] == "BIGINT"


def test_build_schema_dict_keeps_existing_nullable_for_non_nullable_input() -> None:
    """Existing nullable field remains nullable on subsequent writes."""
    spec = _make_output_spec()
    evolving_df = ExecEnv.SESSION.createDataFrame(
        [],
        T.StructType(
            [
                T.StructField("id", T.LongType(), False),
                T.StructField("test_column", T.StringType(), False),
            ]
        ),
    )
    existing = {
        "fields": [
            {"id": 0, "name": "id", "type": "BIGINT NOT NULL"},
            {"id": 1, "name": "test_column", "type": "STRING"},
        ]
    }
    schema = PaimonUtils.build_schema_dict(evolving_df, spec, existing)
    fields_by_name = {field["name"]: field for field in schema["fields"]}
    assert fields_by_name["test_column"]["type"] == "STRING"


def test_build_schema_dict_preserves_columns_missing_from_dataframe() -> None:
    """Columns from existing schema remain even when absent from incoming DataFrame."""
    spec = _make_output_spec()
    narrow_df = ExecEnv.SESSION.createDataFrame(
        [],
        T.StructType([T.StructField("id", T.LongType(), False)]),
    )
    existing = {
        "fields": [
            {"id": 0, "name": "id", "type": "BIGINT NOT NULL"},
            {"id": 1, "name": "legacy_column", "type": "STRING"},
        ]
    }
    schema = PaimonUtils.build_schema_dict(narrow_df, spec, existing)
    fields_by_name = {field["name"]: field for field in schema["fields"]}
    assert fields_by_name["legacy_column"]["id"] == 1
    assert fields_by_name["legacy_column"]["type"] == "STRING"


def test_build_schema_dict_orders_fields_by_id_when_columns_removed_and_added() -> None:
    """Schema field order remains id-ordered across remove+add evolutions."""
    spec = _make_output_spec()
    evolving_df = ExecEnv.SESSION.createDataFrame(
        [],
        T.StructType(
            [
                T.StructField("id", T.LongType(), False),
                T.StructField("new_col", T.StringType(), True),
            ]
        ),
    )
    existing = {
        "fields": [
            {"id": 0, "name": "id", "type": "BIGINT NOT NULL"},
            {"id": 1, "name": "old_a", "type": "STRING"},
            {"id": 2, "name": "old_b", "type": "INT"},
        ]
    }
    schema = PaimonUtils.build_schema_dict(evolving_df, spec, existing)
    assert [field["name"] for field in schema["fields"]] == [
        "id",
        "old_a",
        "old_b",
        "new_col",
    ]
    assert [field["id"] for field in schema["fields"]] == [0, 1, 2, 3]


def test_align_dataframe_to_schema_adds_missing_columns_as_nulls() -> None:
    """Missing schema columns are injected as nulls in the output DataFrame."""
    df = ExecEnv.SESSION.createDataFrame(
        [(1, "a")],
        T.StructType(
            [
                T.StructField("id", T.LongType(), False),
                T.StructField("name", T.StringType(), True),
            ]
        ),
    )
    schema = {
        "fields": [
            {"id": 0, "name": "id", "type": "BIGINT NOT NULL"},
            {"id": 1, "name": "name", "type": "STRING"},
            {"id": 2, "name": "legacy_column", "type": "INT"},
        ]
    }
    aligned_df = PaimonUtils._align_dataframe_with_table_schema(df, schema)
    collected = aligned_df.collect()[0].asDict()
    assert aligned_df.columns == ["id", "name", "legacy_column"]
    assert collected["legacy_column"] is None


def test_align_dataframe_with_table_schema_uses_latest_schema(sample_df: Any) -> None:
    """Public alignment helper reads latest schema and aligns DataFrame."""
    spec = _make_output_spec(location="/paimon/table")
    dbutils = MagicMock()
    schema = {
        "fields": [
            {"id": 0, "name": "id", "type": "BIGINT NOT NULL"},
            {"id": 1, "name": "name", "type": "STRING"},
            {"id": 2, "name": "legacy_column", "type": "STRING"},
        ]
    }

    with (
        patch(
            "lakehouse_engine.utils.paimon_utils.DatabricksUtils.get_db_utils",
            return_value=dbutils,
        ),
        patch.object(
            PaimonUtils,
            "_get_latest_schema_version",
            return_value=(3, "/paimon/table/schema/schema-3"),
        ),
        patch.object(PaimonUtils, "_read_schema_file", return_value=schema),
    ):
        aligned_df = PaimonUtils.align_dataframe_with_table_schema(
            MagicMock(), sample_df.select("id", "name"), spec
        )

    assert "legacy_column" in aligned_df.columns
