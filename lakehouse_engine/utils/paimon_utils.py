"""Utilities for managing Apache Paimon path-based schema files.

Paimon's path-based writer requires a schema file
(``<location>/schema/schema-N``) to exist before any data is written; otherwise
it fails with ``Schema file not found in location <path>. Please create table
first.``

This means that when writing data in a fresh paimon location it is necessary
to create the schema file before any write operation can be performed and
evolve it when schema changes are detected.

These utils allow the engine to transparently create and evolve the schema
file when writing to a Paimon location, without requiring the user to
pre-create the table or manage schema evolution manually.
"""

import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse_engine.core.definitions import OutputSpec
from lakehouse_engine.utils.databricks_utils import DatabricksUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler

SIMPLE_PAIMON_TYPES = {
    T.BooleanType: "BOOLEAN",
    T.ByteType: "TINYINT",
    T.ShortType: "SMALLINT",
    T.IntegerType: "INT",
    T.LongType: "BIGINT",
    T.FloatType: "FLOAT",
    T.DoubleType: "DOUBLE",
    T.StringType: "STRING",
    T.BinaryType: "BYTES",
    T.DateType: "DATE",
    T.TimestampType: "TIMESTAMP(6) WITH LOCAL TIME ZONE",
}


class PaimonUtils(object):
    """Helpers to bootstrap and describe Apache Paimon path-based tables."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    PRIMARY_KEY_OPTION = "primary-key"
    PARTITION_OPTION = "partition"
    NOT_NULL = " NOT NULL"
    SCHEMA_VERSION = 3

    @classmethod
    def ensure_table_exists(
        cls, spark: Any, df: DataFrame, output_spec: OutputSpec
    ) -> None:
        """Ensure the Paimon schema file exists and evolves when schema changes.

        Args:
            spark: active Spark session.
            df: dataframe to be written.
            output_spec: output specification driving the write operation.
        """
        location = output_spec.location.rstrip("/")

        if cls._write_schema_file(spark, df, output_spec, location):
            return
        raise RuntimeError(
            "Unable to access dbutils to bootstrap/evolve Paimon schema files at "
            f"{location}. This flow requires direct file access to schema/schema-N."
        )

    @classmethod
    def align_dataframe_with_table_schema(
        cls, spark: Any, df: DataFrame, output_spec: OutputSpec
    ) -> DataFrame:
        """Align DataFrame columns with the latest Paimon schema file.

        Missing columns from the table schema are added as ``NULL`` values cast to
        the corresponding Paimon type, so writes remain backward-compatible when the
        incoming DataFrame omits already-existing columns.

        Args:
            spark: active Spark session.
            df: dataframe to be written.
            output_spec: output specification driving the write operation.

        Returns:
            DataFrame with columns aligned to the latest Paimon schema.
        """
        location = output_spec.location.rstrip("/")
        try:
            dbutils = DatabricksUtils.get_db_utils(spark)
        except Exception as e:  # noqa: BLE001
            cls._LOGGER.debug(
                "dbutils unavailable, cannot align Paimon DataFrame schema: %s", e
            )
            return df

        _, latest_schema_path = cls._get_latest_schema_version(dbutils, location)
        if latest_schema_path is None:
            return df

        table_schema = cls._read_schema_file(dbutils, latest_schema_path)
        return cls._align_dataframe_with_table_schema(df, table_schema)

    @classmethod
    def _write_schema_file(
        cls, spark: Any, df: DataFrame, output_spec: OutputSpec, location: str
    ) -> bool:
        """Create/evolve Paimon ``schema-N`` JSON file at the table location.

        Returns:
            True when the latest schema file is aligned with the incoming DataFrame;
            False if ``dbutils`` is unavailable and no write was attempted.
        """
        try:
            dbutils = DatabricksUtils.get_db_utils(spark)
        except Exception as e:  # noqa: BLE001
            cls._LOGGER.debug(
                "dbutils unavailable, cannot write Paimon schema directly: %s", e
            )
            return False

        latest_version, latest_schema_path = cls._get_latest_schema_version(
            dbutils, location
        )
        existing_schema = (
            cls._read_schema_file(dbutils, latest_schema_path)
            if latest_schema_path is not None
            else None
        )
        desired_schema = cls.build_schema_dict(df, output_spec, existing_schema)
        desired_schema = cls._relax_newly_added_required_fields(
            dbutils, location, latest_version, existing_schema, desired_schema
        )

        if existing_schema is not None and cls._create_normalized_schema(
            existing_schema
        ) == cls._create_normalized_schema(desired_schema):
            cls._LOGGER.info(
                "Paimon schema already up-to-date at %s.", latest_schema_path
            )
            return True

        next_version = 0 if latest_version is None else latest_version + 1
        schema_path = f"{location}/schema/schema-{next_version}"

        schema_json = json.dumps(desired_schema, indent=2)
        cls._LOGGER.info(
            "Writing Paimon schema version %s at %s.",
            next_version,
            schema_path,
        )

        try:
            dbutils.fs.put(schema_path, schema_json, False)
        except Exception as e:  # noqa: BLE001
            if DatabricksUtils.check_dbutils_path_exists(dbutils, schema_path):
                written_schema = cls._read_schema_file(dbutils, schema_path)
                if cls._create_normalized_schema(
                    written_schema
                ) == cls._create_normalized_schema(desired_schema):
                    cls._LOGGER.info(
                        "Schema version %s was written concurrently at %s.",
                        next_version,
                        schema_path,
                    )
                    return True
            raise e
        return True

    @classmethod
    def _relax_newly_added_required_fields(
        cls,
        dbutils: Any,
        location: str,
        latest_version: Optional[int],
        existing_schema: Optional[Dict[str, Any]],
        desired_schema: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Relax required fields that were newly introduced in the latest schema.

        Args:
            dbutils: Databricks utilities instance.
            location: Location of the Paimon table.
            latest_version: Latest schema version number.
            existing_schema: Existing schema dictionary.
            desired_schema: Desired schema dictionary.

        Returns:
            Schema dictionary with newly added required fields relaxed to nullable.
        """
        if (
            existing_schema is None
            or latest_version is None
            or latest_version <= 0
            or not desired_schema.get("fields")
        ):
            return desired_schema

        previous_schema_path = f"{location}/schema/schema-{latest_version - 1}"
        try:
            previous_schema = cls._read_schema_file(dbutils, previous_schema_path)
        except Exception:  # noqa: BLE001
            return desired_schema

        previous_field_names = {
            str(field.get("name"))
            for field in previous_schema.get("fields", [])
            if field.get("name") is not None
        }
        primary_keys = {str(key) for key in desired_schema.get("primaryKeys", [])}

        updated_fields = []
        has_changes = False
        for field in desired_schema.get("fields", []):
            field_name = str(field.get("name"))
            field_type = str(field.get("type", ""))
            if (
                field_name not in previous_field_names
                and field_name not in primary_keys
                and field_type.endswith(cls.NOT_NULL)
            ):
                relaxed_field = dict(field)
                relaxed_field["type"] = field_type[: -len(cls.NOT_NULL)]
                updated_fields.append(relaxed_field)
                has_changes = True
                cls._LOGGER.warning(
                    "Relaxing newly added required column '%s' to nullable in "
                    "Paimon schema to keep older data files readable.",
                    field_name,
                )
            else:
                updated_fields.append(field)

        if not has_changes:
            return desired_schema

        relaxed_schema = dict(desired_schema)
        relaxed_schema["fields"] = updated_fields
        relaxed_schema["highestFieldId"] = max(
            (int(field.get("id", -1)) for field in updated_fields), default=-1
        )
        relaxed_schema["timeMillis"] = int(time.time() * 1000)
        return relaxed_schema

    @classmethod
    def _get_latest_schema_version(
        cls, dbutils: Any, location: str
    ) -> Tuple[Optional[int], Optional[str]]:
        """Get latest ``schema-N`` version and path from table schema directory.

        Args:
            dbutils: Databricks utilities instance.
            location: Location of the Paimon table.

        Returns:
            Tuple of latest schema version and its path, or (None, None) if not found.
        """
        schema_dir = f"{location}/schema"
        try:
            entries = dbutils.fs.ls(schema_dir)
        except Exception:  # noqa: BLE001
            return None, None

        latest: Optional[Tuple[int, str]] = None
        for entry in entries:
            name = DatabricksUtils.get_dbutils_entry_name(entry)
            if not name:
                continue
            match = re.search(r"schema-(\d+)$", name.rstrip("/"))
            if not match:
                continue
            version = int(match.group(1))
            entry_path = DatabricksUtils.get_dbutils_entry_path(entry) or (
                f"{schema_dir}/{name}"
            )
            if latest is None or version > latest[0]:
                latest = (version, entry_path)
        return (latest[0], latest[1]) if latest else (None, None)

    @staticmethod
    def _read_schema_file(dbutils: Any, schema_path: str) -> Any:
        """Read and parse a schema JSON file from DBFS.

        Args:
            dbutils: Databricks utilities instance.
            schema_path: Path to the schema JSON file.

        Returns:
            Parsed schema as a dictionary.
        """
        return json.loads(dbutils.fs.head(schema_path))

    @classmethod
    def _create_normalized_schema(cls, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a schema payload for stable equality checks.

        This function removes non-essential fields from the schema as well as
        sorting them so a comparison between two schemas can be done in a
        more stable manner.

        Args:
            schema: The schema dictionary to normalize.

        Returns:
            A normalized schema dictionary.
        """
        normalized = dict(schema)
        normalized.pop("timeMillis", None)
        normalized["fields"] = sorted(
            [
                {
                    "id": field.get("id"),
                    "name": field.get("name"),
                    "type": field.get("type"),
                }
                for field in normalized.get("fields", [])
            ],
            key=lambda field: (field["id"], str(field["name"])),
        )
        normalized["partitionKeys"] = list(normalized.get("partitionKeys", []))
        normalized["primaryKeys"] = list(normalized.get("primaryKeys", []))
        normalized["options"] = dict(normalized.get("options", {}))
        return normalized

    @classmethod
    def build_schema_json(cls, df: DataFrame, output_spec: OutputSpec) -> str:
        """Build the JSON content of Paimon's ``schema-N`` file.

        Args:
            df: dataframe to be writen.
            output_spec: output specification driving the write operation.

        Returns:
            JSON string with schema definition.
        """
        return json.dumps(cls.build_schema_dict(df, output_spec), indent=2)

    @classmethod
    def build_schema_dict(
        cls,
        df: DataFrame,
        output_spec: OutputSpec,
        existing_schema: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build the schema payload as a dictionary.

        When ``existing_schema`` is provided, field ids are preserved and only
        new fields receive new ids.

        Args:
            df: dataframe to be writen.
            output_spec: output specification driving the write operation.
            existing_schema: optional existing schema dictionary to preserve field ids.

        Returns:
            Dictionary with schema definition.
        """
        options = dict(output_spec.options) if output_spec.options else {}
        paimon_options = options.get("paimon_options", {})
        partition_keys = cls._extract_string_list(
            paimon_options.pop(cls.PARTITION_OPTION, None)
        ) or list(output_spec.partitions or [])
        primary_keys = cls._extract_string_list(
            paimon_options.pop(cls.PRIMARY_KEY_OPTION, None)
        )

        fields = cls._build_fields_with_stable_ids(df, existing_schema)
        highest_field_id = max((f["id"] for f in fields), default=-1)

        return {
            "version": cls.SCHEMA_VERSION,
            "id": 0,
            "fields": fields,
            "highestFieldId": highest_field_id,
            "partitionKeys": partition_keys,
            "primaryKeys": primary_keys,
            "options": {k: str(v) for k, v in paimon_options.items()},
            "comment": "",
            "timeMillis": int(time.time() * 1000),
        }

    @classmethod
    def _build_fields_with_stable_ids(
        cls, df: DataFrame, existing_schema: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Build fields list preserving ids from existing schema when possible.

        For evolved tables, newly added non-nullable DataFrame columns are downgraded
        to nullable in the schema file so older data files remain readable.
        """
        existing_fields = (
            list(existing_schema.get("fields", [])) if existing_schema else []
        )
        has_existing_schema = bool(existing_fields)
        existing_by_name = cls._get_existing_fields_by_name(existing_fields)
        incoming_names = {field.name for field in df.schema.fields}

        next_id = max((int(field["id"]) for field in existing_fields), default=-1) + 1
        fields: List[Dict[str, Any]] = []
        for field in df.schema.fields:
            existing = existing_by_name.get(field.name)
            if existing:
                fields.append(cls._build_existing_field_entry(field, existing))
                continue

            new_field_entry, next_id = cls._build_new_field_entry(
                field, has_existing_schema, next_id
            )
            fields.append(new_field_entry)

        fields.extend(
            cls._build_preserved_missing_field_entries(existing_by_name, incoming_names)
        )
        return sorted(fields, key=lambda field: int(field["id"]))

    @staticmethod
    def _get_existing_fields_by_name(
        existing_fields: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """Map existing schema fields by name, ignoring malformed entries.

        Args:
            existing_fields: List of existing schema fields.

        Returns:
            Dictionary mapping field names to field definitions.
        """
        return {
            str(field["name"]): field
            for field in existing_fields
            if "name" in field and "id" in field and "type" in field
        }

    @classmethod
    def _build_preserved_missing_field_entries(
        cls,
        existing_by_name: Dict[str, Dict[str, Any]],
        incoming_names: set,
    ) -> List[Dict[str, Any]]:
        """Preserve existing schema columns absent from the incoming DataFrame."""
        missing_existing_columns = [
            existing_by_name[name]
            for name in existing_by_name
            if name not in incoming_names
        ]
        return [
            {
                "id": int(field["id"]),
                "name": str(field["name"]),
                "type": str(field["type"]),
            }
            for field in sorted(
                missing_existing_columns, key=lambda entry: int(entry["id"])
            )
        ]

    @classmethod
    def _build_existing_field_entry(
        cls, spark_field: Any, existing_field: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build a field entry for an already-existing column."""
        rendered_type = cls.spark_type_to_paimon(
            spark_field.dataType, spark_field.nullable
        )
        existing_type = str(existing_field["type"])
        if cls._get_paimon_top_level_base_type(
            existing_type
        ) != cls._get_paimon_top_level_base_type(rendered_type):
            raise ValueError(
                "Incompatible Paimon schema evolution: changing existing "
                f"column type is not supported for '{spark_field.name}' "
                f"({existing_type} -> {rendered_type})."
            )
        # Keep persisted nullability to avoid oscillating schema versions.
        return {
            "id": int(existing_field["id"]),
            "name": spark_field.name,
            "type": existing_type,
        }

    @classmethod
    def _build_new_field_entry(
        cls, spark_field: Any, has_existing_schema: bool, next_id: int
    ) -> Tuple[Dict[str, Any], int]:
        """Build a field entry for a newly-added column."""
        rendered_type = cls.spark_type_to_paimon(
            spark_field.dataType,
            (
                True
                if has_existing_schema and not spark_field.nullable
                else spark_field.nullable
            ),
        )
        if has_existing_schema and not spark_field.nullable:
            cls._LOGGER.warning(
                "Adding non-nullable column '%s' as nullable in Paimon "
                "schema evolution to keep older files readable.",
                spark_field.name,
            )

        field_entry = {"id": next_id, "name": spark_field.name, "type": rendered_type}
        return field_entry, next_id + 1

    @staticmethod
    def _get_paimon_top_level_base_type(type_literal: str) -> str:
        """Extract top-level type removing trailing ``NOT NULL`` marker.

        Args:
            type_literal: The type literal string.

        Returns:
            The top-level type without the trailing ``NOT NULL`` marker.
        """
        return (
            type_literal[: -len(PaimonUtils.NOT_NULL)]
            if type_literal.endswith(PaimonUtils.NOT_NULL)
            else type_literal
        )

    @classmethod
    def _align_dataframe_with_table_schema(
        cls, df: DataFrame, schema: Dict[str, Any]
    ) -> DataFrame:
        """Align DataFrame columns to schema fields, adding missing as null."""
        schema_fields = list(schema.get("fields", []))
        if not schema_fields:
            return df

        incoming_names = set(df.columns)
        ordered_schema_fields = sorted(
            (
                field
                for field in schema_fields
                if "name" in field and "id" in field and "type" in field
            ),
            key=lambda field: int(field["id"]),
        )
        schema_names = {str(field["name"]) for field in ordered_schema_fields}

        select_columns = []
        for field in ordered_schema_fields:
            field_name = str(field["name"])
            if field_name in incoming_names:
                select_columns.append(F.col(f"`{field_name}`"))
                continue

            base_type = cls._get_paimon_top_level_base_type(str(field["type"]))
            select_columns.append(F.lit(None).cast(base_type).alias(field_name))
            cls._LOGGER.warning(
                "Column '%s' missing from incoming DataFrame. Writing it as NULL.",
                field_name,
            )

        select_columns.extend(
            F.col(f"`{column}`") for column in df.columns if column not in schema_names
        )
        return df.select(*select_columns)

    @classmethod
    def _append_fields(cls, spark_fields: list, out: list, start_id: int) -> int:
        """Append Spark struct fields to ``out`` as Paimon ``DataField`` dicts.

        Args:
            spark_fields: list of Spark struct fields.
            out: list of Paimon ``DataField`` dicts.
            start_id: start id of ``DataField`` dict.

        Returns:
             The next free field id after appending.
        """
        next_id = start_id
        for f in spark_fields:
            entry = {
                "id": next_id,
                "name": f.name,
                "type": cls.spark_type_to_paimon(f.dataType, f.nullable),
            }
            next_id += 1
            out.append(entry)
        return next_id

    @classmethod
    def spark_type_to_paimon(cls, dtype: T.DataType, nullable: bool) -> str:
        """Convert a Spark DataType to a Paimon type literal.

        Args:
            dtype: DataType to be converted.
            nullable: Whether or not the DataType is nullable.

        Returns:
            PAIMON type to be used, with nullability suffix if needed.
        """
        base = cls._convert_spark_type_to_paimon_base(dtype)
        return base if nullable else f"{base}{cls.NOT_NULL}"

    @classmethod
    def _convert_spark_type_to_paimon_base(cls, dtype: T.DataType) -> str:
        """Render the nullable form of a Spark DataType as Paimon SQL.

        Args:
            dtype: DataType to be converted.

        Returns:
            PAIMON type to be used.
        """
        if type(dtype) in SIMPLE_PAIMON_TYPES:
            return SIMPLE_PAIMON_TYPES[type(dtype)]

        timestamp_ntz = getattr(T, "TimestampNTZType", None)
        if timestamp_ntz is not None and isinstance(dtype, timestamp_ntz):
            return "TIMESTAMP(6)"
        if isinstance(dtype, T.DecimalType):
            return f"DECIMAL({dtype.precision}, {dtype.scale})"
        if isinstance(dtype, T.ArrayType):
            return cls._array_to_paimon(dtype)
        if isinstance(dtype, T.MapType):
            return cls._map_to_paimon(dtype)
        if isinstance(dtype, T.StructType):
            return cls._struct_to_paimon(dtype)

        raise NotImplementedError(
            f"Unsupported Spark type for Paimon schema bootstrap: {dtype}. "
            "Please pre-create the Paimon table manually."
        )

    @classmethod
    def _array_to_paimon(cls, dtype: T.ArrayType) -> str:
        """Render a Spark ArrayType as a Paimon ``ARRAY<...>`` literal.

        Args:
            dtype: ArrayType to be converted.

        Returns:
            String array definition.
        """
        element = cls.spark_type_to_paimon(dtype.elementType, dtype.containsNull)
        return f"ARRAY<{element}>"

    @classmethod
    def _map_to_paimon(cls, dtype: T.MapType) -> str:
        """Render a Spark MapType as a Paimon ``MAP<...>`` literal.

        Args:
            dtype: MapType to be converted.

        Returns:
            String map definition.
        """
        key = cls.spark_type_to_paimon(dtype.keyType, False)
        value = cls.spark_type_to_paimon(dtype.valueType, dtype.valueContainsNull)
        return f"MAP<{key}, {value}>"

    @classmethod
    def _struct_to_paimon(cls, dtype: T.StructType) -> str:
        """Render a Spark StructType as a Paimon ``ROW<...>`` literal.

        Args:
            dtype: StructType to be converted.

        Returns:
            String struct definition.
        """
        inner = ", ".join(
            f"`{sf.name}` {cls.spark_type_to_paimon(sf.dataType, sf.nullable)}"
            for sf in dtype.fields
        )
        return f"ROW<{inner}>"

    @staticmethod
    def _extract_string_list(value: Any) -> List[str]:
        """Normalize a primary-key/partition option value into a list of strings.

        Args:
            value: Option value, which can be a string or a list/tuple of strings.

        Returns:
            List of columns to be used as partition options.
        """
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            return [str(v).strip() for v in value if str(v).strip()]
        return [part.strip() for part in str(value).split(",") if part.strip()]
