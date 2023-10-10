"""Utilities to facilitate dataframe schema management."""
from logging import Logger
from typing import Any, List, Optional

from pyspark.sql.functions import col
from pyspark.sql.types import StructType

from lakehouse_engine.core.definitions import InputSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.storage.file_storage_functions import FileStorageFunctions


class SchemaUtils(object):
    """Schema utils that help retrieve and manage schemas of dataframes."""

    _logger: Logger = LoggingHandler(__name__).get_logger()

    @staticmethod
    def from_file(file_path: str) -> StructType:
        """Get a spark schema from a file (spark StructType json file) in a file system.

        Args:
            file_path: path of the file in a file system. Check here:
                https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/
                StructType.html

        Returns:
            Spark schema struct type.
        """
        return StructType.fromJson(FileStorageFunctions.read_json(file_path))

    @staticmethod
    def from_file_to_dict(file_path: str) -> Any:
        """Get a dict with the spark schema from a file in a file system.

        Args:
            file_path: path of the file in a file system. Check here:
                https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/
                StructType.html

        Returns:
             Spark schema in a dict.
        """
        return FileStorageFunctions.read_json(file_path)

    @staticmethod
    def from_dict(struct_type: dict) -> StructType:
        """Get a spark schema from a dict.

        Args:
            struct_type: dict containing a spark schema structure. Check here:
                https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/
                StructType.html

        Returns:
             Spark schema struct type.
        """
        return StructType.fromJson(struct_type)

    @staticmethod
    def from_table_schema(table: str) -> StructType:
        """Get a spark schema from a table.

        Args:
            table: table name from which to inherit the schema.

        Returns:
            Spark schema struct type.
        """
        return ExecEnv.SESSION.read.table(table).schema

    @classmethod
    def from_input_spec(cls, input_spec: InputSpec) -> Optional[StructType]:
        """Get a spark schema from an input specification.

        This covers scenarios where the schema is provided as part of the input
        specification of the algorithm. Schema can come from the table specified in the
        input specification (enforce_schema_from_table) or by the dict with the spark
        schema provided there also.

        Args:
            input_spec: input specification.

        Returns:
            spark schema struct type.
        """
        if input_spec.enforce_schema_from_table:
            cls._logger.info(
                f"Reading schema from table: {input_spec.enforce_schema_from_table}"
            )
            return SchemaUtils.from_table_schema(input_spec.enforce_schema_from_table)
        elif input_spec.schema_path:
            cls._logger.info(f"Reading schema from file: {input_spec.schema_path}")
            return SchemaUtils.from_file(input_spec.schema_path)
        elif input_spec.schema:
            cls._logger.info(
                f"Reading schema from configuration file: {input_spec.schema}"
            )
            return SchemaUtils.from_dict(input_spec.schema)
        else:
            cls._logger.info("No schema was provided... skipping enforce schema")
            return None

    @staticmethod
    def _get_prefix_alias(num_chars: int, prefix: str, shorten_names: bool) -> str:
        """Get prefix alias for a field."""
        return (
            f"""{'_'.join(
                [item[:num_chars] for item in prefix.split('.')]
            )}_"""
            if shorten_names
            else f"{prefix}_".replace(".", "_")
        )

    @staticmethod
    def schema_flattener(
        schema: StructType,
        prefix: str = None,
        level: int = 1,
        max_level: int = None,
        shorten_names: bool = False,
        alias: bool = True,
        num_chars: int = 7,
        ignore_cols: List = None,
    ) -> List:
        """Recursive method to flatten the schema of the dataframe.

        Args:
            schema: schema to be flattened.
            prefix: prefix of the struct to get the value for. Only relevant
            for being used in the internal recursive logic.
            level: level of the depth in the schema being flattened. Only relevant
            for being used in the internal recursive logic.
            max_level: level until which you want to flatten the schema. Default: None.
            shorten_names: whether to shorten the names of the prefixes of the fields
            being flattened or not. Default: False.
            alias: whether to define alias for the columns being flattened or
            not. Default: True.
            num_chars: number of characters to consider when shortening the names of
            the fields. Default: 7.
            ignore_cols: columns which you don't want to flatten. Default: None.

        Returns:
            A function to be called in .transform() spark function.
        """
        cols = []
        ignore_cols = ignore_cols if ignore_cols else []
        for field in schema.fields:
            name = prefix + "." + field.name if prefix else field.name
            field_type = field.dataType

            if (
                isinstance(field_type, StructType)
                and name not in ignore_cols
                and (max_level is None or level <= max_level)
            ):
                cols += SchemaUtils.schema_flattener(
                    schema=field_type,
                    prefix=name,
                    level=level + 1,
                    max_level=max_level,
                    shorten_names=shorten_names,
                    alias=alias,
                    num_chars=num_chars,
                    ignore_cols=ignore_cols,
                )
            else:
                if alias and prefix:
                    prefix_alias = SchemaUtils._get_prefix_alias(
                        num_chars, prefix, shorten_names
                    )
                    cols.append(col(name).alias(f"{prefix_alias}{field.name}"))
                else:
                    cols.append(col(name))
        return cols
