"""Module with column reshaping transformers."""

from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional

import pyspark.sql.types as spark_types
from pyspark.sql import DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    col,
    explode_outer,
    expr,
    from_json,
    map_entries,
    struct,
    to_json,
)

from lakehouse_engine.transformers.exceptions import WrongArgumentsException
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.schema_utils import SchemaUtils


class ColumnReshapers(object):
    """Class containing column reshaping transformers."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def cast(cls, cols: Dict[str, str]) -> Callable:
        """Cast specific columns into the designated type.

        Args:
            cols: dict with columns and respective target types.
                Target types need to have the exact name of spark types:
                https://spark.apache.org/docs/latest/sql-ref-datatypes.html

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            cast_df = df
            for c, t in cols.items():
                cast_df = cast_df.withColumn(c, col(c).cast(getattr(spark_types, t)()))

            return cast_df

        return inner

    @classmethod
    def column_selector(cls, cols: OrderedDict) -> Callable:
        """Select specific columns with specific output aliases.

        Args:
            cols: dict with columns to select and respective aliases.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            return df.select(*[col(c).alias(a) for c, a in cols.items()])

        return inner

    @classmethod
    def flatten_schema(
        cls,
        max_level: int = None,
        shorten_names: bool = False,
        alias: bool = True,
        num_chars: int = 7,
        ignore_cols: List = None,
    ) -> Callable:
        """Flatten the schema of the dataframe.

        Args:
            max_level: level until which you want to flatten the schema.
                Default: None.
            shorten_names: whether to shorten the names of the prefixes
                of the fields being flattened or not. Default: False.
            alias: whether to define alias for the columns being flattened
                or not. Default: True.
            num_chars: number of characters to consider when shortening
                the names of the fields. Default: 7.
            ignore_cols: columns which you don't want to flatten.
                Default: None.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            return df.select(
                SchemaUtils.schema_flattener(
                    schema=df.schema,
                    max_level=max_level,
                    shorten_names=shorten_names,
                    alias=alias,
                    num_chars=num_chars,
                    ignore_cols=ignore_cols,
                )
            )

        return inner

    @classmethod
    def explode_columns(
        cls,
        explode_arrays: bool = False,
        array_cols_to_explode: List[str] = None,
        explode_maps: bool = False,
        map_cols_to_explode: List[str] = None,
    ) -> Callable:
        """Explode columns with types like ArrayType and MapType.

        After it can be applied the flatten_schema transformation,
        if we desired for example to explode the map (as we explode a StructType)
        or to explode a StructType inside the array.
        We recommend you to specify always the columns desired to explode
        and not explode all columns.

        Args:
            explode_arrays: whether you want to explode array columns (True)
                or not (False). Default: False.
            array_cols_to_explode: array columns which you want to explode.
                If you don't specify it will get all array columns and explode them.
                Default: None.
            explode_maps: whether you want to explode map columns (True)
                or not (False). Default: False.
            map_cols_to_explode: map columns which you want to explode.
                If you don't specify it will get all map columns and explode them.
                Default: None.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            if explode_arrays or (array_cols_to_explode is not None):
                df = cls._explode_arrays(df, array_cols_to_explode)

            if explode_maps or (map_cols_to_explode is not None):
                df = cls._explode_maps(df, map_cols_to_explode)

            return df

        return inner

    @classmethod
    def _get_columns(
        cls,
        df: DataFrame,
        data_type: Any,
    ) -> List:
        """Get a list of columns from the dataframe of the data types specified.

        Args:
            df: input dataframe.
            data_type: data type specified.

        Returns:
            List of columns with the datatype specified.
        """
        cols = []
        for field in df.schema.fields:
            if isinstance(field.dataType, data_type):
                cols.append(field.name)
        return cols

    @classmethod
    def with_expressions(cls, cols_and_exprs: Dict[str, str]) -> Callable:
        """Execute Spark SQL expressions to create the specified columns.

        This function uses the Spark expr function:
        https://spark.apache.org/docs/latest/api/python/reference/api/
        pyspark.sql.functions.expr.html

        Args:
            cols_and_exprs: dict with columns and respective expressions to compute
                (Spark SQL expressions).

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            enriched_df = df
            for c, e in cols_and_exprs.items():
                enriched_df = enriched_df.withColumn(c, expr(e))

            return enriched_df

        return inner

    @classmethod
    def rename(cls, cols: Dict[str, str], escape_col_names: bool = True) -> Callable:
        """Rename specific columns into the designated name.

        Args:
            cols: dict with columns and respective target names.
            escape_col_names: whether to escape column names (e.g. `/BIC/COL1`) or not.
            If True it creates a column with the new name and drop the old one.
            If False, uses the native withColumnRenamed Spark function. Default: True.

        Returns:
            Function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            renamed_df = df
            if escape_col_names:
                for old_name, new_name in cols.items():
                    renamed_df = renamed_df.withColumn(new_name, col(old_name))
                    renamed_df = renamed_df.drop(old_name)
            else:
                for old_name, new_name in cols.items():
                    renamed_df = df.withColumnRenamed(old_name, new_name)

            return renamed_df

        return inner

    @classmethod
    def from_avro(
        cls,
        schema: str = None,
        key_col: str = "key",
        value_col: str = "value",
        options: dict = None,
        expand_key: bool = False,
        expand_value: bool = True,
    ) -> Callable:
        """Select all attributes from avro.

        Args:
            schema: the schema string.
            key_col: the name of the key column.
            value_col: the name of the value column.
            options: extra options (e.g., mode: "PERMISSIVE").
            expand_key: whether you want to expand the content inside the key
            column or not. Default: false.
            expand_value: whether you want to expand the content inside the value
            column or not. Default: true.

        Returns:
            Function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            cols_to_select = [
                column for column in df.columns if column not in [key_col, value_col]
            ]

            return df.select(
                *cols_to_select,
                key_col,
                from_avro(col(value_col), schema, options if options else {}).alias(
                    value_col
                ),
            ).select(
                *cols_to_select,
                f"{key_col}.*" if expand_key else key_col,
                f"{value_col}.*" if expand_value else value_col,
            )

        return inner

    @classmethod
    def from_avro_with_registry(
        cls,
        schema_registry: str,
        value_schema: str,
        value_col: str = "value",
        key_schema: str = None,
        key_col: str = "key",
        expand_key: bool = False,
        expand_value: bool = True,
    ) -> Callable:
        """Select all attributes from avro using a schema registry.

        Args:
            schema_registry: the url to the schema registry.
            value_schema: the name of the value schema entry in the schema registry.
            value_col: the name of the value column.
            key_schema: the name of the key schema entry in the schema
            registry. Default: None.
            key_col: the name of the key column.
            expand_key: whether you want to expand the content inside the key
            column or not. Default: false.
            expand_value: whether you want to expand the content inside the value
            column or not. Default: true.

        Returns:
            Function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            cols_to_select = [
                column for column in df.columns if column not in [key_col, value_col]
            ]

            return df.select(  # type: ignore
                *cols_to_select,
                from_avro(
                    col(key_col), key_schema, schema_registry  # type: ignore
                ).alias(key_col)
                if key_schema
                else key_col,
                from_avro(
                    col(value_col), value_schema, schema_registry  # type: ignore
                ).alias(value_col),
            ).select(
                *cols_to_select,
                f"{key_col}.*" if expand_key else key_col,
                f"{value_col}.*" if expand_value else value_col,
            )

        return inner

    @classmethod
    def from_json(
        cls,
        input_col: str,
        schema_path: Optional[str] = None,
        schema: Optional[dict] = None,
        json_options: Optional[dict] = None,
        drop_all_cols: bool = False,
    ) -> Callable:
        """Convert a json string into a json column (struct).

        The new json column can be added to the existing columns (default) or it can
        replace all the others, being the only one to output. The new column gets the
        same name as the original one suffixed with '_json'.

        Args:
            input_col: dict with columns and respective target names.
            schema_path: path to the StructType schema (spark schema).
            schema: dict with the StructType schema (spark schema).
            json_options: options to parse the json value.
            drop_all_cols: whether to drop all the input columns or not.
                Defaults to False.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            if schema_path:
                json_schema = SchemaUtils.from_file(schema_path)
            elif schema:
                json_schema = SchemaUtils.from_dict(schema)
            else:
                raise WrongArgumentsException(
                    "A file or dict schema needs to be provided."
                )

            if drop_all_cols:
                df_with_json = df.select(
                    from_json(
                        col(input_col).cast("string").alias(f"{input_col}_json"),
                        json_schema,
                        json_options if json_options else {},
                    ).alias(f"{input_col}_json")
                )
            else:
                df_with_json = df.select(
                    "*",
                    from_json(
                        col(input_col).cast("string").alias(f"{input_col}_json"),
                        json_schema,
                        json_options if json_options else {},
                    ).alias(f"{input_col}_json"),
                )

            return df_with_json

        return inner

    @classmethod
    def to_json(
        cls, in_cols: List[str], out_col: str, json_options: Optional[dict] = None
    ) -> Callable:
        """Convert dataframe columns into a json value.

        Args:
            in_cols: name(s) of the input column(s).
                Example values:
                "*" - all
                columns; "my_col" - one column named "my_col";
                "my_col1, my_col2" - two columns.
            out_col: name of the output column.
            json_options: options to parse the json value.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            return df.withColumn(
                out_col, to_json(struct(*in_cols), json_options if json_options else {})
            )

        return inner

    @classmethod
    def _explode_arrays(cls, df: DataFrame, cols_to_explode: List[str]) -> DataFrame:
        """Explode array columns from dataframe.

        Args:
            df: the dataframe to apply the explode operation.
            cols_to_explode: list of array columns to perform explode.

        Returns:
            A dataframe with array columns exploded.
        """
        if cols_to_explode is None:
            cols_to_explode = cls._get_columns(df, spark_types.ArrayType)

        for column in cols_to_explode:
            df = df.withColumn(column, explode_outer(column))

        return df

    @classmethod
    def _explode_maps(cls, df: DataFrame, cols_to_explode: List[str]) -> DataFrame:
        """Explode map columns from dataframe.

        Args:
            df: the dataframe to apply the explode operation.
            cols_to_explode: list of map columns to perform explode.

        Returns:
            A dataframe with map columns exploded.
        """
        if cols_to_explode is None:
            cols_to_explode = cls._get_columns(df, spark_types.MapType)

        for column in cols_to_explode:
            df = df.withColumn(column, explode_outer(map_entries(col(column))))

        return df
