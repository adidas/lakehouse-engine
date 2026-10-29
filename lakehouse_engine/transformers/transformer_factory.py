"""Module with the factory pattern to return transformers."""

from collections import OrderedDict
from collections.abc import Callable

from lakehouse_engine.core.definitions import TransformerSpec
from lakehouse_engine.transformers.aggregators import Aggregators
from lakehouse_engine.transformers.column_creators import ColumnCreators
from lakehouse_engine.transformers.column_reshapers import ColumnReshapers
from lakehouse_engine.transformers.condensers import Condensers
from lakehouse_engine.transformers.custom_transformers import CustomTransformers
from lakehouse_engine.transformers.data_maskers import DataMaskers
from lakehouse_engine.transformers.date_transformers import DateTransformers
from lakehouse_engine.transformers.filters import Filters
from lakehouse_engine.transformers.joiners import Joiners
from lakehouse_engine.transformers.null_handlers import NullHandlers
from lakehouse_engine.transformers.optimizers import Optimizers
from lakehouse_engine.transformers.regex_transformers import RegexTransformers
from lakehouse_engine.transformers.repartitioners import Repartitioners
from lakehouse_engine.transformers.unions import Unions
from lakehouse_engine.transformers.watermarker import Watermarker
from lakehouse_engine.utils.logging_handler import LoggingHandler


class TransformerFactory:
    """TransformerFactory class following the factory pattern."""

    _logger = LoggingHandler(__name__).get_logger()

    UNSUPPORTED_STREAMING_TRANSFORMERS = [
        "condense_record_mode_cdc",
        "group_and_rank",
        "with_auto_increment_id",
        "with_row_id",
    ]

    AVAILABLE_TRANSFORMERS = {
        "add_current_date": DateTransformers.add_current_date,
        "cache": Optimizers.cache,
        "cast": ColumnReshapers.cast,
        "coalesce": Repartitioners.coalesce,
        "column_dropper": DataMaskers.column_dropper,
        "column_filter_exp": Filters.column_filter_exp,
        "column_selector": ColumnReshapers.column_selector,
        "condense_record_mode_cdc": Condensers.condense_record_mode_cdc,
        "convert_to_date": DateTransformers.convert_to_date,
        "convert_to_timestamp": DateTransformers.convert_to_timestamp,
        "custom_transformation": CustomTransformers.custom_transformation,
        "drop_duplicate_rows": Filters.drop_duplicate_rows,
        "expression_filter": Filters.expression_filter,
        "format_date": DateTransformers.format_date,
        "flatten_schema": ColumnReshapers.flatten_schema,
        "explode_columns": ColumnReshapers.explode_columns,
        "from_avro": ColumnReshapers.from_avro,
        "from_avro_with_registry": ColumnReshapers.from_avro_with_registry,
        "from_json": ColumnReshapers.from_json,
        "get_date_hierarchy": DateTransformers.get_date_hierarchy,
        "get_max_value": Aggregators.get_max_value,
        "group_and_rank": Condensers.group_and_rank,
        "hash_masker": DataMaskers.hash_masker,
        "incremental_filter": Filters.incremental_filter,
        "join": Joiners.join,
        "persist": Optimizers.persist,
        "rename": ColumnReshapers.rename,
        "repartition": Repartitioners.repartition,
        "replace_nulls": NullHandlers.replace_nulls,
        "sql_transformation": CustomTransformers.sql_transformation,
        "to_json": ColumnReshapers.to_json,
        "union": Unions.union,
        "union_by_name": Unions.union_by_name,
        "with_watermark": Watermarker.with_watermark,
        "unpersist": Optimizers.unpersist,
        "with_auto_increment_id": ColumnCreators.with_auto_increment_id,
        "with_expressions": ColumnReshapers.with_expressions,
        "with_literals": ColumnCreators.with_literals,
        "with_regex_value": RegexTransformers.with_regex_value,
        "with_row_id": ColumnCreators.with_row_id,
    }

    @staticmethod
    def get_transformer(
        spec: TransformerSpec, data: OrderedDict | None = None
    ) -> Callable:
        """Get a transformer following the factory pattern.

        Args:
            spec: transformer specification (individual transformation... not to be
                confused with list of all transformations).
            data: ordered dict of dataframes to be transformed. Needed when a
                transformer requires more than one dataframe as input.

        Returns:
            Transformer function to be executed in .transform() spark function.

        {{get_example(method_name='get_transformer')}}
        """
        special_handler = TransformerFactory._get_special_transformers().get(
            spec.function
        )
        if special_handler:
            return special_handler(spec, data)

        transformer = TransformerFactory.AVAILABLE_TRANSFORMERS.get(spec.function)
        if transformer:
            return transformer(**(spec.args or {}))

        raise NotImplementedError(
            f"The requested transformer {spec.function} is not implemented."
        )

    @staticmethod
    def _get_special_transformers() -> dict[str, Callable]:
        """Get handlers for transformers requiring data pre-processing."""
        return {
            "incremental_filter": TransformerFactory._build_incremental_filter,
            "join": TransformerFactory._build_join,
            "union": TransformerFactory._build_union,
            "union_by_name": TransformerFactory._build_union,
        }

    @staticmethod
    def _build_incremental_filter(
        spec: TransformerSpec, data: OrderedDict | None
    ) -> Callable:
        """Build incremental_filter transformer, resolving optional increment_df.

        Args:
            spec: transformer specification.
            data: ordered dict of dataframes to be transformed.

        Returns:
            Transformer function to be executed in .transform() spark function.
        """
        args_copy = TransformerFactory._get_spec_args_copy(spec.args)
        if "increment_df" in args_copy:
            if not data:
                raise ValueError("data is required for incremental_filter transformer.")
            args_copy["increment_df"] = data[args_copy["increment_df"]]
        return TransformerFactory.AVAILABLE_TRANSFORMERS[spec.function](**args_copy)

    @staticmethod
    def _build_join(spec: TransformerSpec, data: OrderedDict | None) -> Callable:
        """Build join transformer, resolving join_with dataframe from input ids.

        Args:
            spec: transformer specification.
            data: ordered dict of dataframes to be transformed.

        Returns:
            Transformer function to be executed in .transform() spark function.
        """
        args_copy = TransformerFactory._get_spec_args_copy(spec.args)
        if not data:
            raise ValueError("data is required for join transformer.")
        args_copy["join_with"] = data[args_copy["join_with"]]
        return TransformerFactory.AVAILABLE_TRANSFORMERS[spec.function](**args_copy)

    @staticmethod
    def _build_union(spec: TransformerSpec, data: OrderedDict | None) -> Callable:
        """Build union transformer, resolving union_with input ids to dataframes.

        Args:
            spec: transformer specification.
            data: ordered dict of dataframes to be transformed.

        Returns:
            Transformer function to be executed in .transform() spark function.
        """
        args_copy = TransformerFactory._get_spec_args_copy(spec.args)
        if not data:
            raise ValueError("data is required for union transformer.")
        if not spec.args:
            raise ValueError("args are required for union transformer.")
        args_copy["union_with"] = [
            data[union_with_spec_id] for union_with_spec_id in spec.args["union_with"]
        ]
        return TransformerFactory.AVAILABLE_TRANSFORMERS[spec.function](**args_copy)

    @staticmethod
    def _get_spec_args_copy(spec_args: dict | None) -> dict:
        """Returns a shallow copy of `spec_args` to ensure immutability.

        Args:
            spec_args (dict): A dictionary containing the arguments of a
            TransformerSpec.

        Returns:
            dict: A shallow copy of `spec_args`, preventing modifications to the
            original dictionary. This is important in Spark, especially when
            retries of failed attempts occur. For example, if during the first
            run the `join_with` argument (initially a string) is replaced with a
            DataFrame (as done in the `get_transformer` function), then on a retry,
            depending on how Spark handles state, the `join_with` argument may no
            longer be a string but a DataFrame, leading to key error.
        """
        return dict(spec_args or {})
