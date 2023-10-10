"""Module with the factory pattern to return transformers."""
from typing import Callable, OrderedDict

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


class TransformerFactory(object):
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
    def get_transformer(spec: TransformerSpec, data: OrderedDict = None) -> Callable:
        """Get a transformer following the factory pattern.

        Args:
            spec: transformer specification (individual transformation... not to be
                confused with list of all transformations).
            data: ordered dict of dataframes to be transformed. Needed when a
                transformer requires more than one dataframe as input.

        Returns:
            Transformer function to be executed in .transform() spark function.
        """
        if spec.function == "incremental_filter":
            # incremental_filter optionally expects a DataFrame as input, so find it.
            if "increment_df" in spec.args:
                spec.args["increment_df"] = data[spec.args["increment_df"]]
            return TransformerFactory.AVAILABLE_TRANSFORMERS[  # type: ignore
                spec.function
            ](**spec.args)
        elif spec.function == "join":
            # get the dataframe given the input_id in the input specs of the acon.
            spec.args["join_with"] = data[spec.args["join_with"]]
            return TransformerFactory.AVAILABLE_TRANSFORMERS[  # type: ignore
                spec.function
            ](**spec.args)
        elif spec.function == "union" or spec.function == "union_by_name":
            # get the list of dataframes given the input_id in the input specs
            # of the acon.
            df_to_transform = spec.args["union_with"]
            spec.args["union_with"] = []
            for item in df_to_transform:
                spec.args["union_with"].append(data[item])
            return TransformerFactory.AVAILABLE_TRANSFORMERS[  # type: ignore
                spec.function
            ](**spec.args)
        elif spec.function in TransformerFactory.AVAILABLE_TRANSFORMERS:
            return TransformerFactory.AVAILABLE_TRANSFORMERS[  # type: ignore
                spec.function
            ](**spec.args)
        else:
            raise NotImplementedError(
                f"The requested transformer {spec.function} is not implemented."
            )
