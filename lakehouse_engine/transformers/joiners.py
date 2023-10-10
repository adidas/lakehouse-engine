"""Module with join transformers."""
from typing import Callable, List, Optional

from pyspark.sql import DataFrame

from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.transformers.watermarker import Watermarker
from lakehouse_engine.utils.logging_handler import LoggingHandler


class Joiners(object):
    """Class containing join transformers."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def join(
        cls,
        join_with: DataFrame,
        join_condition: str,
        left_df_alias: str = "a",
        right_df_alias: str = "b",
        join_type: str = "inner",
        broadcast_join: bool = True,
        select_cols: Optional[List[str]] = None,
        watermarker: Optional[dict] = None,
    ) -> Callable:
        """Join two dataframes based on specified type and columns.

        Some stream to stream joins are only possible if you apply Watermark, so this
        method also provides a parameter to enable watermarking specification.

        Args:
            left_df_alias: alias of the first dataframe.
            join_with: right dataframe.
            right_df_alias: alias of the second dataframe.
            join_condition: condition to join dataframes.
            join_type: type of join. Defaults to inner.
                Available values: inner, cross, outer, full, full outer,
                left, left outer, right, right outer, semi,
                left semi, anti, and left anti.
            broadcast_join: whether to perform a broadcast join or not.
            select_cols: list of columns to select at the end.
            watermarker: properties to apply watermarking.

        Returns:
            A function to be called in .transform() spark function.
        """

        def inner(df: DataFrame) -> DataFrame:
            # To enable join on foreachBatch processing we had
            # to change to global temp view. The goal here is to
            # avoid problems on simultaneously running process,
            # so we added application id on table name.
            app_id = ExecEnv.SESSION.sparkContext.applicationId
            left = f"`{app_id}_{left_df_alias}`"
            right = f"`{app_id}_{right_df_alias}`"
            df_join_with = join_with
            if watermarker:
                left_df_watermarking = watermarker.get(left_df_alias, None)
                right_df_watermarking = watermarker.get(right_df_alias, None)
                if left_df_watermarking:
                    df = Watermarker.with_watermark(
                        left_df_watermarking["col"],
                        left_df_watermarking["watermarking_time"],
                    )(df)
                if right_df_watermarking:
                    df_join_with = Watermarker.with_watermark(
                        right_df_watermarking["col"],
                        right_df_watermarking["watermarking_time"],
                    )(df_join_with)

            df.createOrReplaceGlobalTempView(left)  # type: ignore
            df_join_with.createOrReplaceGlobalTempView(right)  # type: ignore

            query = f"""
                SELECT {f"/*+ BROADCAST({right_df_alias}) */" if broadcast_join else ""}
                {", ".join(select_cols)}
                FROM global_temp.{left} AS {left_df_alias}
                {join_type.upper()}
                JOIN global_temp.{right} AS {right_df_alias}
                ON {join_condition}
            """  # nosec: B608

            cls._logger.info(f"Execution query: {query}")

            return ExecEnv.SESSION.sql(query)

        return inner
