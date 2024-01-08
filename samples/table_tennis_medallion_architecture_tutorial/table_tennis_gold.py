# pip install lakehouse-engine

from lakehouse_engine.engine import load_data
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sum,
    count,
    when,
    lit
)


def compute_match_metrics(df: DataFrame) -> DataFrame:
    players_df = (
        df.select(col("home_player").alias("player_name")).distinct()
        .union(df.select(col("visitor_player").alias("player_name")).distinct())
        .distinct()
    )

    return (
        df
        .join(players_df, (col("home_player") == players_df["player_name"]) | (col("visitor_player") == players_df["player_name"]))
        .select(
            players_df["player_name"],
            (col("home_player") == players_df["player_name"]).alias("as_home"),
            when(col("home_player") == players_df["player_name"], col("sets_home")).otherwise(col("sets_visitor")).alias("sets_won"),
            col("sets_played"),
            (col("as_home") == col("home_winner")).alias("winner"),
            (col("sets_played") == col("sets_won")).alias("clean_victory"),
            col("match_year").alias("season")
        )
        .groupBy(col("player_name"), col("season"))
        .agg(
            count(lit(1)).alias("total_matches"),
            sum(when(col("winner"), 1).otherwise(0)).alias("victories"),
            sum(when(col("as_home"), 1).otherwise(0)).alias("home_matches"),
            sum(when(col("as_home") & col("winner"), 1).otherwise(0)).alias("home_victories"),
            sum(when(col("clean_victory"), 1).otherwise(0)).alias("clean_victories"),
            sum(col("sets_played")).alias("sets_played"),
            sum(col("sets_won")).alias("sets_victories")
        )
    )


acon = {
    "input_specs": [
        {
            "spec_id": "table_tennis_matches_silver",
            "read_type": "streaming",
            "data_format": "delta",
            "db_table": "your_database.table_tennis_matches",
        }
    ],
    "transform_specs": [
        {
            "spec_id": "table_tennis_matches_silver_transformed",
            "input_id": "table_tennis_matches_silver",
            "transformers": [
                {
                    "function": "custom_transformation",
                    "args": {"custom_transformer": compute_match_metrics},
                },
            ],
            "force_streaming_foreach_batch_processing": True,
        }
    ],
    "output_specs": [
        {
            "spec_id": "table_tennis_player_season_stats_gold",
            "input_id": "table_tennis_matches_silver_transformed",
            "data_format": "delta",
            "write_type": "merge",
            "merge_opts": {
                "merge_predicate": """
                    new.player_name = current.player_name
                    AND new.season = current.season
                """,
            },
            "db_table": "your_gold_database.table_tennis_player_season_stats",
            "options": {
                "checkpointLocation": "s3://my-data-product-bucket/checkpoints/sports/table_tennis/gold/"
                                      "table_tennis_player_season_stats"
            },
        }
    ]
}

load_data(acon=acon)
