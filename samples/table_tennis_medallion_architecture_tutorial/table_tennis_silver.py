# pip install lakehouse-engine

from lakehouse_engine.engine import load_data


cols_to_rename = {
    "_c0": "match_id",
    "Date": "match_date",
    "Player1": "home_player",
    "Player2": "visitor_player",
    "Sets_P1": "sets_home",
    "Sets_P2": "sets_visitor",
    "P1_G1": "home_player_game_1",
    "P2_G1": "visitor_player_game_1",
    "P1_G2": "home_player_game_2",
    "P2_G2": "visitor_player_game_2",
    "P1_G3": "home_player_game_3",
    "P2_G3": "visitor_player_game_3",
    "P1_G4": "home_player_game_4",
    "P2_G4": "visitor_player_game_4",
    "P1_G5": "home_player_game_5",
    "P2_G5": "visitor_player_game_5",
    "HomeWinner": "home_winner"
}

acon = {
    "input_specs": [
        {
            "spec_id": "table_tennis_bronze",
            "read_type": "batch",
            "data_format": "delta",
            "location": "s3://my-data-product-bucket/sports/table_tennis/bronze/",
        }
    ],
    "transform_specs": [
        {
            "spec_id": "table_tennis_bronze_transformed",
            "input_id": "table_tennis_bronze",
            "transformers": [
                {
                    "function": "add_current_date",
                    "args": {
                        "output_col": "lh_created_on"
                    }
                },
                {
                    "function": "rename",
                    "args": {
                        "cols": cols_to_rename,
                    },
                },
                {
                    "function": "with_expressions",
                    "args": {
                        "cols_and_exprs": {
                            "sets_played": "sets_home + sets_visitor",
                            "match_year": "YEAR(match_date)",
                            "match_month": "MONTH(match_date)",
                            "home_player_game_4": "CASE WHEN home_player_game_4 = 'NA' THEN null "
                                                  "ELSE home_player_game_4 END",
                            "visitor_player_game_4": "CASE WHEN visitor_player_game_4 = 'NA' THEN null "
                                                     "ELSE visitor_player_game_4 END",
                            "home_player_game_5": "CASE WHEN home_player_game_5 = 'NA' THEN null "
                                                  "ELSE home_player_game_5 END",
                            "visitor_player_game_5": "CASE WHEN visitor_player_game_5 = 'NA' THEN null "
                                                     "ELSE visitor_player_game_5 END"
                        }
                    }
                }
            ],
        }
    ],
    "dq_specs": [
        {
            "spec_id": "check_table_tennis_bronze_transformed",
            "input_id": "table_tennis_bronze_transformed",
            "dq_type": "validator",
            "bucket": "my-data-product-bucket",
            "result_sink_db_table": "your_database.table_tennis_matches_dq_checks",
            "result_sink_location": "s3://my-data-product-bucket/dq/sports/table_tennis/"
                                    "table_tennis_matches_dq_checks/",
            "fail_on_error": False,
            "dq_functions": [
                {
                    "function": "expect_column_values_to_not_be_null",
                    "args": {
                        "column": "match_date"
                    }
                },
                {
                    "function": "expect_column_values_to_be_between",
                    "args": {
                        "column": "sets_home",
                        "min_value": 0,
                        "max_value": 3
                    },
                },
                {
                    "function": "expect_column_values_to_be_between",
                    "args": {
                        "column": "sets_visitor",
                        "min_value": 0,
                        "max_value": 3
                    }
                },
                {
                    "function": "expect_column_values_to_be_between",
                    "args": {
                        "column": "sets_played",
                        "min_value": 3,
                        "max_value": 5
                    }
                }
            ],
        },
    ],
    "output_specs": [
        {
            "spec_id": "table_tennis_matches_silver",
            "input_id": "check_table_tennis_bronze_transformed",
            "data_format": "delta",
            "write_type": "merge",
            "merge_opts": {
                "merge_predicate": """
                    new.match_id = current.match_id
                """,
            },
            "db_table": "your_database.table_tennis_matches"
        }
    ],
    "terminate_specs": [
        {
            "function": "optimize_dataset",
            "args": {
                "db_table": "your_database.table_tennis_matches"
            }
        }
    ]
}

load_data(acon=acon)
