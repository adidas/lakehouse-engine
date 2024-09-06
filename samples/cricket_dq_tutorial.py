# This sample tutorial is based on the dataset available here: https://www.kaggle.com/datasets/vikramrn/icc-mens-cricket-odi-world-cup-wc-2023-bowling.
# The goal of the tutorial is to demonstrate how you can use the Lakehouse Engine to load data into a target location while assessing its data quality.

# You can install the Lakehouse Engine framework with below command just like any other python library,
# or you can also install it as a cluster-scoped library
pip install lakehouse-engine

# The ACON (algorithm configuration) is the way how you can interact with the Lakehouse Engine.
# Note: don't forget to change locations, buckets and databases to match your environment.
acon = {
    "input_specs": [
        {
            "spec_id": "cricket_world_cup_bronze",
            "read_type": "batch",
            "data_format": "csv",
            "options": {
                "header": True,
                "delimiter": ",",
            },
            "location": "s3://your_bucket_file_location/icc_wc_23_bowl.csv",
        }
    ],
    "dq_specs": [
        {
            "spec_id": "cricket_world_cup_data_quality",
            "input_id": "cricket_world_cup_bronze",
            "dq_type": "validator",
            "store_backend": "s3",
            "bucket": "your_bucket",
            "result_sink_location": "s3://your_bucket/dq_result_sink/gx_blog/",
            "result_sink_db_table": "your_database.gx_blog_result_sink",
            "tag_source_data": True,
            "unexpected_rows_pk": ["player", "match_id"],
            "fail_on_error": False,
            "critical_functions": [
                {
                    "function": "expect_column_values_to_be_in_set",
                    "args": {
                        "column": "team",
                        "value_set": [
                            "Sri Lanka", "Netherlands", "Australia", "England", "Bangladesh",
                            "New Zealand", "India", "Afghanistan", "South Africa", "Pakistan",
                        ],
                    },
                },
                {
                    "function": "expect_column_values_to_be_in_set",
                    "args": {
                        "column": "opponent",
                        "value_set": [
                            "Sri Lanka", "Netherlands", "Australia", "England", "Bangladesh",
                            "New Zealand", "India", "Afghanistan", "South Africa", "Pakistan",
                        ],
                    },
                },
            ],
            "dq_functions": [
                {
                    "function": "expect_column_values_to_not_be_null",
                    "args": {"column": "player"},
                },
                {
                    "function": "expect_column_values_to_be_between",
                    "args": {"column": "match_id", "min_value": 0, "max_value": 47},
                },
                {
                    "function": "expect_column_values_to_be_in_set",
                    "args": {"column": "maidens", "value_set": [0, 1]},
                },
            ],
        },
    ],
    "output_specs": [
        {
            "spec_id": "cricket_world_cup_silver",
            "input_id": "cricket_world_cup_data_quality",
            "write_type": "overwrite",
            "db_table": "your_database.gx_blog_cricket",
            "location": "s3://your_bucket/rest_of_path/gx_blog_cricket/",
            "data_format": "delta",
        }
    ],
}

# You need to import the Load Data algorithm from the Lakehouse Engine, so that you can perform Data Loads.
from lakehouse_engine.engine import load_data

# Finally, you just need to run the Load Data algorithm with the ACON that you have just defined.
load_data(acon=acon)
