from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "orders_bronze",
            "read_type": "streaming",
            "data_format": "csv",
            "schema_path": "s3://my-data-product-bucket/artefacts/metadata/bronze/schemas/orders.json",
            "with_filepath": True,
            "options": {
                "badRecordsPath": "s3://my-data-product-bucket/badrecords/order_events_with_dq/",
                "header": False,
                "delimiter": "\u005E",
                "dateFormat": "yyyyMMdd",
            },
            "location": "s3://my-data-product-bucket/bronze/orders/",
        }
    ],
    "transform_specs": [
        {
            "spec_id": "orders_bronze_with_extraction_date",
            "input_id": "orders_bronze",
            "transformers": [
                {"function": "with_row_id"},
                {
                    "function": "with_regex_value",
                    "args": {
                        "input_col": "lhe_extraction_filepath",
                        "output_col": "extraction_date",
                        "drop_input_col": True,
                        "regex": ".*WE_SO_SCL_(\\d+).csv",
                    },
                },
            ],
        }
    ],
    "dq_specs": [
        {
            "spec_id": "check_orders_bronze_with_extraction_date",
            "input_id": "orders_bronze_with_extraction_date",
            "dq_type": "validator",
            "result_sink_db_table": "your_database.order_events_dq_checks",
            "fail_on_error": False,
            "dq_functions": [
                {
                    "dq_function": "expect_column_values_to_not_be_null",
                    "args": {"column": "omnihub_locale_code"},
                },
                {
                    "dq_function": "expect_column_unique_value_count_to_be_between",
                    "args": {"column": "product_division", "min_value": 10, "max_value": 100},
                },
                {
                    "dq_function": "expect_column_max_to_be_between",
                    "args": {"column": "so_net_value", "min_value": 10, "max_value": 1000},
                },
                {
                    "dq_function": "expect_column_value_lengths_to_be_between",
                    "args": {"column": "omnihub_locale_code", "min_value": 1, "max_value": 10},
                },
                {
                    "dq_function": "expect_column_mean_to_be_between",
                    "args": {"column": "coupon_code", "min_value": 15, "max_value": 20},
                },
            ],
        },
    ],
    "output_specs": [
        {
            "spec_id": "orders_silver",
            "input_id": "check_orders_bronze_with_extraction_date",
            "data_format": "delta",
            "write_type": "merge",
            "partitions": ["order_date_header"],
            "merge_opts": {
                "merge_predicate": """
                    new.sales_order_header = current.sales_order_header
                    AND new.sales_order_schedule = current.sales_order_schedule
                    AND new.sales_order_item=current.sales_order_item
                    AND new.epoch_status=current.epoch_status
                    AND new.changed_on=current.changed_on
                    AND new.extraction_date=current.extraction_date
                    AND new.lhe_batch_id=current.lhe_batch_id
                    AND new.lhe_row_id=current.lhe_row_id
                """,
                "insert_only": True,
            },
            "db_table": "your_database.order_events_with_dq",
            "options": {"checkpointLocation": "s3://my-data-product-bucket/checkpoints/template_order_events_with_dq/"},
        }
    ],
    "terminate_specs": [
        {
            "function": "optimize_dataset",
            "args": {"db_table": "your_database.order_events_with_dq"},
        }
    ],
    "exec_env": {
        "spark.databricks.delta.schema.autoMerge.enabled": True,
        "spark.databricks.delta.optimizeWrite.enabled": True,
        "spark.databricks.delta.autoCompact.enabled": True,
    },
}

load_data(acon=acon)
