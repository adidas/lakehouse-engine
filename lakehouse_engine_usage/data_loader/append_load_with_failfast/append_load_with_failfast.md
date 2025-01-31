# Append Load with FAILFAST

This scenario is an append load enforcing the schema (using the schema of the target table to enforce the schema of the source, i.e., the schema of the source needs to exactly match the schema of the target table) and FAILFASTING if the schema of the input data does not match the one we specified.

```python
from lakehouse_engine.engine import load_data

acon = {
  "input_specs": [
    {
      "spec_id": "sales_source",
      "read_type": "batch",
      "data_format": "csv",
      "enforce_schema_from_table": "test_db.failfast_table",
      "options": {
        "header": True,
        "delimiter": "|",
        "mode": "FAILFAST"
      },
      "location": "file:///app/tests/lakehouse/in/feature/append_load/failfast/data"
    },
    {
      "spec_id": "sales_bronze",
      "read_type": "batch",
      "db_table": "test_db.failfast_table"
    }
  ],
  "transform_specs": [
    {
      "spec_id": "max_sales_bronze_date",
      "input_id": "sales_bronze",
      "transformers": [
        {
          "function": "get_max_value",
          "args": {
            "input_col": "date"
          }
        }
      ]
    },
    {
      "spec_id": "appended_sales",
      "input_id": "sales_source",
      "transformers": [
        {
          "function": "incremental_filter",
          "args": {
            "input_col": "date",
            "increment_df": "max_sales_bronze_date"
          }
        }
      ]
    }
  ],
  "output_specs": [
    {
      "spec_id": "sales_bronze",
      "input_id": "appended_sales",
      "write_type": "append",
      "db_table": "test_db.failfast_table",
      "data_format": "delta",
      "partitions": [
        "date"
      ],
      "location": "file:///app/tests/lakehouse/out/feature/append_load/failfast/data"
    }
  ]
}

load_data(acon=acon)
```
##### Relevant notes

- The **ReadMode** is **FAILFAST** in this scenario, i.e., fail the algorithm if the schema of the input data does not match the one we specified via schema_path, read_schema_from_table or schema Input_specs variables.
- In this scenario we do an append load by getting the max date (transformer_spec ["get_max_value"](../../../reference/packages/transformers/aggregators.md#packages.transformers.aggregators.Aggregators.get_max_value)) on bronze and use that date to filter the source to only get data with a date greater than that max date on bronze (transformer_spec ["incremental_filter"](../../../reference/packages/transformers/filters.md#packages.transformers.filters.Filters.incremental_filter)). **That is the standard way we do incremental batch loads in the lakehouse engine.** For streaming incremental loads we rely on Spark Streaming checkpoint feature [(check a streaming append load ACON example)](../streaming_append_load_with_terminator/streaming_append_load_with_terminator.md).