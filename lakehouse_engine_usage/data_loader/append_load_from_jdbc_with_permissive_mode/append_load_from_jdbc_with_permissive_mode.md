# Append Load from JDBC with PERMISSIVE mode (default)

This scenario is an append load from a JDBC source (e.g., SAP BW, Oracle Database, SQL Server Database...).

```python
from lakehouse_engine.engine import load_data

acon = {
  "input_specs": [
    {
      "spec_id": "sales_source",
      "read_type": "batch",
      "data_format": "jdbc",
      "jdbc_args": {
        "url": "jdbc:sqlite:/app/tests/lakehouse/in/feature/append_load/jdbc_permissive/tests.db",
        "table": "jdbc_permissive",
        "properties": {
          "driver": "org.sqlite.JDBC"
        }
      },
      "options": {
        "numPartitions": 1
      }
    },
    {
      "spec_id": "sales_bronze",
      "read_type": "batch",
      "db_table": "test_db.jdbc_permissive_table"
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
      "db_table": "test_db.jdbc_permissive_table",
      "data_format": "delta",
      "partitions": [
        "date"
      ],
      "location": "file:///app/tests/lakehouse/out/feature/append_load/jdbc_permissive/data"
    }
  ]
}

load_data(acon=acon)
```

##### Relevant notes

- The **ReadMode** is **PERMISSIVE** in this scenario, which **is the default in Spark**, hence we **don't need to specify it**. Permissive means don't enforce any schema on the input data. 
- From a JDBC source the ReadType needs to be "batch" always as "streaming" is not available for a JDBC source.
- In this scenario we do an append load by getting the max date (transformer_spec ["get_max_value"](../../../reference/packages/transformers/aggregators.md#packages.transformers.aggregators.Aggregators.get_max_value)) on bronze and use that date to filter the source to only get data with a date greater than that max date on bronze (transformer_spec ["incremental_filter"](../../../reference/packages/transformers/filters.md#packages.transformers.filters.Filters.incremental_filter)). **That is the standard way we do incremental batch loads in the lakehouse engine.** For streaming incremental loads we rely on Spark Streaming checkpoint feature [(check a streaming append load ACON example)](../streaming_append_load_with_terminator/streaming_append_load_with_terminator.md).