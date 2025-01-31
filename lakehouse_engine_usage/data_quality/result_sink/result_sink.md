# Result Sink

These scenarios store the results of the dq_specs into a result sink. For that, both scenarios include parameters defining
the specific table and location (`result_sink_db_table` and `result_sink_location`) where the results
are expected to be stored. With this configuration, people can, later on, check the history of the DQ
executions using the configured table/location, as shown bellow. You can configure saving the output of the
results in the result sink following two approaches:

- [**Denormalized/exploded Data Model (recommended)**](#1-result-sink-exploded-recommended) - the results are stored in a detailed format in which
people are able to analyse them by Data Quality Run, by expectation_type and by keyword arguments.

| ...                         | source     | column     | max_value | min_value | expectation_type                        | expectation_success | observed_value | run_time_year | ... |
|-----------------------------|------------|------------|-----------|-----------|-----------------------------------------|---------------------|----------------|---------------|-----|
| all columns from raw + more | deliveries | salesorder | null      | null      | expect_column_to_exist                  | TRUE                | null           | 2023          | ... |
| all columns from raw + more | deliveries | null       | null      | null      | expect_table_row_count_to_be_between    | TRUE                | 23             | 2023          | ... |
| all columns from raw + more | deliveries | null       | null      | null      | expect_table_column_count_to_be_between | TRUE                | 6              | 2023          | ... |

- [**Raw Format Data Model (not recommended)**](#2-raw-result-sink) - the results are stored in the raw format that Great
Expectations outputs. This is not recommended as the data will be highly nested and in a
string format (to prevent problems with schema changes), which makes analysis and the creation of a dashboard on top way 
harder.

| checkpoint_config    | run_name                   | run_time                         | run_results                   | success                | validation_result_identifier | spec_id | input_id |
|----------------------|----------------------------|----------------------------------|-------------------------------|------------------------|------------------------------|---------|----------|
| entire configuration | 20230323-...-dq_validation | 2023-03-23T15:11:32.225354+00:00 | results of the 3 expectations | true/false for the run | identifier                   | spec_id | input_id |

!!! note
    - More configurations can be applied in the result sink, as the file format and partitions.
    - It is recommended to:

        - Use the same result sink table/location for all dq_specs across different data loads, from different 
        sources, in the same Data Product.
        - Use the parameter `source` (only available with `"result_sink_explode": True`), in the dq_specs, as
        used in both scenarios, with the name of the data source, to be easier to distinguish sources in the
        analysis. If not specified, the `input_id` of the dq_spec will be considered as the `source`.
        - These recommendations will enable more rich analysis/dashboard at Data Product level, considering
        all the different sources and data loads that the Data Product is having.

## 1. Result Sink Exploded (Recommended)

This scenario stores DQ Results (results produces by the execution of the dq_specs) in the Result Sink,
in a detailed format, in which people are able to analyse them by Data Quality Run, by expectation_type and
by keyword arguments. This is the recommended approach since it makes the analysis on top of the result
sink way easier and faster.

For achieving the exploded data model, this scenario introduces the parameter `result_sink_explode`, which
is a flag to determine if the output table/location should have the columns exploded (as `True`) or
not (as `False`). **Default:** `True`, but it is still provided explicitly in this scenario for demo purposes.
The table/location will include a schema which contains general columns, statistic columns, arguments of
expectations, and others, thus part of the schema will be always with values and other part will depend on
the expectations chosen.

```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "dummy_deliveries_source",
            "read_type": "batch",
            "data_format": "csv",
            "options": {
                "header": True,
                "delimiter": "|",
                "inferSchema": True,
            },
            "location": "s3://my_data_product_bucket/dummy_deliveries/",
        }
    ],
    "dq_specs": [
        {
            "spec_id": "dq_validator",
            "input_id": "dummy_deliveries_source",
            "dq_type": "validator",
            "bucket": "my_data_product_bucket",
            "data_docs_bucket": "my_dq_data_docs_bucket",
            "data_docs_prefix": "dq/my_data_product/data_docs/site/",
            "result_sink_db_table": "my_database.dq_result_sink",
            "result_sink_location": "my_dq_path/dq_result_sink/",
            "result_sink_explode": True,
            "tbl_to_derive_pk": "my_database.dummy_deliveries",
            "source": "deliveries_success",
            "dq_functions": [
                {"function": "expect_column_to_exist", "args": {"column": "salesorder"}},
                {"function": "expect_table_row_count_to_be_between", "args": {"min_value": 15, "max_value": 25}},
                {"function": "expect_table_column_count_to_be_between", "args": {"max_value": 7}},
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "dummy_deliveries_bronze",
            "input_id": "dq_validator",
            "write_type": "overwrite",
            "data_format": "delta",
            "location": "s3://my_data_product_bucket/bronze/dummy_deliveries_dq_template/",
        }
    ],
}

load_data(acon=acon)
```

To check the history of the DQ results, you can run commands like:

- the table: `display(spark.table("my_database.dq_result_sink"))`
- the location: `display(spark.read.format("delta").load("my_dq_path/dq_result_sink/"))`

## 2. Raw Result Sink
This scenario is very similar to the previous one, but it changes the parameter `result_sink_explode` to `False` so that
it produces a raw result sink output containing only one row representing the full run of `dq_specs` (no
matter the amount of expectations/dq_functions defined there). Being a raw output, **it is not a
recommended approach**, as it will be more complicated to analyse and make queries on top of it.

```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "dummy_deliveries_source",
            "read_type": "batch",
            "data_format": "csv",
            "options": {
                "header": True,
                "delimiter": "|",
                "inferSchema": True,
            },
            "location": "s3://my_data_product_bucket/dummy_deliveries/",
        }
    ],
    "dq_specs": [
        {
            "spec_id": "dq_validator",
            "input_id": "dummy_deliveries_source",
            "dq_type": "validator",
            "bucket": "my_data_product_bucket",
            "data_docs_bucket": "my_dq_data_docs_bucket",
            "data_docs_prefix": "dq/my_data_product/data_docs/site/",
            "result_sink_db_table": "my_database.dq_result_sink_raw",
            "result_sink_location": "my_dq_path/dq_result_sink_raw/",
            "result_sink_explode": False,
            "tbl_to_derive_pk": "my_database.dummy_deliveries",
            "source": "deliveries_success_raw",
            "dq_functions": [
                {"function": "expect_column_to_exist", "args": {"column": "salesorder"}},
                {"function": "expect_table_row_count_to_be_between", "args": {"min_value": 15, "max_value": 25}},
                {"function": "expect_table_column_count_to_be_between", "args": {"max_value": 7}},
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "dummy_deliveries_bronze",
            "input_id": "dq_validator",
            "write_type": "overwrite",
            "data_format": "delta",
            "location": "s3://my_data_product_bucket/bronze/dummy_deliveries_dq_template/",
        }
    ],
}

load_data(acon=acon)
```

To check the history of the DQ results, you can run commands like:

- the table: `display(spark.table("my_database.dq_result_sink_raw"))`
- the location: `display(spark.read.format("delta").load("my_dq_path/dq_result_sink_raw/"))`
