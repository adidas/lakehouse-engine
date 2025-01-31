# Minimal Example

This scenario illustrates the minimal configuration that you can have to use `dq_specs`, in which
it uses required parameters: `spec_id, input_id, dq_type, bucket, dq_functions` and the optional
parameter `data_docs_bucket`. This parameter allows you to store the GX documentation in another
bucket that can be used to make your data docs available, in DQ Web App (GX UI), without giving users access to your bucket.
The`data_docs_bucket` property supersedes the `bucket` property only for data docs storage.

Regarding the dq_functions, it uses 3 functions (retrieved from the expectations supported by GX), which check:

- **expect_column_to_exist** - if a column exist in the data;
- **expect_table_row_count_to_be_between** - if the row count of the data is between the defined interval;
- **expect_table_column_count_to_be_between** - if the number of columns in the data is bellow the max value defined.


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
            "tbl_to_derive_pk": "my_database.dummy_deliveries",
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