# Data Quality Validator

DQValidator algorithm allows DQ Validations isolated from the data load (only read and apply data quality validations).
With this algorithm you have the capacity to apply the Lakehouse-Engine Data Quality Process,
using [Great Expectations](https://greatexpectations.io/expectations/) functions directly into a specific dataset also
making use of all the [InputSpecs](../../lakehouse_engine/core/definitions.html#InputSpec) available in the engine.

Validating the Data Quality, using this algorithm, is a matter of defining the data you want to read and the validations you want to do to your data, detailing the great expectations functions you want to apply on the data to assess its quality.

.. warning::
    **This algorithm also gives the possibility to restore a previous version of a delta table or delta files in case the DQ
    process raises any exception. Please use it carefully!!** You may lose important commits and data. Moreover, this will
    highly depend on the frequency that you run your Data Quality validations. If you run your data loads daily and Data
    Quality validations weekly, and you define the restore_prev_version to true, this means that the table will be restored
    to the previous version, but the error could have happened 4 or 5 versions before.

## When to use?

- **Post-Load validation**: check quality of data already loaded to a table/location
- **Pre-Load validation**: check quality of the data you want to load (check DQ by reading a set of files in a specific
  location...)
- **Validation of a DataFrame computed in the notebook itself** (e.g. check data quality after joining or filtering
  datasets, using the computed DataFrame as input for the validation)

This algorithm also gives teams some freedom to:

- **Schedule isolated DQ Validations to run periodically**, with the frequency they need;
- Define a DQ Validation process **as an end-to-end test** of the respective data product.

## How to use?

All of these configurations are passed via the ACON to instantiate
a [DQValidatorSpec object](../../lakehouse_engine/core/definitions.html#DQValidatorSpec). The DQValidator algorithm uses an
ACON to configure its execution. In [DQValidatorSpec](../../lakehouse_engine/core/definitions.html#DQValidatorSpec) you can
find the meaning of each ACON property.

Here is an example of ACON configuration:

```python
from lakehouse_engine.engine import load_data

acon = {
    "input_spec": {
        "spec_id": "sales_source",
        "read_type": "batch",
        "data_format": "table",
        "db_table": "my_database.my_table"
    },
    "dq_spec": {
        "spec_id": "dq_sales",
        "input_id": "sales_source",
        "dq_type": "validator",
        "store_backend": "file_system",
        "local_fs_root_dir": "/app/tests/lakehouse/in/feature/dq_validator/dq",
        "result_sink_db_table": "my_database.dq_validator",
        "result_sink_format": "json",
        "fail_on_error": False,
        "dq_functions": [
            {"function": "expect_column_to_exist", "args": {"column": "article"}},
            {
                "function": "expect_table_row_count_to_be_between",
                "args": {"min_value": 3, "max_value": 11},
            },
        ],
    },
    "restore_prev_version": True,
}

load_data(acon=acon)
```

On this page you will also find the following examples of usage:
1. Dataframe as input & Success on the DQ Validation
2. Table as input & Failure on DQ Validation & Restore previous version
3. Files as input & Failure on DQ Validation & Fail_on_error disabled
4. Files as input & Failure on DQ Validation & Critical functions defined
5. Files as input & Failure on DQ Validation & Max failure percentage defined


### Example 1 : Dataframe as input & Success on the DQ Validation

This example focuses on using a dataframe, computed in this notebook, directly in the input spec. First, a new
DataFrame is generated as a result of the join of data from two tables (dummy_deliveries and dummy_pd_article) and
some DQ Validations are applied on top of this dataframe.

```python
from lakehouse_engine.engine import execute_dq_validation

input_df = spark.sql("""
        SELECT a.*, b.article_category, b.article_color
        FROM my_database.dummy_deliveries a
        JOIN my_database.dummy_pd_article b
            ON a.article_id = b.article_id
        """
)

acon = {
    "input_spec": {
        "spec_id": "deliveries_article_input",
        "read_type": "batch",
        "data_format": "dataframe",
        "df_name": input_df,
    },
    "dq_spec": {
        "spec_id": "deliveries_article_dq",
        "input_id": "deliveries_article_input",
        "dq_type": "validator",
        "bucket": "my_data_product_bucket",
        "result_sink_db_table": "my_database.dq_validator_deliveries",
        "result_sink_location": "my_dq_path/dq_validator/dq_validator_deliveries/",
        "expectations_store_prefix": "dq/dq_validator/expectations/",
        "validations_store_prefix": "dq/dq_validator/validations/",
        "data_docs_prefix": "dq/dq_validator/data_docs/site/",
        "checkpoint_store_prefix": "dq/dq_validator/checkpoints/",
        "unexpected_rows_pk": ["salesorder", "delivery_item", "article_id"],
        "dq_functions": [{"function": "expect_column_values_to_not_be_null", "args": {"column": "delivery_date"}}],
    },
    "restore_prev_version": False,
}

execute_dq_validation(acon=acon)
```


### Example 2: Table as input & Failure on DQ Validation & Restore previous version

In this example we are using a table as input to validate the data that was loaded. Here, we are forcing the DQ Validations to fail in order to show the possibility of restoring the table to the previous version.

.. warning::
    **Be careful when using the feature of restoring a previous version of a delta table or delta files.** You may
    lose important commits and data. Moreover, this will highly depend on the frequency that you run your Data Quality
    validations. If you run your data loads daily and Data Quality validations weekly, and you define the
    restore_prev_version to true, this means that the table will be restored to the previous version, but the error
    could have happened 4 or 5 versions before (because loads are daily, validations are weekly).

Steps followed in this example to show how the restore_prev_version feature works.
1. **Insert rows into the dummy_deliveries table** to adjust the total numbers of rows and **make the DQ process fail**.
2. **Use the "DESCRIBE HISTORY" statement to check the number of versions available on the table** and check the version
   number resulting from the insertion to the table.
3. **Execute the DQ Validation**, using the configured acon (based on reading the dummy_deliveries table and setting the 
`restore_prev_version` to `true`). Checking the logs of the process, you can see that the data did not pass all the 
expectations defined and that the table version restore process was triggered.
4. **Re-run a "DESCRIBE HISTORY" statement to check that the previous version of the table was restored** and thus, the row inserted in the beginning of the process is no longer present in the table.

```python
from lakehouse_engine.engine import execute_dq_validation

# Force failure of data quality by adding new row
spark.sql("""INSERT INTO my_database.dummy_deliveries VALUES (7, 1, 20180601, 71, "article1", "delivered")""")


# Check history of the table
spark.sql("""DESCRIBE HISTORY my_database.dummy_deliveries""")

acon = {
    "input_spec": {
        "spec_id": "deliveries_input",
        "read_type": "batch",
        "db_table": "my_database.dummy_deliveries",
    },
    "dq_spec": {
        "spec_id": "dq_deliveries",
        "input_id": "deliveries_input",
        "dq_type": "validator",
        "bucket": "my_data_product_bucket",
        "data_docs_bucket": "my_dq_data_docs_bucket",
        "data_docs_prefix": "dq/my_data_product/data_docs/site/",
        "tbl_to_derive_pk": "my_database.dummy_deliveries",
        "dq_functions": [
            {"function": "expect_column_values_to_not_be_null", "args": {"column": "delivery_date"}},
            {"function": "expect_table_row_count_to_be_between", "args": {"min_value": 15, "max_value": 19}},
        ],
    },
    "restore_prev_version": True,
}

execute_dq_validation(acon=acon)
 
# Check that the previous version of the table was restored
spark.sql("""DESCRIBE HISTORY my_database.dummy_deliveries""")
```


### Example 3: Files as input & Failure on DQ Validation & Fail_on_error disabled

In this example we are using a location as input to validate the files in a specific folder.
Here, we are forcing the DQ Validations to fail, however disabling the "fail_on_error" configuration,
so the algorithm warns about the expectations that failed but the process/the execution of the algorithm doesn't fail.

```python
from lakehouse_engine.engine import execute_dq_validation

acon = {
    "input_spec": {
        "spec_id": "deliveries_input",
        "data_format": "delta",
        "read_type": "streaming",
        "location": "s3://my_data_product_bucket/silver/dummy_deliveries/",
    },
    "dq_spec": {
        "spec_id": "dq_deliveries",
        "input_id": "deliveries_input",
        "dq_type": "validator",
        "bucket": "my_data_product_bucket",
        "data_docs_bucket": "my_dq_data_docs_bucket",
        "data_docs_prefix": "dq/my_data_product/data_docs/site/",
        "tbl_to_derive_pk": "my_database.dummy_deliveries",
        "fail_on_error": False,
        "dq_functions": [
            {"function": "expect_column_values_to_not_be_null", "args": {"column": "delivery_date"}},
            {"function": "expect_table_row_count_to_be_between", "args": {"min_value": 15, "max_value": 17}},
        ],
    },
    "restore_prev_version": False,
}

execute_dq_validation(acon=acon)
```


### Example 4: Files as input & Failure on DQ Validation & Critical functions defined

In this example we are using a location as input to validate the files in a specific folder.
Here, we are forcing the DQ Validations to fail by using the critical functions feature, which will throw an error
if any of the functions fails.

```python
from lakehouse_engine.engine import execute_dq_validation

acon = {
    "input_spec": {
        "spec_id": "deliveries_input",
        "data_format": "delta",
        "read_type": "streaming",
        "location": "s3://my_data_product_bucket/silver/dummy_deliveries/",
    },
    "dq_spec": {
        "spec_id": "dq_deliveries",
        "input_id": "deliveries_input",
        "dq_type": "validator",
        "bucket": "my_data_product_bucket",
        "data_docs_bucket": "my_dq_data_docs_bucket",
        "data_docs_prefix": "dq/my_data_product/data_docs/site/",
        "tbl_to_derive_pk": "my_database.dummy_deliveries",
        "fail_on_error": True,
        "dq_functions": [
            {"function": "expect_column_values_to_not_be_null", "args": {"column": "delivery_date"}},
        ],
        "critical_functions": [
            {"function": "expect_table_row_count_to_be_between", "args": {"min_value": 15, "max_value": 17}},
        ],
    },
    "restore_prev_version": False,
}

execute_dq_validation(acon=acon)
```


### Example 5: Files as input & Failure on DQ Validation & Max failure percentage defined

In this example we are using a location as input to validate the files in a specific folder.
Here, we are forcing the DQ Validations to fail by using the max_percentage_failure,
which will throw an error if the percentage of failures surpasses the defined maximum threshold.

```python
from lakehouse_engine.engine import execute_dq_validation

acon = {
    "input_spec": {
        "spec_id": "deliveries_input",
        "data_format": "delta",
        "read_type": "streaming",
        "location": "s3://my_data_product_bucket/silver/dummy_deliveries/",
    },
    "dq_spec": {
        "spec_id": "dq_deliveries",
        "input_id": "deliveries_input",
        "dq_type": "validator",
        "bucket": "my_data_product_bucket",
        "data_docs_bucket": "my_dq_data_docs_bucket",
        "data_docs_prefix": "dq/my_data_product/data_docs/site/",
        "tbl_to_derive_pk": "my_database.dummy_deliveries",
        "fail_on_error": True,
        "dq_functions": [
            {"function": "expect_column_values_to_not_be_null", "args": {"column": "delivery_date"}},
            {"function": "expect_table_row_count_to_be_between", "args": {"min_value": 15, "max_value": 17}},
        ],
        "max_percentage_failure": 0.2,
    },
    "restore_prev_version": False,
}

execute_dq_validation(acon=acon)
```


## Limitations

Unlike DataLoader, this new DQValidator algorithm only allows, for now, one input_spec (instead of a list of input_specs) and one dq_spec (instead of a list of dq_specs). There are plans and efforts already initiated to make this available in the input_specs and one dq_spec (instead of a list of dq_specs). However, you can prepare a Dataframe which joins more than a source, and use it as input, in case you need to assess the Data Quality from different sources at the same time. Alternatively, you can also show interest on any enhancement on this feature, as well as contributing yourself.
