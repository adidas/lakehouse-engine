# Write and Read Dataframe

DataFrame writer can give us some advantages by returning a dictionary containing the `spec_id` and the computed dataframe.
In these examples we will cover the following scenarios of using the output `dataframe` format:

1. [**Write to dataframe**: Consuming the output spec as DataFrame;](#1-write-to-dataframe-consuming-the-output-spec-as-dataframe)
2. [**Write all dataframes**: Consuming all DataFrames generated per specs;](#2-write-all-dataframes-consuming-all-dataframes-generated-per-specs)
3. [**Read from and Write to dataframe**: Making use of the DataFrame output spec to compose silver data.](#3-read-from-and-write-to-dataframe-making-use-of-the-dataframe-output-spec-to-compose-silver-data)

#### Main advantages of using this output writer:

- **Debugging purposes**: as we can access any dataframe used in any part of our ACON
  we can observe what is happening with the computation and identify what might be wrong
  or can be improved.
- **Flexibility**: in case we have some very specific need not covered yet by the lakehouse
  engine capabilities, example: return the Dataframe for further processing like using a machine
  learning model/prediction.
- **Simplify ACONs**: instead developing a single complex ACON, using the Dataframe writer,
  we can compose our ACON from the output of another ACON. This allows us to identify
  and split the notebook logic across ACONs.

If you want/need, you can add as many dataframes as you want in the output spec
referencing the spec_id you want to add.

.. warning::
  **This is not intended to replace the other capabilities offered by the
  lakehouse-engine** and in case **other feature can cover your use case**,
  you should **use it instead of using the Dataframe writer**, as they
  are much **more extensively tested on different type of operations**.
  
  *Additionally, please always introspect if the problem that you are trying to resolve and for which no lakehouse-engine feature is available, could be a common problem and thus deserve a common solution and feature.*
  
  Moreover, **Dataframe writer is not supported for the streaming trigger
  types `processing time` and `continuous`.**

## 1. Write to dataframe: Consuming the output spec as DataFrame

### Silver Dummy Sales Write to DataFrame

In this example we will cover the Dummy Sales write to a result containing the output DataFrame.

- An ACON is used to read from bronze, apply silver transformations and write to a dictionary
  containing the output spec as key and the dataframe as value through the following steps:
    - 1 - Definition of how to read data (input data location, read type and data format);
    - 2 - Transformation of data (rename relevant columns);
    - 3 - Write the data to dict containing the dataframe;

.. note:: If you are trying to retrieve more than once the same data using checkpoint it will return an empty dataframe with empty schema as we don't have new data to read.


```python
from lakehouse_engine.engine import load_data

cols_to_rename = {"item": "ordered_item", "date": "order_date", "article": "article_id"}

acon = {
    "input_specs": [
        {
            "spec_id": "dummy_sales_bronze",
            "read_type": "streaming",
            "data_format": "delta",
            "location": "s3://my_data_product_bucket/bronze/dummy_sales",
        }
    ],
    "transform_specs": [
        {
            "spec_id": "dummy_sales_transform",
            "input_id": "dummy_sales_bronze",
            "transformers": [
                {
                    "function": "rename",
                    "args": {
                        "cols": cols_to_rename,
                    },
                },
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "dummy_sales_silver",
            "input_id": "dummy_sales_transform",
            "data_format": "dataframe",
            "options": {
                "checkpointLocation": "s3://my_data_product_bucket/checkpoints/bronze/dummy_sales",
            },
        }
    ],
}
```

### Run the Load and Return the Dictionary with the DataFrames by OutputSpec

This exploratory test will return a dictionary with the output spec and the dataframe
that will be stored after transformations.

```python
output = load_data(acon=acon)
display(output.keys())
display(output.get("dummy_sales_silver"))
```

## 2. Write all dataframes: Consuming all DataFrames generated per specs

### Silver Dummy Sales Write to DataFrame

In this example we will cover the Dummy Sales write to a result containing the specs and related DataFrame.

- An ACON is used to read from bronze, apply silver transformations and write to a dictionary
  containing the spec id as key and the DataFrames as value through the following steps:
    - Definition of how to read data (input data location, read type and data format);
    - Transformation of data (rename relevant columns);
    - Write the data to a dictionary containing all the spec ids and DataFrames computed per step;

```python
from lakehouse_engine.engine import load_data

cols_to_rename = {"item": "ordered_item", "date": "order_date", "article": "article_id"}

acon = {
    "input_specs": [
        {
            "spec_id": "dummy_sales_bronze",
            "read_type": "batch",
            "data_format": "delta",
            "location": "s3://my_data_product_bucket/bronze/dummy_sales",
        }
    ],
    "transform_specs": [
        {
            "spec_id": "dummy_sales_transform",
            "input_id": "dummy_sales_bronze",
            "transformers": [
                {
                    "function": "rename",
                    "args": {
                        "cols": cols_to_rename,
                    },
                },
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "sales_bronze",
            "input_id": "dummy_sales_bronze",
            "data_format": "dataframe",
        },
        {
            "spec_id": "sales_silver",
            "input_id": "dummy_sales_transform",
            "data_format": "dataframe",
        },
    ],
}
```

### Run the Load and Return the Dictionary with the related DataFrames by Spec

This exploratory test will return a dictionary with all specs and the related dataframe.
You can access the DataFrame you need by `output.get(<spec_id>)` for future developments and tests.

```python
output = load_data(acon=acon)
display(output.keys())
display(output.get("sales_bronze"))
display(output.get("sales_silver"))
```

## 3. Read from and Write to dataframe: Making use of the DataFrame output spec to compose silver data

### Silver Load Dummy Deliveries

In this example we will cover the Dummy Deliveries table read and incremental load to silver composing the silver data to write using the DataFrame output spec:

- First ACON is used to get the latest data from bronze, in this step we are using more than one output because we will need the bronze data with the latest data in the next step.
- Second ACON is used to consume the bronze data and the latest data to perform silver transformation, in this ACON we are using as **input the two dataframes computed by the first ACON.**
- Third ACON is used to write the silver computed data from the previous ACON to the target.

.. note:: This example is not a recommendation on how to deal with incremental loads, the ACON was split in 3 for demo purposes.

Consume bronze data, generate the latest data and return a dictionary with bronze and transformed dataframes:

```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "dummy_deliveries_bronze",
            "read_type": "batch",
            "data_format": "delta",
            "location": "s3://my_data_product_bucket/bronze/dummy_sales",
        },
        {
            "spec_id": "dummy_deliveries_silver_source",
            "read_type": "batch",
            "data_format": "delta",
            "db_table": "my_database.dummy_deliveries",
        },
    ],
    "transform_specs": [
        {
            "spec_id": "dummy_deliveries_table_max_value",
            "input_id": "dummy_deliveries_silver_source",
            "transformers": [
                {
                    "function": "get_max_value",
                    "args": {"input_col": "delivery_date", "output_col": "latest"},
                },
                {
                    "function": "with_expressions",
                    "args": {
                        "cols_and_exprs": {"latest": "CASE WHEN latest IS NULL THEN 0 ELSE latest END"},
                    },
                },
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "deliveries_bronze",
            "input_id": "dummy_deliveries_bronze",
            "data_format": "dataframe",
        },
        {
            "spec_id": "dummy_deliveries_transformed",
            "input_id": "dummy_deliveries_table_max_value",
            "data_format": "dataframe",
        },
    ],
}

dummy_deliveries_transformed = load_data(acon=acon)

dummy_deliveries_transformed_df = dummy_deliveries_transformed.get("dummy_deliveries_transformed")
dummy_deliveries_bronze_df = dummy_deliveries_transformed.get("deliveries_bronze")
```

Consume previous dataframes generated by the first ACON (bronze and latest bronze data) to generate the silver data. In this acon we are only using **just one output** because we only need the dataframe from the output for the next step.

```python
from lakehouse_engine.engine import load_data

cols_to_rename = {"delivery_note_header": "delivery_note", "article": "article_id"}

acon = {
    "input_specs": [
        {
            "spec_id": "dummy_deliveries_bronze",
            "read_type": "batch",
            "data_format": "dataframe",
            "df_name": dummy_deliveries_bronze_df,
        },
        {
            "spec_id": "dummy_deliveries_table_max_value",
            "read_type": "batch",
            "data_format": "dataframe",
            "df_name": dummy_deliveries_transformed_df,
        },
    ],
    "transform_specs": [
        {
            "spec_id": "dummy_deliveries_transform",
            "input_id": "dummy_deliveries_bronze",
            "transformers": [
                {
                    "function": "rename",
                    "args": {
                        "cols": cols_to_rename,
                    },
                },
                {
                    "function": "incremental_filter",
                    "args": {
                        "input_col": "delivery_date",
                        "increment_df": "dummy_deliveries_table_max_value",
                        "increment_col": "latest",
                        "greater_or_equal": False,
                    },
                },
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "dummy_deliveries_silver",
            "input_id": "dummy_deliveries_transform",
            "data_format": "dataframe",
        }
    ],
}

dummy_deliveries_silver = load_data(acon=acon)
dummy_deliveries_silver_df = dummy_deliveries_silver.get("dummy_deliveries_silver")
```

Write the silver data generated by previous ACON into the target

```python
from lakehouse_engine.engine import load_data

write_silver_acon = {
    "input_specs": [
        {
            "spec_id": "dummy_deliveries_silver",
            "read_type": "batch",
            "data_format": "dataframe",
            "df_name": dummy_deliveries_silver_df,
        },
    ],
    "dq_specs": [
        {
            "spec_id": "dummy_deliveries_quality",
            "input_id": "dummy_deliveries_silver",
            "dq_type": "validator",
            "bucket": "my_data_product_bucket",
            "expectations_store_prefix": "dq/expectations/",
            "validations_store_prefix": "dq/validations/",
            "data_docs_prefix": "dq/data_docs/site/",
            "checkpoint_store_prefix": "dq/checkpoints/",
            "result_sink_db_table": "my_database.dummy_deliveries_dq",
            "result_sink_location": "my_data_product_bucket/dq/dummy_deliveries",
            "fail_on_error": False,
            "tbl_to_derive_pk": "my_database.dummy_deliveries",
            "dq_functions": [
                {
                    "function": "expect_column_values_to_not_be_null",
                    "args": {"column": "delivery_note"},
                },
                {
                    "function": "expect_table_row_count_to_be_between",
                    "args": {"min_value": 19},
                },
                {
                    "function": "expect_column_max_to_be_between",
                    "args": {"column": "delivery_item", "min_value": 2},
                },
            ],
        },
    ],
    "output_specs": [
        {
            "spec_id": "dummy_deliveries_silver",
            "input_id": "dummy_deliveries_quality",
            "write_type": "append",
            "location": "s3://my_data_product_bucket/silver/dummy_deliveries_df_writer",
            "data_format": "delta",
        }
    ],
    "exec_env": {
        "spark.databricks.delta.schema.autoMerge.enabled": True,
        "spark.databricks.delta.optimizeWrite.enabled": True,
        "spark.databricks.delta.autoCompact.enabled": True,
    },
}

load_data(acon=write_silver_acon)
```