# Write to Console

Console writer is an interesting feature to debug / validate what have been done on lakehouse engine. Before moving forward and store data somewhere, it is possible to show / print the final dataframe to the console, which means it is possible to transform the data as many times as you want and display the final result to validate if it is as expected.

## Silver Dummy Sales Write to Console Example

In this template we will cover the Dummy Sales write to console. An ACON is used to read from bronze, apply silver transformations and write on console through the following steps:

1. Definition of how to read data (input data location, read type and data format);
2. Transformation of data (rename relevant columns);
3. Definition of how to print to console (limit, truncate, vertical options);

For this, the ACON specs are :

- **input_specs** (MANDATORY): specify how to read data;
- **transform specs** (OPTIONAL): specify how to transform data;
- **output_specs** (MANDATORY): specify how to write data to the target.

.. note:: Writer to console **is a wrapper for spark.show() function**, if you want to know more about the function itself or the available options, [please check the spark documentation here](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html).

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
            "data_format": "console",
            "options": {"limit": 8, "truncate": False, "vertical": False},
        }
    ],
}
```

And then, **Run the Load and Exit the Notebook**: This exploratory test will write to the console, which means the final
dataframe will be displayed.

```python
load_data(acon=acon)
```
