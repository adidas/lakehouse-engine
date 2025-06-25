# Custom Transformer

There may appear a scenario where the data product dev team faces the need to perform complex data transformations that are either not yet available in the lakehouse engine or the logic is just too complex to chain in an ACON file. In the context of the lakehouse, the only layers that usually can impose that complexity is silver+ and gold. This page targets exactly those cases.

Below you'll find a notebook where you can pass your own PySpark or Spark SQL logic into the ACON, by dynamically injecting a python function into the ACON dictionary. The lakehouse engine will take care of executing those transformations in the transformation step of the data loader algorithm. Please read the notebook's comments carefully to understand how it works, or simply open it in your notebook environment, which will make the notebook's code and comments more readable.

!!! warning "Force Streaming Micro Batch Processing."
    When you use streaming mode, with a custom transformer, it’s highly advisable that you set the `force_streaming_microbatch_processing` flag to `True` in the transform specification, as explained above!

## What is a custom transformer in the Lakehouse Engine and how you can use it to write your own pyspark logic?

We highly promote the Lakehouse Engine for creating Data Products aligned with the data source (bronze/silver layer), pumping data into silver so our Data Scientists and Analysts can leverage the value of the data in silver, as close as it comes from the source.
The low-code and configuration-driven nature of the lakehouse engine makes it a compelling framework to use in such cases, where the transformations that are done from bronze to silver are not that many, as we want to keep the data close to the source.

However, when it comes to Data Products enriched in some way or for insights (silver+, gold), they are typically heavy
on transformations (they are the T of the overall ELT process), so the nature of the lakehouse engine may would have
get into the way of adequately building it. Considering this, and considering our user base that prefers an ACON-based
approach and all the nice off-the-shelf features of the lakehouse engine, we have developed a feature that
allows us to **pass custom transformers where you put your entire pyspark logic and can pass it as an argument
in the ACON** (the configuration file that configures every lakehouse engine algorithm).

!!! note "Motivation"
    Doing that, you let the ACON guide your read, data quality, write and terminate processes, and you just focus on transforming data :)

## Custom transformation Function

The function below is the one that encapsulates all your defined pyspark logic and sends it as a python function to the lakehouse engine. This function will then be invoked internally in the lakehouse engine via a df.transform() function. If you are interested in checking the internals of the lakehouse engine, our codebase is openly available here: https://github.com/adidas/lakehouse-engine

!!! warning "Attention!!!"
    For this process to work, your function defined below needs to receive a DataFrame and return a DataFrame. Attempting any other method signature (e.g., defining more parameters) will not work, unless you use something like [python partials](https://docs.python.org/3/library/functools.html#functools.partial), for example.

```python
def get_new_data(df: DataFrame) -> DataFrame:
    """Get the new data from the lakehouse engine reader and prepare it."""
    return (
        df.withColumn("amount", when(col("_change_type") == "delete", lit(0)).otherwise(col("amount")))
        .select("article_id", "order_date", "amount")
        .groupBy("article_id", "order_date")
        .agg(sum("amount").alias("amount"))
    )


def get_joined_data(new_data_df: DataFrame, current_data_df: DataFrame) -> DataFrame:
    """Join the new data with the current data already existing in the target dataset."""
    return (
        new_data_df.alias("new_data")
        .join(
            current_data_df.alias("current_data"),
            [
                new_data_df.article_id == current_data_df.article_id,
                new_data_df.order_date == current_data_df.order_date,
            ],
            "left_outer",
        )
        .withColumn(
            "current_amount", when(col("current_data.amount").isNull(), lit(0)).otherwise("current_data.amount")
        )
        .withColumn("final_amount", col("current_amount") + col("new_data.amount"))
        .select(col("new_data.article_id"), col("new_data.order_date"), col("final_amount").alias("amount"))
    )


def calculate_kpi(df: DataFrame) -> DataFrame:
    """Calculate KPI through a custom transformer that will be provided in the ACON.
 
    Args:
        df: DataFrame passed as input.
 
    Returns:
        DataFrame: the transformed DataFrame.
    """
    new_data_df = get_new_data(df)

    # we prefer if you use 'ExecEnv.SESSION' instead of 'spark', because is the internal object the
    # lakehouse engine uses to refer to the spark session. But if you use 'spark' should also be fine.
    current_data_df = ExecEnv.SESSION.table(
        "my_database.my_table"
    )

    transformed_df = get_joined_data(new_data_df, current_data_df)

    return transformed_df
```

### Don't like pyspark API? Write SQL

You don't have to comply to the pyspark API if you prefer SQL. Inside the function above (or any of
the auxiliary functions you decide to develop) you can write something like:

````python
def calculate_kpi(df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("new_data")

    # we prefer if you use 'ExecEnv.SESSION' instead of 'spark', because is the internal object the
    # lakehouse engine uses to refer to the spark session. But if you use 'spark' should also be fine.
    ExecEnv.SESSION.sql(
        """
          CREATE OR REPLACE TEMP VIEW my_kpi AS
          SELECT ... FROM new_data ...
        """
    )

    return ExecEnv.SESSION.table("my_kpi")
````

## Just your regular ACON

If you notice the ACON below, everything is the same as you would do in a Data Product, but the `transform_specs` section of the ACON has a difference, which is a function called `"custom_transformation"` where we supply as argument the function defined above with the pyspark code.

!!! warning "Attention!!!"
    Do not pass the function as calculate_kpi(), but as calculate_kpi, otherwise you are telling python to invoke the function right away, as opposed to pass it as argument to be invoked later by the lakehouse engine.

```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "sales",
            "read_type": "streaming",
            "data_format": "delta",
            "db_table": "my_database.dummy_sales",
            "options": {"readChangeFeed": "true"},
        }
    ],
    "transform_specs": [
        {
            "spec_id": "transformed_sales_kpi",
            "input_id": "sales",
            # because we are using streaming, this allows us to make sure that
            # all the computation in our custom transformer gets pushed to
            # Spark's foreachBatch method in a stream, which allows us to
            # run all Spark functions in a micro batch DataFrame, as there
            # are some Spark functions that are not supported in streaming.
            "force_streaming_foreach_batch_processing": True,
            "transformers": [
                {
                    "function": "custom_transformation",
                    "args": {"custom_transformer": calculate_kpi},
                },
            ],
        }
    ],
    "dq_specs": [
        {
            "spec_id": "my_table_quality",
            "input_id": "transformed_sales_kpi",
            "dq_type": "validator",
            "bucket": "my_dq_bucket",
            "expectations_store_prefix": "dq/expectations/",
            "validations_store_prefix": "dq/validations/",
            "checkpoint_store_prefix": "dq/checkpoints/",
            "tbl_to_derive_pk": "my_table",
            "dq_functions": [
                {"function": "expect_column_values_to_not_be_null", "args": {"column": "article_id"}},
            ],
        },
    ],
    "output_specs": [
        {
            "spec_id": "sales_kpi",
            "input_id": "transformed_sales_kpi",
            "write_type": "merge",
            "data_format": "delta",
            "db_table": "my_database.my_table",
            "options": {
                "checkpointLocation": "s3://my_data_product_bucket/gold/my_table",
            },
            "merge_opts": {
                "merge_predicate": "new.article_id = current.article_id AND new.order_date = current.order_date"
            },
        }
    ],
}

load_data(acon=acon)
```