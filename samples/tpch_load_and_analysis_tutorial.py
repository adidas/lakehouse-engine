# Databricks notebook source
# MAGIC %md
# MAGIC ### How to use the Lakehouse Engine to load and analyse Data
# MAGIC This sample is composed of two main sections and goals:
# MAGIC 1. **Data Load (integrate data into the Lakehouse)**
# MAGIC     - load 2 data sources
# MAGIC     - join both sources and enhance the dataset with more information
# MAGIC     - write the output into a target table
# MAGIC 2. **Data Analysis (analyse the data ingested in the previous step)**
# MAGIC     - read the ingested data
# MAGIC     - assess the quality of that data
# MAGIC     - output this data as a DataFrame to enable further processing
# MAGIC     - analyse the data with sample Databricks Notebook Dashboards
# MAGIC
# MAGIC The base dataset used, on this sample, is the TPCH Dataset from Databricks Datasets (https://docs.databricks.com/en/discover/databricks-datasets.html).
# MAGIC Moreover, Databricks Notebook Dashboards are also used. This is why this example consists of a Databricks python Notebook, instead of simple raw python.

# COMMAND ----------

# You can install the Lakehouse Engine framework with below command just like any other python library,
# or you can also install it as a cluster-scoped library
pip install lakehouse-engine

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Data Load
# MAGIC On this section an example is provided in order to accomplish the following:
# MAGIC - loading `orders` and `customers` TPCH data
# MAGIC - add current date, join both data sources and identify Super VIPs
# MAGIC - write data into the final table
# MAGIC
# MAGIC **Note:** as it can be seen in the following code, the Lakehouse Engine cannot offer transformers for everything one might want to do on the data, as there may be very specific use cases. This is why the Lakehouse Engine provides full flexibility with Custom Transformations (`custom_transformation`), which can be used to pass any custom function, as the `is_a_super_vip` function used on this example. 

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import DataFrame

def is_a_super_vip(df: DataFrame) -> DataFrame:
    """Example of custom transformation.
    
    It checks if the totalprice for a particular order is within the 
    10% higher and if the order priority is URGENT.
    If both criterias are met, the customer is considered a super vip.

    Args:
        df: DataFrame passed as input.

    Returns:
        DataFrame: the transformed DataFrame.
    """
    percentile_90 = df.approxQuantile("o_totalprice", [0.9], 0)[0]
    df = df.withColumn(
            "is_a_super_vip", 
            (col("o_totalprice") >= percentile_90) & 
            (col("o_orderpriority") == "1-URGENT")
        )
    return df

# COMMAND ----------

acon = {
        "input_specs": [
            # Batch (streaming is also supported) read tpch orders delta files from Databricks datasets location
            {
                "spec_id": "tpch_orders",
                "read_type": "batch",
                "data_format": "delta",
                "location": "/databricks-datasets/tpch/delta-001/orders",
            },
            # Batch read tpch customers from a samples delta table in Databricks
            {
                "spec_id": "tpch_customer",
                "read_type": "batch",
                "data_format": "delta",
                "db_table": "samples.tpch.customer",
            }
        ],
        "transform_specs": [
            {
                "spec_id": "tpch_orders_transformed",
                "input_id": "tpch_orders",
                "transformers": [
                    # Add current date to easily track when a particular row was added
                    {
                        "function": "add_current_date",
                        "args": {
                            "output_col": "lak_load_date"
                        }
                    },
                    # Join orders with customers to get the customer name.
                    # Having customer name in the table will make analysis easier
                    {
                        "function": "join",
                        "args": {
                            "join_with": "tpch_customer",
                            "join_type": "left outer",
                            "join_condition": "a.o_custkey = b.c_custkey",
                            "select_cols": ["a.*", "b.c_name as customer_name"]
                        }
                    },
                    # Custom transformation to assess if a customer should be considered Super VIP.
                    {
                        "function": "custom_transformation",
                        "args": {"custom_transformer": is_a_super_vip},
                    }
                ],
            },
        ],
        "output_specs": [
            # Overwrite data into an external table on top of the specified location, using delta data format.
            # Note: other write types are supported, such as append and merge, but overwrite is used for simplicity on this demo.
            {
                "spec_id": "tpch_orders_output",
                "input_id": "tpch_orders_transformed",
                "write_type": "overwrite",
                "db_table": "your_database.tpch_orders",
                "location": "s3://your_s3_bucket/silver/tpch_orders/",
                "data_format": "delta",
            }
        ],
    }

from lakehouse_engine.engine import load_data

tpch_df = load_data(acon=acon)

# COMMAND ----------

# As soon as the algorithm is finished, the dataframe output of the framework can be directly checked in order to analyse the data that have been just produced
display(tpch_df["tpch_orders_output"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Data Analysis
# MAGIC On this section an example is provided in order to accomplish the following:
# MAGIC - reading the data loaded on the previous step, using a SQL query
# MAGIC - assess the quality of the data, by applying Data Quality functions/expectations
# MAGIC - output the data as a DataFrame for further processing
# MAGIC - analyse the data with sample Databricks Notebook Dashboards

# COMMAND ----------

acon = {
        "input_specs": [
            # Batch read a custom SQL query from the table we have just inserted data into
            {
                "spec_id": "tpch_orders",
                "read_type": "batch",
                "data_format": "sql",
                "query": """
                    SELECT o_orderkey, customer_name, o_totalprice, is_a_super_vip
                    FROM your_database.tpch_orders
                """,
            },
        ],
        "dq_specs": [
            # Assess the quality of data, by ensuring that the specified 3 columns have no nulls.
            {
                "spec_id": "tpch_orders_dq",
                "input_id": "tpch_orders",
                "dq_type": "validator",
                "bucket": "your_s3_bucket",
                "dq_functions": [
                    {"function": "expect_column_values_to_not_be_null", "args": {"column": "o_orderkey"}},
                    {"function": "expect_column_values_to_not_be_null", "args": {"column": "customer_name"}},
                    {"function": "expect_column_values_to_not_be_null", "args": {"column": "o_totalprice"}}
                    ]
            },
        ],
        "output_specs": [
            # As the data is being analysed, there is no need to write it into any table or location.
            # Thus, the data output is just a Dataframe that can be used for further debug or processing.
            {
                "spec_id": "validated_tpch_orders",
                "input_id": "tpch_orders_dq",
                "data_format": "dataframe",
            }
        ],
    }

from lakehouse_engine.engine import load_data

validated_tpch_df = load_data(acon=acon)

# COMMAND ----------

# Create a Temporary View to make it easier to interact with the Data using SQL
validated_tpch_df["validated_tpch_orders"].createOrReplaceTempView("tpch_order_analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the data that came from the previous load_data algorithm execution can now be queried
# MAGIC -- to analyse the customers and orders classified as SUPER VIP
# MAGIC SELECT customer_name, o_totalprice, is_a_super_vip
# MAGIC FROM tpch_order_analysis
# MAGIC GROUP BY customer_name, o_totalprice, is_a_super_vip
# MAGIC ORDER BY o_totalprice desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_name, o_totalprice
# MAGIC FROM tpch_order_analysis
# MAGIC WHERE is_a_super_vip is True
# MAGIC GROUP BY customer_name, o_totalprice
# MAGIC ORDER BY o_totalprice desc
# MAGIC LIMIT 10

# COMMAND ----------


