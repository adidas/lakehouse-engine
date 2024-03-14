# Data Loader

## How to configure a DataLoader algorithm in the lakehouse-engine by using an ACON file?

An algorithm (e.g., data load) in the lakehouse-engine is configured using an ACON. The lakehouse-engine is a
configuration-driven framework, so people don't have to write code to execute a Spark algorithm. In contrast, the
algorithm is written in pyspark and accepts configurations through a JSON file (an ACON - algorithm configuration). The
ACON is the configuration providing the behaviour of a lakehouse engine algorithm. [You can check the algorithm code, and
how it interprets the ACON here](../lakehouse_engine/algorithms/algorithm.html).
In this page we will go through the structure of an ACON file and what are the most suitable ACON files for common data
engineering scenarios.
Check the underneath pages to find several **ACON examples** that cover many data extraction, transformation and loading scenarios.

## Overview of the Structure of the ACON file for DataLoads

An ACON-based algorithm needs several specifications to work properly, but some of them might be optional. The available
specifications are:

- **Input specifications (input_specs)**: specify how to read data. This is a **mandatory** keyword.
- **Transform specifications (transform_specs)**: specify how to transform data.
- **Data quality specifications (dq_specs)**: specify how to execute the data quality process.
- **Output specifications (output_specs)**: specify how to write data to the target. This is a **mandatory** keyword.
- **Terminate specifications (terminate_specs)**: specify what to do after writing into the target (e.g., optimising target table, vacuum, compute stats, expose change data feed to external location, etc.).
- **Execution environment (exec_env)**: custom Spark session configurations to be provided for your algorithm (configurations can also be provided from your job/cluster configuration, which we highly advise you to do instead of passing performance related configs here for example).

Below is an example of a complete ACON file that reads from a s3 folder with CSVs and incrementally loads that data (using a merge) into a delta lake table.

.. note::
    **spec_id** is one of the main concepts to ensure you can chain the steps of the algorithm, so, for example, you can specify the transformations (in transform_specs) of a DataFrame that was read in the input_specs. Check ACON below to see how the spec_id of the input_specs is used as input_id in one transform specification.

```python
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
        "dateFormat": "yyyyMMdd"
      },
      "location": "s3://my-data-product-bucket/bronze/orders/"
    }
  ],
  "transform_specs": [
    {
      "spec_id": "orders_bronze_with_extraction_date",
      "input_id": "orders_bronze",
      "transformers": [
        {
          "function": "with_row_id"
        },
        {
          "function": "with_regex_value",
          "args": {
            "input_col": "lhe_extraction_filepath",
            "output_col": "extraction_date",
            "drop_input_col": True,
            "regex": ".*WE_SO_SCL_(\\d+).csv"
          }
        }
      ]
    }
  ],
  "dq_specs": [
    {
      "spec_id": "check_orders_bronze_with_extraction_date",
      "input_id": "orders_bronze_with_extraction_date",
      "dq_type": "validator",
      "result_sink_db_table": "my_database.my_table_dq_checks",
      "fail_on_error": False,
      "dq_functions": [
        {
          "dq_function": "expect_column_values_to_not_be_null",
          "args": {
            "column": "omnihub_locale_code"
          }
        },
        {
          "dq_function": "expect_column_unique_value_count_to_be_between",
          "args": {
            "column": "product_division",
            "min_value": 10,
            "max_value": 100
          }
        },
        {
          "dq_function": "expect_column_max_to_be_between",
          "args": {
            "column": "so_net_value",
            "min_value": 10,
            "max_value": 1000
          }
        },
        {
          "dq_function": "expect_column_value_lengths_to_be_between",
          "args": {
            "column": "omnihub_locale_code",
            "min_value": 1,
            "max_value": 10
          }
        },
        {
          "dq_function": "expect_column_mean_to_be_between",
          "args": {
            "column": "coupon_code",
            "min_value": 15,
            "max_value": 20
          }
        }
      ]
    }
  ],
  "output_specs": [
    {
      "spec_id": "orders_silver",
      "input_id": "check_orders_bronze_with_extraction_date",
      "data_format": "delta",
      "write_type": "merge",
      "partitions": [
        "order_date_header"
      ],
      "merge_opts": {
        "merge_predicate": """
            new.sales_order_header = current.sales_order_header
            and new.sales_order_schedule = current.sales_order_schedule
            and new.sales_order_item=current.sales_order_item
            and new.epoch_status=current.epoch_status
            and new.changed_on=current.changed_on
            and new.extraction_date=current.extraction_date
            and new.lhe_batch_id=current.lhe_batch_id
            and new.lhe_row_id=current.lhe_row_id
        """,
        "insert_only": True
      },
      "db_table": "my_database.my_table_with_dq",
      "location": "s3://my-data-product-bucket/silver/order_events_with_dq/",
      "with_batch_id": True,
      "options": {
        "checkpointLocation": "s3://my-data-product-bucket/checkpoints/order_events_with_dq/"
      }
    }
  ],
  "terminate_specs": [
    {
      "function": "optimize_dataset",
      "args": {
        "db_table": "my_database.my_table_with_dq"
      }
    }
  ],
  "exec_env": {
    "spark.databricks.delta.schema.autoMerge.enabled": True
  }
}

load_data(acon=acon)
```

## Input Specifications

You specify how to read the data by providing a list of Input Specifications. Usually there's just one element in that
list, as, in the lakehouse, you are generally focused on reading data from one layer (e.g., source, bronze, silver,
gold) and put it on the next layer. However, there may be scenarios where you would like to combine two datasets (e.g.,
joins or incremental filtering on one dataset based on the values of another
one), therefore you can use one or more elements.
[More information about InputSpecs](../lakehouse_engine/core/definitions.html#InputSpec).

##### Relevant notes

- A spec id is fundamental, so you can use the input data later on in any step of the algorithm (transform, write, dq process, terminate).
- You don't have to specify `db_table` and `location` at the same time. Depending on the data_format sometimes you read from a table (e.g., jdbc or deltalake table) sometimes you read from a location (e.g., files like deltalake, parquet, json, avro... or kafka topic).

## Transform Specifications

In the lakehouse engine, you transform data by providing a transform specification, which contains a list of transform functions (transformers). So the transform specification acts upon on input, and it can execute multiple lakehouse engine transformation functions (transformers) upon that input.

If you look into the example above we ask the lakehouse engine to execute two functions on the `orders_bronze` input
data: `with_row_id` and `with_regex_value`. Those functions can of course receive arguments. You can see a list of all
available transformation functions (transformers) here `lakehouse_engine.transformers`. Then, you just invoke them in
your ACON as demonstrated above, following exactly the same function name and parameters name as described in the code
documentation. 
[More information about TransformSpec](../lakehouse_engine/core/definitions.html#TransformSpec).

##### Relevant notes

- This stage is fully optional, you can omit it from the ACON.
- There is one relevant option `force_streaming_foreach_batch_processing` that can be used to force the transform to be
  executed in the foreachBatch function to ensure non-supported streaming operations can be properly executed. You don't
  have to worry about this if you are using regular lakehouse engine transformers. But if you are providing your custom
  logic in pyspark code via our lakehouse engine
  custom_transformation (`lakehouse_engine.transformers.custom_transformers`) then sometimes your logic may contain
  Spark functions that are not compatible with Spark Streaming, and therefore this flag can enable all of your
  computation to be streaming-compatible by pushing down all the logic into the foreachBatch() function.

## Data Quality Specifications

One of the most relevant features of the lakehouse engine is that you can have data quality guardrails that prevent you
from loading bad data into your target layer (e.g., bronze, silver or gold). The lakehouse engine data quality process
includes two main features at the moment:

- **Assistant**: The capability to profile data out of a read (`input_spec`) or transform (`transform_spec`) stage and based
  on the profiling, display it and generate expectations that can be used for the validator.
- **Validator**: The capability to perform data quality checks on that data (e.g., is the max value of a column bigger
  than x?) and even tag your data with the results of the DQ checks.

The output of the data quality process can be written into a [**Result Sink**](data_quality/result_sink.html) target (e.g. table or files) and is integrated with a [Data Docs website](data_quality.html#3-data-docs-website), which can be a company-wide available website for people to check the quality of their data and share with others.

To achieve all of this functionality the lakehouse engine uses [Great Expectations](https://greatexpectations.io/) internally. To hide the Great Expectations internals from our user base and provide friendlier abstractions using the ACON, we have developed the concept of DQSpec that can contain many DQFunctionSpec objects, which is very similar to the relationship between the TransformSpec and TransformerSpec, which means you can have multiple Great Expectations functions executed inside a single data quality specification (as in the ACON above).

.. note::
   The names of the functions and args are a 1 to 1 match of [Great Expectations API](https://greatexpectations.io/expectations/).

[More information about DQSpec](../lakehouse_engine/core/definitions.html#DQSpec).

##### Relevant notes

- You can write the outputs of the DQ process to a sink through the result_sink* parameters of the
  DQSpec. `result_sink_options` takes any Spark options for a DataFrame writer, which means you can specify the options
  according to your sink format (e.g., delta, parquet, json, etc.). We usually recommend using `"delta"` as format.
- You can use the results of the DQ checks to tag the data that you are validating. When configured, these details will
  appear as a new column (like any other), as part of the tables of your Data Product.
- To be able to make an analysis with the data of `result_sink*`, we have available an approach in which you
  set `result_sink_explode` as true (which is the default) and then you have some columns expanded. Those are:
    - General columns: Those are columns that have the basic information regarding `dq_specs` and will have always values
      and does not depend on the expectation types chosen.
        -
      Columns: `checkpoint_config`, `run_name`, `run_time`, `run_results`, `success`, `validation_result_identifier`, `spec_id`, `input_id`, `validation_results`, `run_time_year`, `run_time_month`, `run_time_day`.
    - Statistics columns: Those are columns that have information about the runs of expectations, being those values for
      the run and not for each expectation. Those columns come from `run_results.validation_result.statistics.*`.
        - Columns: `evaluated_expectations`, `success_percent`, `successful_expectations`, `unsuccessful_expectations`.
    - Expectations columns: Those are columns that have information about the expectation executed.
        - Columns: `expectation_type`, `batch_id`, `expectation_success`, `exception_info`. Those columns are exploded
          from `run_results.validation_result.results`
          inside `expectation_config.expectation_type`, `expectation_config.kwargs.batch_id`, `success as expectation_success`,
          and `exception_info`. Moreover, we also include `unexpected_index_list`, `observed_value` and `kwargs`.
    - Arguments of Expectations columns: Those are columns that will depend on the expectation_type selected. Those
      columns are exploded from `run_results.validation_result.results` inside `expectation_config.kwargs.*`.
        - We can have for
          example: `column`, `column_A`, `column_B`, `max_value`, `min_value`, `value`, `value_pairs_set`, `value_set`,
          and others.
    - More columns desired? Those can be added, using `result_sink_extra_columns` in which you can select columns
      like `<name>` and/or explode columns like `<name>.*`.
- Use the parameter `"source"` to identify the data used for an easier analysis.
- By default, Great Expectation will also provide a site presenting the history of the DQ validations that you have performed on your data.
- You can make an analysis of all your expectations and create a dashboard aggregating all that information.
- This stage is fully optional, you can omit it from the ACON.

## Output Specifications

The output_specs section of an ACON is relatively similar to the input_specs section, but of course focusing on how to write the results of the algorithm, instead of specifying the input for the algorithm, hence the name output_specs (output specifications). [More information about OutputSpec](../lakehouse_engine/core/definitions.html#OutputSpec).

##### Relevant notes

- Respect the supported write types and output formats.
- One of the most relevant options to specify in the options parameter is the `checkpoint_location` when in streaming
  read mode, because that location will be responsible for storing which data you already read and transformed from the
  source, **when the source is a Spark Streaming compatible source (e.g., Kafka or S3 files)**.

## Terminate Specifications

The terminate_specs section of the ACON is responsible for some "wrapping up" activities like optimising a table,
vacuuming old files in a delta table, etc. With time the list of available terminators will likely increase (e.g.,
reconciliation processes), but for now we have the [following terminators](../lakehouse_engine/terminators.html).
This stage is fully optional, you can omit it from the ACON.
The most relevant now in the context of the lakehouse initiative are the following:

- [dataset_optimizer](../lakehouse_engine/terminators/dataset_optimizer.html)
- [cdf_processor](../lakehouse_engine/terminators/cdf_processor.html)
- [sensor_terminator](../lakehouse_engine/terminators/sensor_terminator.html)
- [notifier_terminator](../lakehouse_engine/terminators/notifiers/email_notifier.html)

[More information about TerminatorSpec](../lakehouse_engine/core/definitions.html#TerminatorSpec).

## Execution Environment

In the exec_env section of the ACON you can pass any Spark Session configuration that you want to define for the
execution of your algorithm. This is basically just a JSON structure that takes in any Spark Session property, so no
custom lakehouse engine logic. This stage is fully optional, you can omit it from the ACON.

.. note::
   Please be aware that Spark Session configurations that are not allowed to be changed when the Spark cluster is already
   running need to be passed in the configuration of the job/cluster that runs this algorithm, not here in this section.
   This section only accepts Spark Session configs that can be changed in runtime. Whenever you introduce an option make
   sure that it takes effect during runtime, as to the best of our knowledge there's no list of allowed Spark properties
   to be changed after the cluster is already running. Moreover, typically Spark algorithms fail if you try to modify a
   config that can only be set up before the cluster is running.
