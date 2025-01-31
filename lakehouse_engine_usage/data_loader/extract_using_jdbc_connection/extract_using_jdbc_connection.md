# Extract using JDBC connection

!!! danger "**SAP Extraction**"

    SAP is only used as an example to demonstrate how we can use a JDBC connection to extract data.

    **If you are looking to extract data from SAP, please use our sap_b4 or sap_bw reader.**

    You can find the **sap_b4 reader** documentation: [Extract from SAP B4 ADSOs](../../data_loader/extract_from_sap_b4_adso/extract_from_sap_b4_adso.md) and the **sap_bw reader** documentarion: [Extract from SAP BW DSOs](../../data_loader/extract_from_sap_bw_dso/extract_from_sap_bw_dso.md)

!!! danger "**Parallel Extraction**"
    Parallel extractions **can bring a jdbc source down** if a lot of stress is put on the system. Be careful choosing the number of partitions. Spark is a distributed system and can lead to many connections.

## Introduction

Many databases allow a JDBC connection to extract data. Our engine has one reader where you can configure all the necessary definitions to connect to a database using JDBC.

In the next section you will find several examples about how to do it.

## The Simplest Scenario using sqlite 
!!! warning "Not parallel"
    Recommended for smaller datasets only, or when stressing the source system is a high concern

This scenario is the simplest one we can have, not taking any advantage of Spark JDBC optimisation techniques and using a single connection to retrieve all the data from the source.

Here we use a sqlite database where any connection is allowed. Due to that, we do not specify any username or password.

Same as spark, we provide two different ways to run jdbc reader.

1 - We can use the **jdbc() function**, passing inside all the arguments needed for Spark to work, and we can even combine this with additional options passed through .options().

2 - Other way is using **.format("jdbc")** and pass all necessary arguments through .options(). It's important to say by choosing jdbc() we can also add options() to the execution.


**You can find and run the following code in our local test for the engine.**

### jdbc() function

As we can see in the next cell, all the arguments necessary to establish the jdbc connection are passed inside the `jdbc_args` object. Here we find the url, the table, and the driver. Besides that, we can add options, such as the partition number. The partition number will impact in the queries' parallelism.

The below code is an example in how to use jdbc() function in our ACON.
As for other cases, the acon configuration should be executed with `load_data` using:
```python
from lakehouse_engine.engine import load_data
acon = {...}
load_data(acon=acon)
```
Example of ACON configuration:
```json
{!../../../../tests/resources/feature/jdbc_reader/jdbc_function/correct_arguments/batch_init.json!}
```

This is same as using the following code in pyspark:

```python
spark.read.jdbc(
  url="jdbc:sqlite:/app/tests/lakehouse/in/feature/jdbc_reader/jdbc_function/correct_arguments/tests.db",
  table="jdbc_function",
  properties={"driver":"org.sqlite.JDBC"})
  .option("numPartitions", 1)
```

### .format("jdbc")

In this example we do not use the `jdbc_args` object. All the jdbc connection parameters are inside the dictionary with the object options.
As for other cases, the acon configuration should be executed with `load_data` using:
```python
from lakehouse_engine.engine import load_data
acon = {...}
load_data(acon=acon)
```
Example of ACON configuration:
```json
{!../../../../tests/resources/feature/jdbc_reader/jdbc_format/correct_arguments/batch_init.json!}
```

This is same as using the following code in pyspark:

```python
spark.read.format("jdbc")
    .option("url", "jdbc:sqlite:/app/tests/lakehouse/in/feature/jdbc_reader/jdbc_format/correct_arguments/tests.db")
    .option("driver", "org.sqlite.JDBC")
    .option("dbtable", "jdbc_format")
    .option("numPartitions", 1)
```

## Template with more complete and runnable examples
In this template we will use a **SAP as example** for a more complete and runnable example.
These definitions can be used in several databases that allow JDBC connection.

The following scenarios of extractions are covered:

- 1 - The Simplest Scenario (Not parallel -  Recommended for smaller datasets only,
or when stressing the source system is a high concern)
- 2 - Parallel extraction
  - 2.1 - Simplest Scenario 
  - 2.2 - Provide upperBound (Recommended)
  - 2.3 - Provide predicates (Recommended)

!!! note "Disclaimer"
    This template only uses **SAP as demonstration example for JDBC connection.**
    **This isn't a SAP template!!!**
    **If you are looking to extract data from SAP, please use our sap_b4 reader or the sap_bw reader.**

The JDBC connection has 2 main sections to be filled, the **jdbc_args** and **options**:

- jdbc_args - Here you need to fill everything related to jdbc connection itself, like table/query, url, user,
..., password.
- options - This section is more flexible, and you can provide additional options like "fetchSize", "batchSize",
"numPartitions", ..., upper and "lowerBound".

If you want to know more regarding jdbc spark options you can follow the link below:

- https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

If you want to have a better understanding about JDBC Spark optimizations, you can find them in the following:

- https://docs.databricks.com/en/connect/external-systems/jdbc.html
- https://stackoverflow.com/questions/41085238/what-is-the-meaning-of-partitioncolumn-lowerbound-upperbound-numpartitions-pa
- https://newbedev.com/how-to-optimize-partitioning-when-migrating-data-from-jdbc-source

### 1 - The Simplest Scenario (Not parallel - Recommended for smaller datasets, or for not stressing the source)
This scenario is the simplest one we can have, not taking any advantage of Spark JDBC optimisation techniques
and using a single connection to retrieve all the data from the source. It should only be used in case the data
you want to extract from is a small one, with no big requirements in terms of performance to fulfill.

When extracting from the source, we can have two options:

- **Delta Init** - full extraction of the source. You should use it in the first time you extract from the
source or any time you want to re-extract completely. Similar to a so-called full load.
- **Delta** - extracts the portion of the data that is new or has changed in the source, since the last
extraction (for that, the logic at the transformation step needs to be applied). On the examples below,
the logic using REQTSN column is applied, which means that the maximum value on bronze is filtered
and its value is used to filter incoming data from the data source.

##### Init - Load data into the Bronze Bucket
```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "jdbc",
            "jdbc_args": {
                "url": "my_sap_b4_url",
                "table": "my_database.my_table",
                "properties": {
                    "user": "my_user",
                    "password": "my_b4_hana_pwd",
                    "driver": "com.sap.db.jdbc.Driver",
                },
            },
            "options": {
                "fetchSize": 100000,
                "compress": True,
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": "overwrite",
            "data_format": "delta",
            "partitions": ["REQTSN"],
            "location": "s3://my_path/jdbc_template/no_parallel/my_identifier/",
        }
    ],
    "exec_env": {
        "spark.databricks.delta.schema.autoMerge.enabled": True,
        "spark.databricks.delta.optimizeWrite.enabled": True,
        "spark.databricks.delta.autoCompact.enabled": True,
    },
}
 
load_data(acon=acon)
```

##### Delta - Load data into the Bronze Bucket
```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "jdbc",
            "jdbc_args": {
                "url": "my_jdbc_url",
                "table": "my_database.my_table",
                "properties": {
                    "user": "my_user",
                    "password": "my_b4_hana_pwd",
                    "driver": "com.sap.db.jdbc.Driver",
                },
            },
            "options": {
                "fetchSize": 100000,
                "compress": True,
            },
        },
        {
            "spec_id": "my_identifier_bronze",
            "read_type": "batch",
            "data_format": "delta",
            "location": "s3://my_path/jdbc_template/no_parallel/my_identifier/",
        },
    ],
    "transform_specs": [
        {
            "spec_id": "max_my_identifier_bronze_date",
            "input_id": "my_identifier_bronze",
            "transformers": [{"function": "get_max_value", "args": {"input_col": "REQTSN"}}],
        },
        {
            "spec_id": "appended_my_identifier",
            "input_id": "my_identifier_source",
            "transformers": [
                {
                    "function": "incremental_filter",
                    "args": {"input_col": "REQTSN", "increment_df": "max_my_identifier_bronze_date"},
                }
            ],
        },
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "appended_my_identifier",
            "write_type": "append",
            "data_format": "delta",
            "partitions": ["REQTSN"],
            "location": "s3://my_path/jdbc_template/no_parallel/my_identifier/",
        }
    ],
    "exec_env": {
        "spark.databricks.delta.schema.autoMerge.enabled": True,
        "spark.databricks.delta.optimizeWrite.enabled": True,
        "spark.databricks.delta.autoCompact.enabled": True,
    },
}
 
load_data(acon=acon)
```

### 2 - Parallel extraction
On this section we present 3 possible scenarios for parallel extractions from JDBC sources.

!!! note "Disclaimer for parallel extraction"
    Parallel extractions can bring a jdbc source down if a lot of stress
    is put on the system. **Be careful when choosing the number of partitions. 
    Spark is a distributed system and can lead to many connections.**

#### 2.1 - Parallel Extraction, Simplest Scenario
This scenario provides the simplest example you can have for a parallel extraction from JDBC sources, only using
the property `numPartitions`. The goal of the scenario is to cover the case in which people do not have
much experience around how to optimize the extraction from JDBC sources or cannot identify a column that can
be used to split the extraction in several tasks. This scenario can also be used if the use case does not
have big performance requirements/concerns, meaning you do not feel the need to optimize the performance of
the extraction to its maximum potential.

On the example bellow, `"numPartitions": 10` is specified, meaning that Spark will open 10 parallel connections
to the source and automatically decide how to parallelize the extraction upon that requirement. This is the
only change compared to the example provided in the scenario 1.

##### Delta Init - Load data into the Bronze Bucket
```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "jdbc",
            "jdbc_args": {
                "url": "my_sap_b4_url",
                "table": "my_database.my_table",
                "properties": {
                    "user": "my_user",
                    "password": "my_b4_hana_pwd",
                    "driver": "com.sap.db.jdbc.Driver",
                },
            },
            "options": {
                "fetchSize": 100000,
                "compress": True,
                "numPartitions": 10,
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": "overwrite",
            "data_format": "delta",
            "partitions": ["REQTSN"],
            "location": "s3://my_path/jdbc_template/parallel_1/my_identifier/",
        }
    ],
    "exec_env": {
        "spark.databricks.delta.schema.autoMerge.enabled": True,
        "spark.databricks.delta.optimizeWrite.enabled": True,
        "spark.databricks.delta.autoCompact.enabled": True,
    },
}

load_data(acon=acon)
```

##### Delta - Load data into the Bronze Bucket
```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "jdbc",
            "jdbc_args": {
                "url": "my_sap_b4_url",
                "table": "my_database.my_table",
                "properties": {
                    "user": "my_user",
                    "password": "my_b4_hana_pwd",
                    "driver": "com.sap.db.jdbc.Driver",
                },
            },
            "options": {
                "fetchSize": 100000,
                "compress": True,
                "numPartitions": 10,
            },
        },
        {
            "spec_id": "my_identifier_bronze",
            "read_type": "batch",
            "data_format": "delta",
            "location": "s3://my_path/jdbc_template/parallel_1/my_identifier/",
        },
    ],
    "transform_specs": [
        {
            "spec_id": "max_my_identifier_bronze_date",
            "input_id": "my_identifier_bronze",
            "transformers": [{"function": "get_max_value", "args": {"input_col": "REQTSN"}}],
        },
        {
            "spec_id": "appended_my_identifier",
            "input_id": "my_identifier_source",
            "transformers": [
                {
                    "function": "incremental_filter",
                    "args": {"input_col": "REQTSN", "increment_df": "max_my_identifier_bronze_date"},
                }
            ],
        },
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "appended_my_identifier",
            "write_type": "append",
            "data_format": "delta",
            "partitions": ["REQTSN"],
            "location": "s3://my_path/jdbc_template/parallel_1/my_identifier/",
        }
    ],
    "exec_env": {
        "spark.databricks.delta.schema.autoMerge.enabled": True,
        "spark.databricks.delta.optimizeWrite.enabled": True,
        "spark.databricks.delta.autoCompact.enabled": True,
    },
}
 
load_data(acon=acon)
```

#### 2.2 - Parallel Extraction, Provide upper_bound (Recommended)
This scenario performs the extraction from the JDBC source in parallel, but has more concerns trying to
optimize and have more control (compared to 2.1 example) on how the extraction is split and performed,
using the following options:

- `numPartitions` - number of Spark partitions to split the extraction.
- `partitionColumn` - column used to split the extraction. It must be a numeric, date, or timestamp.
It should be a column that is able to split the extraction evenly in several tasks. An auto-increment
column is usually a very good candidate.
- `lowerBound` - lower bound to decide the partition stride.
- `upperBound` - upper bound to decide the partition stride.

This is an adequate example to be followed if there is a column in the data source that is good to
be used as the `partitionColumn`. Comparing with the previous example,
the `numPartitions` and three additional options to fine tune the extraction (`partitionColumn`, `lowerBound`,
`upperBound`) are provided.

When these 4 properties are used, Spark will use them to build several queries to split the extraction.
**Example:** for `"numPartitions": 10`, `"partitionColumn": "record"`, `"lowerBound: 1"`, `"upperBound: 100"`,
Spark will generate 10 queries like:

- `SELECT * FROM dummy_table WHERE RECORD < 10 OR RECORD IS NULL`
- `SELECT * FROM dummy_table WHERE RECORD >= 10 AND RECORD < 20`
- `SELECT * FROM dummy_table WHERE RECORD >= 20 AND RECORD < 30`
- ...
- `SELECT * FROM dummy_table WHERE RECORD >= 100`

 
##### Init - Load data into the Bronze Bucket
```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "jdbc",
            "jdbc_args": {
                "url": "my_sap_b4_url",
                "table": "my_database.my_table",
                "properties": {
                    "user": "my_user",
                    "password": "my_b4_hana_pwd",
                    "driver": "com.sap.db.jdbc.Driver",
                },
            },
            "options": {
                "partitionColumn": "RECORD",
                "numPartitions": 10,
                "lowerBound": 1,
                "upperBound": 2000,
                "fetchSize": 100000,
                "compress": True,
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": "overwrite",
            "data_format": "delta",
            "partitions": ["RECORD"],
            "location": "s3://my_path/jdbc_template/parallel_2/my_identifier/",
        }
    ],
    "exec_env": {
        "spark.databricks.delta.schema.autoMerge.enabled": True,
        "spark.databricks.delta.optimizeWrite.enabled": True,
        "spark.databricks.delta.autoCompact.enabled": True,
    },
}
 
load_data(acon=acon)
```

##### Delta - Load data into the Bronze Bucket
```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "jdbc",
            "jdbc_args": {
                "url": "my_sap_b4_url",
                "table": "my_database.my_table",
                "properties": {
                    "user": "my_user",
                    "password": "my_b4_hana_pwd",
                    "driver": "com.sap.db.jdbc.Driver",
                },
            },
            "options": {
                "partitionColumn": "RECORD",
                "numPartitions": 10,
                "lowerBound": 1,
                "upperBound": 2000,
                "fetchSize": 100000,
                "compress": True,
            },
        },
        {
            "spec_id": "my_identifier_bronze",
            "read_type": "batch",
            "data_format": "delta",
            "location": "s3://my_path/jdbc_template/parallel_2/my_identifier/",
        },
    ],
    "transform_specs": [
        {
            "spec_id": "max_my_identifier_bronze_date",
            "input_id": "my_identifier_bronze",
            "transformers": [{"function": "get_max_value", "args": {"input_col": "RECORD"}}],
        },
        {
            "spec_id": "appended_my_identifier",
            "input_id": "my_identifier_source",
            "transformers": [
                {
                    "function": "incremental_filter",
                    "args": {"input_col": "RECORD", "increment_df": "max_my_identifier_bronze_date"},
                }
            ],
        },
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "appended_my_identifier",
            "write_type": "append",
            "data_format": "delta",
            "partitions": ["RECORD"],
            "location": "s3://my_path/jdbc_template/parallel_2/my_identifier/",
        }
    ],
    "exec_env": {
        "spark.databricks.delta.schema.autoMerge.enabled": True,
        "spark.databricks.delta.optimizeWrite.enabled": True,
        "spark.databricks.delta.autoCompact.enabled": True,
    },
}

load_data(acon=acon)
```

#### 2.3 - Parallel Extraction with Predicates (Recommended)
This scenario performs the extraction from JDBC source in parallel, useful in contexts where there aren't
numeric, date or timestamp columns to parallelize the extraction:

- `partitionColumn` - column used to split the extraction (can be of any type).

- This is an adequate example to be followed if there is a column in the data source that is good to be
used as the `partitionColumn`, specially if these columns are not complying with the scenario 2.2.

**When this property is used, all predicates to Spark need to be provided, otherwise it will leave data behind.**

Bellow, a lakehouse function to generate predicate list automatically, is presented.

**By using this function one needs to be careful specially on predicates_query and predicates_add_null variables.**

**predicates_query:** At the sample below the whole table (`select distinct(x) from table`) is being considered,
but it is possible to filter using predicates list here, specially if you are applying filter on
transformations spec, and you know entire table won't be necessary, so you can change it to something like this:
`select distinct(x) from table where x > y`.

**predicates_add_null:** One can consider if null on predicates list or not. By default, this property is True.
**Example:** for `"partitionColumn": "record"`

##### Init - Load data into the Bronze Bucket
```python
from lakehouse_engine.engine import load_data
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.extraction.jdbc_extraction_utils import (
    JDBCExtraction,
    JDBCExtractionUtils,
)
ExecEnv.get_or_create()

partitionColumn = "my_partition_col"
dbtable = "my_database.my_table"
 
predicates_query = f"""(SELECT DISTINCT({partitionColumn}) FROM {dbtable})"""
column_for_predicates = partitionColumn
user = "my_user"
password = "my_b4_hana_pwd"
url = "my_sap_b4_url"
driver = "com.sap.db.jdbc.Driver"
predicates_add_null = True
 
jdbc_util = JDBCExtractionUtils(
    JDBCExtraction(
        user=user,
        password=password,
        url=url,
        predicates_add_null=predicates_add_null,
        partition_column=partitionColumn,
        dbtable=dbtable,
    )
)
 
predicates = jdbc_util.get_predicates(predicates_query)

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "jdbc",
            "jdbc_args": {
                "url": "my_sap_b4_url",
                "table": "my_database.my_table",
                "predicates": predicates,
                "properties": {
                    "user": "my_user",
                    "password": "my_b4_hana_pwd",
                    "driver": "com.sap.db.jdbc.Driver",
                },
            },
            "options": {
                "fetchSize": 100000,
                "compress": True,
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": "overwrite",
            "data_format": "delta",
            "partitions": ["RECORD"],
            "location": "s3://my_path/jdbc_template/parallel_3/my_identifier/",
        }
    ],
    "exec_env": {
        "spark.databricks.delta.schema.autoMerge.enabled": True,
        "spark.databricks.delta.optimizeWrite.enabled": True,
        "spark.databricks.delta.autoCompact.enabled": True,
    },
}
 
load_data(acon=acon)
```

##### Delta - Load data into the Bronze Bucket
```python
from lakehouse_engine.engine import load_data
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.extraction.jdbc_extraction_utils import (
    JDBCExtraction,
    JDBCExtractionUtils,
)
ExecEnv.get_or_create()

partitionColumn = "my_partition_col"
dbtable = "my_database.my_table"

predicates_query = f"""(SELECT DISTINCT({partitionColumn}) FROM {dbtable})"""
column_for_predicates = partitionColumn
user = "my_user"
password = "my_b4_hana_pwd"
url = "my_sap_b4_url"
driver = "com.sap.db.jdbc.Driver"
predicates_add_null = True

jdbc_util = JDBCExtractionUtils(
    JDBCExtraction(
        user=user,
        password=password,
        url=url,
        predicates_add_null=predicates_add_null,
        partition_column=partitionColumn,
        dbtable=dbtable,
    )
)

predicates = jdbc_util.get_predicates(predicates_query)

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "jdbc",
            "jdbc_args": {
                "url": "my_sap_b4_url",
                "table": "my_database.my_table",
                "predicates": predicates,
                "properties": {
                    "user": "my_user",
                    "password": "my_b4_hana_pwd",
                    "driver": "com.sap.db.jdbc.Driver",
                },
            },
            "options": {
                "fetchSize": 100000,
                "compress": True,
            },
        },
        {
            "spec_id": "my_identifier_bronze",
            "read_type": "batch",
            "data_format": "delta",
            "location": "s3://my_path/jdbc_template/parallel_3/my_identifier/",
        },
    ],
    "transform_specs": [
        {
            "spec_id": "max_my_identifier_bronze_date",
            "input_id": "my_identifier_bronze",
            "transformers": [{"function": "get_max_value", "args": {"input_col": "RECORD"}}],
        },
        {
            "spec_id": "appended_my_identifier",
            "input_id": "my_identifier_source",
            "transformers": [
                {
                    "function": "incremental_filter",
                    "args": {"input_col": "RECORD", "increment_df": "max_my_identifier_bronze_date"},
                }
            ],
        },
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "appended_my_identifier",
            "write_type": "append",
            "data_format": "delta",
            "partitions": ["RECORD"],
            "location": "s3://my_path/jdbc_template/parallel_3/my_identifier/",
        }
    ],
    "exec_env": {
        "spark.databricks.delta.schema.autoMerge.enabled": True,
        "spark.databricks.delta.optimizeWrite.enabled": True,
        "spark.databricks.delta.autoCompact.enabled": True,
    },
}
 
load_data(acon=acon)
```