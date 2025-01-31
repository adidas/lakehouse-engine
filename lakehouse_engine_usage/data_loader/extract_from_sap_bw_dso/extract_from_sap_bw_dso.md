# Extract from SAP BW DSOs

!!! danger "**Parallelization Limitations**"
    Parallel extractions **can bring a jdbc source down** if a lot of stress is put on the system. Be careful choosing the number of partitions. Spark is a distributed system and can lead to many connections.

A custom sap_bw reader and a few utils are offered in the lakehouse-engine framework so that consumption of data from 
SAP BW DSOs can be easily created. The framework abstracts all the logic behind the init/delta extractions 
(active table, changelog table, activation requests table, how to identify the next delta timestamp...), 
only requiring a few parameters that are explained and exemplified in the 
[template](#extraction-from-sap-bw-template) scenarios that we have created.

This page also provides you a section to help you figure out a good candidate for [partitioning the extraction from SAP BW](#how-can-we-decide-the-partitionColumn).

You can check the code documentation of the reader below:

[**SAP BW Reader**](../../../reference/packages/io/readers/sap_bw_reader.md)

[**JDBC Extractions arguments**](../../../reference/packages/utils/extraction/jdbc_extraction_utils.md#packages.utils.extraction.jdbc_extraction_utils.JDBCExtraction.__init__)

[**SAP BW Extractions arguments**](../../../reference/packages/utils/extraction/sap_bw_extraction_utils.md#packages.utils.extraction.sap_bw_extraction_utils.SAPBWExtraction.__init__)

!!! note
    For extractions using the SAP BW reader, you can use the arguments listed in the SAP BW arguments, but also 
    the ones listed in the JDBC extractions, as those are inherited as well. 


## Extraction from SAP-BW template

This template covers the following scenarios of extractions from the SAP BW DSOs:

- 1 - The Simplest Scenario (Not parallel - Not Recommended)
- 2 - Parallel extraction
  - 2.1 - Simplest Scenario
  - 2.2 - Provide upperBound (Recommended)
  - 2.3 - Automatic upperBound (Recommended)
  - 2.4 - Backfilling
  - 2.5 - Provide predicates (Recommended)
  - 2.6 - Generate predicates (Recommended)
- 3 - Extraction from Write Optimized DSO
  - 3.1 - Get initial actrequest_timestamp from Activation Requests Table

!!! note "Introductory Notes"
    If you want to have a better understanding about JDBC Spark optimizations, 
    here you have a few useful links:
    - https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    - https://docs.databricks.com/en/connect/external-systems/jdbc.html
    - https://bit.ly/3x2eCEm
    - https://newbedev.com/how-to-optimize-partitioning-when-migrating-data-from-jdbc-source

### 1 - The Simplest Scenario (Not parallel - Not Recommended)
This scenario is the simplest one, not taking any advantage of Spark JDBC optimisation techniques 
and using a single connection to retrieve all the data from the source. It should only be used in case the DSO 
you want to extract from SAP BW is a small one, with no big requirements in terms of performance to fulfill.

When extracting from the source DSO, there are two options:

- **Delta Init** - full extraction of the source DSO. You should use it in the first time you extract from the 
DSO or any time you want to re-extract completely. Similar to a so-called full load.
- **Delta** - extracts the portion of the data that is new or has changed in the source, since the last
extraction (using the max `actrequest_timestamp` value in the location of the data already extracted,
by default).

Below example is composed of two cells.

- The first cell is only responsible to define the variables `extraction_type` and `write_type`,
depending on the extraction type **Delta Init** (`LOAD_TYPE = INIT`) or a **Delta** (`LOAD_TYPE = DELTA`).
The variables in this cell will also be referenced by other acons/examples in this notebook, similar to what
you would do in your pipelines/jobs, defining this centrally and then re-using it.
- The second cell is where the acon to be used is defined (which uses the two variables `extraction_type` and
`write_type` defined) and the `load_data` algorithm is executed to perform the extraction.

!!! note
    There may be cases where you might want to always extract fully from the source DSO. In these cases,
    you only need to use a Delta Init every time, meaning you would use `"extraction_type": "init"` and
    `"write_type": "overwrite"` as it is shown below. The explanation about what it is a Delta Init/Delta is
    applicable for all the scenarios presented in this notebook.

```python
from lakehouse_engine.engine import load_data

LOAD_TYPE = "INIT" or "DELTA"

if LOAD_TYPE == "INIT":
    extraction_type = "init"
    write_type = "overwrite"
else:
    extraction_type = "delta"
    write_type = "append"

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            # You should use this custom reader to benefit from the lakehouse-engine utils for extractions from SAP BW
            "data_format": "sap_bw",
            "options": {
                "user": "my_user",
                "password": "my_hana_pwd",
                "url": "my_sap_bw_url",
                "dbtable": "my_database.my_table",
                "odsobject": "my_ods_object",
                "changelog_table": "my_database.my_changelog_table",
                "latest_timestamp_data_location": "s3://my_path/my_identifier/",
                "extraction_type": extraction_type,
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": write_type,
            "data_format": "delta",
            "partitions": ["actrequest_timestamp"],
            "location": "s3://my_path/my_identifier/",
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
In this section, 6 possible scenarios for parallel extractions from SAP BW DSOs.

#### 2.1 - Parallel Extraction, Simplest Scenario
This scenario provides the simplest example you can have for a parallel extraction from SAP BW, only using
the property `numPartitions`. The goal of the scenario is to cover the case in which people does not have
much knowledge around how to optimize the extraction from JDBC sources or cannot identify a column that can
be used to split the extraction in several tasks. This scenario can also be used if the use case does not
have big performance requirements/concerns, meaning you do not feel the need to optimize the performance of
the extraction to its maximum potential. 
On the example below, `"numPartitions": 10` is specified, meaning that Spark will open 10 parallel connections
to the source DSO and automatically decide how to parallelize the extraction upon that requirement. This is the
only change compared to the example provided in the example 1.

```python
from lakehouse_engine.engine import load_data

LOAD_TYPE = "INIT" or "DELTA"

if LOAD_TYPE == "INIT":
    extraction_type = "init"
    write_type = "overwrite"
else:
    extraction_type = "delta"
    write_type = "append"

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "sap_bw",
            "options": {
                "user": "my_user",
                "password": "my_hana_pwd",
                "url": "my_sap_bw_url",
                "dbtable": "my_database.my_table",
                "odsobject": "my_ods_object",
                "changelog_table": "my_database.my_changelog_table",
                "latest_timestamp_data_location": "s3://my_path/my_identifier/",
                "extraction_type": extraction_type,
                "numPartitions": 10,
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": write_type,
            "data_format": "delta",
            "partitions": ["actrequest_timestamp"],
            "location": "s3://my_path/my_identifier/",
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
This scenario performs the extraction from the SAP BW DSO in parallel, but is more concerned with trying to
optimize and have more control (compared to 2.1 example) on how the extraction is split and performed, using
the following options:

- `numPartitions` - number of Spark partitions to split the extraction.
- `partitionColumn` - column used to split the extraction. It must be a numeric, date, or timestamp.
It should be a column that is able to split the extraction evenly in several tasks. An auto-increment
column is usually a very good candidate.
- `lowerBound` - lower bound to decide the partition stride.
- `upperBound` - upper bound to decide the partition stride. It can either be **provided (as it is done in
this example)** or derived automatically by our upperBound optimizer (example 2.3).

This is an adequate example for you to follow if you have/know a column in the DSO that is good to be used as
the `partitionColumn`. If you compare with the previous example, you'll notice that now `numPartitions` and
three additional options are provided to fine tune the extraction (`partitionColumn`, `lowerBound`,
`upperBound`).

When these 4 properties are used, Spark will use them to build several queries to split the extraction.

**Example:** for `"numPartitions": 10`, `"partitionColumn": "record"`, `"lowerBound: 1"`, `"upperBound: 100"`,
Spark will generate 10 queries like this:

- `SELECT * FROM dummy_table WHERE RECORD < 10 OR RECORD IS NULL`
- `SELECT * FROM dummy_table WHERE RECORD >= 10 AND RECORD < 20`
- `SELECT * FROM dummy_table WHERE RECORD >= 20 AND RECORD < 30`
- ...
- `SELECT * FROM dummy_table WHERE RECORD >= 100`

```python
from lakehouse_engine.engine import load_data

LOAD_TYPE = "INIT" or "DELTA"

if LOAD_TYPE == "INIT":
    extraction_type = "init"
    write_type = "overwrite"
else:
    extraction_type = "delta"
    write_type = "append"

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "sap_bw",
            "options": {
                "user": "my_user",
                "password": "my_hana_pwd",
                "url": "my_sap_bw_url",
                "dbtable": "my_database.my_table",
                "odsobject": "my_ods_object",
                "changelog_table": "my_database.my_changelog_table",
                "latest_timestamp_data_location": "s3://my_path/my_identifier/",
                "extraction_type": extraction_type,
                "numPartitions": 3,
                "partitionColumn": "my_partition_col",
                "lowerBound": 1,
                "upperBound": 42,
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": write_type,
            "data_format": "delta",
            "partitions": ["actrequest_timestamp"],
            "location": "s3://my_path/my_identifier/",
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

#### 2.3 - Parallel Extraction, Automatic upper_bound (Recommended)
This scenario is very similar to 2.2, the only difference being that **upper_bound
is not provided**. Instead, the property `calculate_upper_bound` equals to true is used to benefit
from the automatic calculation of the upperBound (derived from the `partitionColumn`) offered by the
lakehouse-engine framework, which is useful, as in most of the cases you will probably not be aware of
the max value for the column. The only thing you need to consider is that if you use this automatic
calculation of the upperBound you will be doing an initial query to the SAP BW DSO to retrieve the max
value for the `partitionColumn`, before doing the actual query to perform the extraction.

```python
from lakehouse_engine.engine import load_data

LOAD_TYPE = "INIT" or "DELTA"

if LOAD_TYPE == "INIT":
    extraction_type = "init"
    write_type = "overwrite"
else:
    extraction_type = "delta"
    write_type = "append"

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "sap_bw",
            "calculate_upper_bound": True,
            "options": {
                "user": "my_user",
                "password": "my_hana_pwd",
                "url": "my_sap_bw_url",
                "dbtable": "my_database.my_table",
                "odsobject": "my_ods_object",
                "changelog_table": "my_database.my_changelog_table",
                "latest_timestamp_data_location": "s3://my_path/my_identifier/",
                "extraction_type": extraction_type,
                "numPartitions": 10,
                "partitionColumn": "my_partition_col",
                "lowerBound": 1,
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": write_type,
            "data_format": "delta",
            "partitions": ["actrequest_timestamp"],
            "location": "s3://my_path/my_identifier/",
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

#### 2.4 - Parallel Extraction, Backfilling
This scenario covers the case, in which you might want to backfill the data extracted from a SAP BW DSO and
made available in the bronze layer. By default, the delta extraction considers the max value of the column
`actrequest_timestamp` on the data already extracted. However, there might be cases, in which you might want
to extract a delta from a particular timestamp onwards or for a particular interval of time. For this, you
can use the properties `min_timestamp` and `max_timestamp`.

Below, a very similar example to the previous one is provided, the only differences being that
the properties `"min_timestamp": "20210910000000"` and `"max_timestamp": "20210913235959"` are not provided,
meaning it will extract the data from the changelog table, using a filter
`"20210910000000" > actrequest_timestamp <= "20210913235959"`, ignoring if some of the data is already
available in the destination or not. Moreover, note that the property `latest_timestamp_data_location`
does not need to be provided, as the timestamps to be considered are being directly provided (if both
the timestamps and the `latest_timestamp_data_location` are provided, the last parameter will have no effect).
Additionally, `"extraction_type": "delta"` and `"write_type": "append"` is forced, instead of using the
variables as in the other  examples, because the backfilling scenario only makes sense for delta extractions.

!!! note
    Note: be aware that the backfilling example being shown has no mechanism to enforce that
    you don't generate duplicated data in bronze. For your scenarios, you can either use this example and solve
    any duplication in the silver layer or extract the delta with a merge strategy while writing to bronze,
    instead of appending.

```python
from lakehouse_engine.engine import load_data

LOAD_TYPE = "INIT" or "DELTA"

if LOAD_TYPE == "INIT":
    extraction_type = "init"
    write_type = "overwrite"
else:
    extraction_type = "delta"
    write_type = "append"

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "sap_bw",
            "calculate_upper_bound": True,
            "options": {
                "user": "my_user",
                "password": "my_hana_pwd",
                "url": "my_sap_bw_url",
                "dbtable": "my_database.my_table",
                "odsobject": "my_ods_object",
                "changelog_table": "my_database.my_changelog_table",
                "extraction_type": "delta",
                "numPartitions": 10,
                "partitionColumn": "my_partition_col",
                "lowerBound": 1,
                "min_timestamp": "20210910000000",
                "max_timestamp": "20210913235959",
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": "append",
            "data_format": "delta",
            "partitions": ["actrequest_timestamp"],
            "location": "s3://my_path/my_identifier/",
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

#### 2.5 - Parallel Extraction, Provide Predicates (Recommended)
This scenario performs the extraction from SAP BW DSO in parallel, useful in contexts in which there is no
numeric, date or timestamp column to parallelize the extraction:

- `partitionColumn` - column used to split the extraction. It can be of any type. 

This is an adequate example for you to follow if you have/know a column in the DSO that is good to be used as
the `partitionColumn`, specially if these columns are not complying with the scenarios 2.2 and 2.3 (otherwise
those would probably be recommended).

**When this property is used all predicates need to be provided to Spark, otherwise it will leave data behind.**

Below the lakehouse function to generate predicate list automatically is presented.

This function needs to be used carefully, specially on predicates_query and predicates_add_null variables.

**predicates_query:** At the sample below the whole table is being considered (`select distinct(x) from table`),
but it is possible to filter predicates list here,
specially if you are applying filter on transformations spec, and you know entire table won't be necessary, so
you can change it to something like this: `select distinct(x) from table where x > y`.

**predicates_add_null:** You can decide if you want to consider null on predicates list or not, by default this
property is True.

```python
from lakehouse_engine.engine import load_data

LOAD_TYPE = "INIT" or "DELTA"

if LOAD_TYPE == "INIT":
    extraction_type = "init"
    write_type = "overwrite"
else:
    extraction_type = "delta"
    write_type = "append"

# import the lakehouse_engine ExecEnv class, so that you can use the functions it offers
# import the lakehouse_engine extraction utils, so that you can use the JDBCExtractionUtils offered functions
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.extraction.jdbc_extraction_utils import (
    JDBCExtraction,
    JDBCExtractionUtils,
)

ExecEnv.get_or_create()

partition_column = "my_partition_column"
dbtable = "my_database.my_table"

predicates_query = f"""(SELECT DISTINCT({partition_column}) FROM {dbtable})"""
column_for_predicates = partition_column
user = "my_user"
password = "my_hana_pwd"
url = "my_bw_url"
predicates_add_null = True

jdbc_util = JDBCExtractionUtils(
    JDBCExtraction(
        user=user,
        password=password,
        url=url,
        dbtable=dbtable,
        partition_column=partition_column,
    )
)

predicates = jdbc_util.get_predicates(predicates_query)

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "sap_bw",
            "options": {
                "user": "my_user",
                "password": "my_hana_pwd",
                "url": "my_sap_bw_url",
                "dbtable": "my_database.my_table",
                "odsobject": "my_ods_object",
                "latest_timestamp_data_location": "s3://my_path/my_identifier/",
                "extraction_type": extraction_type,
                "predicates": predicates,
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": write_type,
            "data_format": "delta",
            "partitions": ["actrequest_timestamp"],
            "location": "s3://my_path/my_identifier/",
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

#### 2.6 - Parallel Extraction, Generate Predicates (Recommended)
This scenario performs the extraction from SAP BW DSO in parallel, useful in contexts in which there is no
numeric, date or timestamp column to parallelize the extraction:

- `partitionColumn` - column used to split the extraction. It can be of any type.

This is an adequate example for you to follow if you have/know a column in the DSO that is good to be used as
the `partitionColumn`, specially if these columns are not complying with the scenarios 2.2 and 2.3 (otherwise
those would probably be recommended).

When this property is used, the lakehouse engine will generate the predicates to be used to extract data from
the source. What the lakehouse engine does is to check for the init/delta portion of the data,
what are the distinct values of the `partitionColumn` serving that data. Then, these values will be used by
Spark to generate several queries to extract from the source in a parallel fashion.
Each distinct value of the `partitionColumn` will be a query, meaning that you will not have control over the
number of partitions used for the extraction. For example, if you face a scenario in which you
are using a `partitionColumn` `LOAD_DATE` and for today's delta, all the data (let's suppose 2 million rows) is
served by a single `LOAD_DATE = 20200101`, that would mean Spark would use a single partition
to extract everything. In this extreme case you would probably need to change your `partitionColumn`. **Note:**
these extreme cases are harder to happen when you use the strategy of the scenarios 2.2/2.3.

**Example:** for `"partitionColumn": "record"`
Generate predicates:

- `SELECT DISTINCT(RECORD) as RECORD FROM dummy_table`
- `1`
- `2`
- `3`
- ...
- `100`
- Predicates List: ['RECORD=1','RECORD=2','RECORD=3',...,'RECORD=100']

Spark will generate 100 queries like this:

- `SELECT * FROM dummy_table WHERE RECORD = 1`
- `SELECT * FROM dummy_table WHERE RECORD = 2`
- `SELECT * FROM dummy_table WHERE RECORD = 3`
- ...
- `SELECT * FROM dummy_table WHERE RECORD = 100`

Generate predicates will also consider null by default:
- `SELECT * FROM dummy_table WHERE RECORD IS NULL`

To disable this behaviour the following variable value should be changed to false: `"predicates_add_null": False`

```python
from lakehouse_engine.engine import load_data

LOAD_TYPE = "INIT" or "DELTA"

if LOAD_TYPE == "INIT":
    extraction_type = "init"
    write_type = "overwrite"
else:
    extraction_type = "delta"
    write_type = "append"

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "sap_bw",
            "generate_predicates": True,
            "options": {
                "user": "my_user",
                "password": "my_hana_pwd",
                "url": "my_sap_bw_url",
                "dbtable": "my_database.my_table",
                "odsobject": "my_ods_object",
                "latest_timestamp_data_location": "s3://my_path/my_identifier/",
                "extraction_type": extraction_type,
                "partitionColumn": "my_partition_col",
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": write_type,
            "data_format": "delta",
            "partitions": ["actrequest_timestamp"],
            "location": "s3://my_path/my_identifier/",
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

### 3 - Extraction from Write Optimized DSOs
This scenario is based on the best practices of the scenario 2.2, but it is ready to extract data from
Write Optimized DSOs, which have the changelog embedded in the active table, instead of having a separate
changelog table. Due to this reason, you need to specify that the `changelog_table` parameter value is equal
to the `dbtable` parameter value.
Moreover, these tables usually already include the changelog technical columns
like `RECORD` and `DATAPAKID`, for example, that the framework adds by default. Thus, you need to specify
`"include_changelog_tech_cols": False` to change this behaviour.
Finally, you also need to specify the name of the column in the table that can be used to join with the
activation requests table to get the timestamp of the several requests/deltas,
which is `"actrequest"` by default (`"request_col_name": 'request'`).

```python
from lakehouse_engine.engine import load_data

LOAD_TYPE = "INIT" or "DELTA"

if LOAD_TYPE == "INIT":
    extraction_type = "init"
    write_type = "overwrite"
else:
    extraction_type = "delta"
    write_type = "append"

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "sap_bw",
            "options": {
                "user": "my_user",
                "password": "my_hana_pwd",
                "url": "my_sap_bw_url",
                "dbtable": "my_database.my_table",
                "changelog_table": "my_database.my_table",
                "odsobject": "my_ods_object",
                "request_col_name": "request",
                "include_changelog_tech_cols": False,
                "latest_timestamp_data_location": "s3://my_path/my_identifier/",
                "extraction_type": extraction_type,
                "numPartitions": 2,
                "partitionColumn": "RECORD",
                "lowerBound": 1,
                "upperBound": 50000,
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": write_type,
            "data_format": "delta",
            "partitions": ["actrequest_timestamp"],
            "location": "s3://my_path/my_identifier/",
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

#### 3.1 - Extraction from Write Optimized DSOs, Get ACTREQUEST_TIMESTAMP from Activation Requests Table
By default, the act_request_timestamp has being hardcoded (either assumes a given extraction_timestamp or the
current timestamp) in the init extraction, however this may be causing problems when merging changes in silver,
for write optimised DSOs. So, a new possibility to choose when to retrieve this timestamp from the
act_req_table was added.

This scenario performs the data extraction from Write Optimized DSOs, forcing the actrequest_timestamp to
assume the value from the activation requests table (timestamp column).

This feature is only available for WODSOs and to use it you need to specify `"get_timestamp_from_actrequest": True`.

```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_source",
            "read_type": "batch",
            "data_format": "sap_bw",
            "options": {
                "user": "my_user",
                "password": "my_hana_pwd",
                "url": "my_sap_bw_url",
                "dbtable": "my_database.my_table",
                "changelog_table": "my_database.my_table",
                "odsobject": "my_ods_object",
                "request_col_name": "request",
                "include_changelog_tech_cols": False,
                "latest_timestamp_data_location": "s3://my_path/my_identifier_ACTREQUEST_TIMESTAMP/",
                "extraction_type": "init",
                "numPartitions": 2,
                "partitionColumn": "RECORD",
                "lowerBound": 1,
                "upperBound": 50000,
                "get_timestamp_from_act_request": True,
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": "overwrite",
            "data_format": "delta",
            "partitions": ["actrequest_timestamp"],
            "location": "s3://my_path/my_identifier_ACTREQUEST_TIMESTAMP",
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

## How can we decide the partitionColumn?

**Compatible partitionColumn for upperBound/lowerBound Spark options:**

It needs to be **int, date, timestamp** → https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

**If you don't have any column to partition on those formats, you can use predicates to partition the table** → https://docs.databricks.com/en/connect/external-systems/jdbc.html#manage-parallelism

One of the most important parameters to optimise the extraction is the **partitionColumn**, as you can see in the template. Thus, this section helps you figure out if a column is a good candidate or not. 

Basically the partition column needs to be a column which is able to adequately split the processing, which means we can use it to "create" different queries with intervals/filters, so that the Spark tasks process similar amounts of rows/volume. Usually a good candidate is an integer auto-increment technical column.

!!! note
    Although RECORD is usually a good candidate, it is usually available on the changelog table only. Meaning that you would need to use a different strategy for the init. In case you don't have good candidates for partitionColumn, you can use the sample acon provided in the **scenario 2.1** in the template above. It might make sense to use **scenario 2.1** for the init and then **scenario 2.2 or 2.3** for the subsequent deltas.

**When there is no int, date or timestamp good candidate for partitionColumn:**

In this case you can opt by the **scenario 2.5 - Generate Predicates**, which supports any kind of column to be defined as **partitionColumn**.

However, you should still analyse if the column you are thinking about is a good candidate or not. In this scenario, Spark will create one query per distinct value of the **partitionColumn**, so you can perform some analysis.