# Extract from SAP B4 ADSOs

A custom sap_b4 reader and a few utils are offered in the lakehouse-engine framework so that consumption of data from
SAP B4 DSOs can be easily created. The framework abstracts all the logic behind the init/delta extractions
(AQ vs CL, active table, changelog table, requests status table, how to identify the next delta timestamp...),
only requiring a few parameters that are explained and exemplified in the
[template](#extraction-from-sap-b4-adsos-template) scenarios that we have created.


.. note:: This custom reader is very similar and uses most features from the sap_bw reader, so if you were using specific filters/parameters with the sap_bw reader, there is a high chance you can keep using it in a very similar way with the sap_b4 reader. The main concepts are applied to both readers, as the strategies on how to parallelize the extractions, for example.

How can I find a good candidate column for [partitioning the extraction from S4Hana?](../../lakehouse_engine_usage/data_loader/extract_from_sap_bw_dso.html#how-can-we-decide-the-partitionColumn)

.. danger:: **Parallelization Limitations**
    There are no limits imposed by the Lakehouse-Engine framework, but you need to consider that there might be differences imposed by the source.

    E.g. Each User might be restricted on utilisation of about 100GB memory at a time from the source.

    Parallel extractions ***can bring a jdbc source down*** if a lot of stress is put on the system. Be careful choosing the number of partitions. Spark is a distributed system and can lead to many connections.

.. danger:: **In case you want to perform further filtering in the REQTSN field, please be aware that it is not being pushed down to SAP B4 by default (meaning it will have bad performance).** 
    In that case, you will need to use customSchema option while reading, so that you are able to enable filter push down for those.


You can check the code documentation of the reader below:

[**SAP B4 Reader**](../../lakehouse_engine/io/readers/sap_b4_reader.html)

[**JDBC Extractions arguments**](../../lakehouse_engine/utils/extraction/jdbc_extraction_utils.html#JDBCExtraction.__init__)

[**SAP B4 Extractions arguments**](../../lakehouse_engine/utils/extraction/sap_b4_extraction_utils.html#SAPB4Extraction.__init__)

.. note:: For extractions using the SAP B4 reader, you can use the arguments listed in the SAP B4 arguments, but also the ones listed in the JDBC extractions, as those are inherited as well.


## Extraction from SAP B4 ADSOs Template
This template covers the following scenarios of extractions from the SAP B4Hana ADSOs:
- 1 - The Simplest Scenario (Not parallel - Not Recommended)
- 2 - Parallel extraction
  - 2.1 - Simplest Scenario
  - 2.2 - Provide upperBound (Recommended)
  - 2.3 - Automatic upperBound (Recommended)
  - 2.4 - Provide predicates (Recommended)
  - 2.5 - Generate predicates (Recommended)

.. note::
    Note: the template will cover two ADSO Types:
    - **AQ**: ADSO which is of append type and for which a single ADSO/tables holds all the information, like an
    event table. For this type, the same ADSO is used for reading data both for the inits and deltas. Usually, these
    ADSOs end with the digit "6".
    - **CL**: ADSO which is split into two ADSOs, one holding the change log events, the other having the active
    data (current version of the truth for a particular source). For this type, the ADSO having the active data
    is used for the first extraction (init) and the change log ADSO is used for the subsequent extractions (deltas).
    Usually, these ADSOs are split into active table ending with the digit "2" and changelog table ending with digit "3".

For each of these ADSO types, the lakehouse-engine abstracts the logic to get the delta extractions. This logic
basically consists of joining the `db_table` (for `AQ`) or the `changelog_table` (for `CL`) with the table
having the requests status (`my_database.requests_status_table`).
One of the fields used for this joining is the `data_target`, which has a relationship with the ADSO
(`db_table`/`changelog_table`), being basically the same identifier without considering parts of it.

Based on the previous insights, the queries that the lakehouse-engine generates under the hood translate to
(this is a simplified version, for more details please refer to the lakehouse-engine code documentation):
**AQ Init Extraction:**
`SELECT t.*, CAST({self._SAP_B4_EXTRACTION.extraction_timestamp} AS DECIMAL(15,0)) AS extraction_start_timestamp
FROM my_database.my_table t`

**AQ Delta Extraction:**
`SELECT tbl.*, CAST({self._B4_EXTRACTION.extraction_timestamp} AS DECIMAL(15,0)) AS extraction_start_timestamp
FROM my_database.my_table AS tbl
JOIN my_database.requests_status_table AS req
WHERE STORAGE = 'AQ' AND REQUEST_IS_IN_PROCESS = 'N' AND LAST_OPERATION_TYPE IN ('C', 'U')
AND REQUEST_STATUS IN ('GG', 'GR') AND UPPER(DATATARGET) = UPPER('my_identifier')
AND req.REQUEST_TSN > max_timestamp_in_bronze AND req.REQUEST_TSN <= max_timestamp_in_requests_status_table`

**CL Init Extraction:**
`SELECT t.*,
    {self._SAP_B4_EXTRACTION.extraction_timestamp}000000000 AS reqtsn,
    '0' AS datapakid,
    0 AS record,
    CAST({self._SAP_B4_EXTRACTION.extraction_timestamp} AS DECIMAL(15,0)) AS extraction_start_timestamp
FROM my_database.my_table_2 t`

**CL Delta Extraction:**
`SELECT tbl.*,
CAST({self._SAP_B4_EXTRACTION.extraction_timestamp} AS DECIMAL(15,0)) AS extraction_start_timestamp`
FROM my_database.my_table_3 AS tbl
JOIN my_database.requests_status_table AS req
WHERE STORAGE = 'AT' AND REQUEST_IS_IN_PROCESS = 'N' AND LAST_OPERATION_TYPE IN ('C', 'U')
AND REQUEST_STATUS IN ('GG') AND UPPER(DATATARGET) = UPPER('my_data_target')
AND req.REQUEST_TSN > max_timestamp_in_bronze AND req.REQUEST_TSN <= max_timestamp_in_requests_status_table`

.. note::
    Introductory Notes:If you want to have a better understanding about JDBC Spark optimizations, here you have a few useful links:
    - https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    - https://docs.databricks.com/en/connect/external-systems/jdbc.html
    - https://bit.ly/3x2eCEm
    - https://newbedev.com/how-to-optimize-partitioning-when-migrating-data-from-jdbc-source

### 1 - The Simplest Scenario (Not parallel - Not Recommended)
This scenario is the simplest one, not taking any advantage of Spark JDBC optimisation techniques
and using a single connection to retrieve all the data from the source. It should only be used in case the ADSO
you want to extract from SAP B4Hana is a small one, with no big requirements in terms of performance to fulfill.
When extracting from the source ADSO, there are two options:
- **Delta Init** - full extraction of the source ADSO. You should use it in the first time you extract from the
ADSO or any time you want to re-extract completely. Similar to a so-called full load.
- **Delta** - extracts the portion of the data that is new or has changed in the source, since the last
extraction (using the `max_timestamp` value in the location of the data already extracted
`latest_timestamp_data_location`).

Below example is composed of two cells.
- The first cell is only responsible to define the variables `extraction_type` and `write_type`,
depending on the extraction type: **Delta Init** (`load_type = "init"`) or a **Delta** (`load_type = "delta"`).
The variables in this cell will also be referenced by other acons/examples in this notebook, similar to what
you would do in your pipelines/jobs, defining this centrally and then re-using it.
- The second cell is where the acon to be used is defined (which uses the two variables `extraction_type` and
`write_type` defined) and the `load_data` algorithm is executed to perform the extraction.

.. note::
    There may be cases where you might want to always extract fully from the source ADSO. In these cases,
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
            "data_format": "sap_b4",
            "options": {
                "url": "my_sap_b4_url",
                "user": "my_user",
                "password": "my_b4_hana_pwd",
                "dbtable": "my_database.my_table",
                "extraction_type": extraction_type,
                "latest_timestamp_data_location": "s3://my_path/my_identifier/",
                "adso_type": "AQ",
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": write_type,
            "data_format": "delta",
            "partitions": ["REQTSN"],
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
In this section, 5 possible scenarios for parallel extractions from SAP B4Hana ADSOs are presented.

#### 2.1 - Parallel Extraction, Simplest Scenario
This scenario provides the simplest example you can have for a parallel extraction from SAP B4Hana, only using
the property `numPartitions`. The goal of the scenario is to cover the case in which people do not have
much knowledge around how to optimize the extraction from JDBC sources or cannot identify a column that can
be used to split the extraction in several tasks. This scenario can also be used if the use case does not
have big performance requirements/concerns, meaning you do not feel the need to optimize the performance of
the extraction to its maximum potential.

On the example below, `"numPartitions": 10` is specified, meaning that Spark will open 10 parallel connections
to the source ADSO and automatically decide how to parallelize the extraction upon that requirement. This is the
only change compared to the example provided in the scenario 1.

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
            "data_format": "sap_b4",
            "options": {
                "url": "my_sap_b4_url",
                "user": "my_user",
                "password": "my_sap_b4_pwd",
                "dbtable": "my_database.my_table",
                "extraction_type": extraction_type,
                "latest_timestamp_data_location": "s3://my_path/my_identifier_par_simple/",
                "adso_type": "AQ",
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
            "partitions": ["REQTSN"],
            "location": "s3://my_path/my_identifier_par_simple/",
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
This scenario performs the extraction from the SAP B4 ADSO in parallel, but is more concerned with trying to
optimize and have more control (compared to 2.1 example) on how the extraction is split and performed,
using the following options:
- `numPartitions` - number of Spark partitions to split the extraction.
- `partitionColumn` - column used to split the extraction. It must be a numeric, date, or timestamp.
It should be a column that is able to split the extraction evenly in several tasks. An auto-increment
column is usually a very good candidate.
- `lowerBound` - lower bound to decide the partition stride.
- `upperBound` - upper bound to decide the partition stride.

This is an adequate example for you to follow if you have/know a column in the ADSO that is good to be used as
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
            "data_format": "sap_b4",
            "options": {
                "url": "my_sap_b4_url",
                "user": "my_user",
                "password": "my_b4_hana_pwd",
                "dbtable": "my_database.my_table",
                "extraction_type": extraction_type,
                "latest_timestamp_data_location": "s3://my_path/my_identifier_par_prov_upper/",
                "adso_type": "AQ",
                "partitionColumn": "RECORD",
                "numPartitions": 10,
                "lowerBound": 1,
                "upperBound": 1000000,
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_bronze",
            "input_id": "my_identifier_source",
            "write_type": write_type,
            "data_format": "delta",
            "partitions": ["REQTSN"],
            "location": "s3://my_path/my_identifier_par_prov_upper/",
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
This scenario is very similar to 2.2, the only difference being that **`upperBound`
is not provided**. Instead, the property `calculate_upper_bound` equals to true is used to benefit
from the automatic calculation of the `upperBound` (derived from the `partitionColumn`) offered by the
lakehouse-engine framework, which is useful, as in most of the cases you will probably not be aware of
the max value for the column. The only thing you need to consider is that if you use this automatic
calculation of the upperBound you will be doing an initial query to the SAP B4 ADSO to retrieve the max
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
            "data_format": "sap_b4",
            "calculate_upper_bound": True,
            "options": {
                "url": "my_sap_b4_url",
                "user": "my_user",
                "password": "my_b4_hana_pwd",
                "dbtable": "my_database.my_table",
                "extraction_type": extraction_type,
                "latest_timestamp_data_location": "s3://my_path/my_identifier_par_calc_upper/",
                "adso_type": "AQ",
                "partitionColumn": "RECORD",
                "numPartitions": 10,
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
            "partitions": ["REQTSN"],
            "location": "s3://my_path/my_identifier_par_calc_upper/",
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

#### 2.4 - Parallel Extraction, Provide Predicates (Recommended)
This scenario performs the extraction from SAP B4 ADSO in parallel, useful in contexts in which there is no
numeric, date or timestamp column to parallelize the extraction (e.g. when extracting from ADSO of Type `CL`,
the active table does not have the `RECORD` column, which is usually a good option for scenarios 2.2 and 2.3):
- `partitionColumn` - column used to split the extraction. It can be of any type.

This is an adequate example for you to follow if you have/know a column in the ADSO that is good to be used as
the `partitionColumn`, specially if these columns are not complying with the scenario 2.2 or 2.3.

**When this property is used all predicates need to be provided to Spark, otherwise it will leave data behind.**

Below the lakehouse function to generate predicate list automatically is presented.

This function needs to be used carefully, specially on predicates_query and predicates_add_null variables.

**predicates_query:** At the sample below the whole table is being considered (`select distinct(x) from table`),
but it is possible to filter predicates list here, specially if you are applying filter on transformations spec,
and you know entire table won't be necessary, so you can change it to something like this: `select distinct(x)
from table where x > y`.

**predicates_add_null:** You can decide if you want to consider null on predicates list or not, by default
this property is `True`.

**Example:** for `"partition_column": "CALMONTH"`

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

partition_column = "CALMONTH"
dbtable = "my_database.my_table_3"

predicates_query = f"""(SELECT DISTINCT({partition_column}) FROM {dbtable})"""
user = "my_user"
password = "my_b4_hana_pwd"
url = "my_sap_b4_url"
predicates_add_null = True

jdbc_util = JDBCExtractionUtils(
    JDBCExtraction(
        user=user,
        password=password,
        url=url,
        predicates_add_null=predicates_add_null,
        partition_column=partition_column,
        dbtable=dbtable,
    )
)

predicates = jdbc_util.get_predicates(predicates_query)

acon = {
    "input_specs": [
        {
            "spec_id": "my_identifier_2_source",
            "read_type": "batch",
            "data_format": "sap_b4",
            "options": {
                "url": "my_sap_b4_url",
                "user": "my_user",
                "password": "my_b4_hana_pwd",
                "driver": "com.sap.db.jdbc.Driver",
                "dbtable": "my_database.my_table_2",
                "changelog_table": "my_database.my_table_3",
                "extraction_type": extraction_type,
                "latest_timestamp_data_location": "s3://my_path/my_identifier_2_prov_predicates/",
                "adso_type": "CL",
                "predicates": predicates,
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_2_bronze",
            "input_id": "my_identifier_2_source",
            "write_type": write_type,
            "data_format": "delta",
            "partitions": ["REQTSN"],
            "location": "s3://my_path/my_identifier_2_prov_predicates/",
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

#### 2.5 - Parallel Extraction, Generate Predicates
This scenario is very similar to the scenario 2.4, with the only difference that it automatically
generates the predicates (`"generate_predicates": True`).

This is an adequate example for you to follow if you have/know a column in the ADSO that is good to be used as
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
            "spec_id": "my_identifier_2_source",
            "read_type": "batch",
            "data_format": "sap_b4",
            "generate_predicates": True,
            "options": {
                "url": "my_sap_b4_url",
                "user": "my_user",
                "password": "my_b4_hana_pwd",
                "driver": "com.sap.db.jdbc.Driver",
                "dbtable": "my_database.my_table_2",
                "changelog_table": "my_database.my_table_3",
                "extraction_type": extraction_type,
                "latest_timestamp_data_location": "s3://my_path/my_identifier_2_gen_predicates/",
                "adso_type": "CL",
                "partitionColumn": "CALMONTH",
            },
        }
    ],
    "output_specs": [
        {
            "spec_id": "my_identifier_2_bronze",
            "input_id": "my_identifier_2_source",
            "write_type": write_type,
            "data_format": "delta",
            "partitions": ["REQTSN"],
            "location": "s3://my_path/my_identifier_2_gen_predicates/",
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