"""Definitions of standard values and structures for core components."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional, Union

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class CollectEngineUsage(Enum):
    """Options for collecting engine usage stats.

    - enabled, enables the collection and storage of Lakehouse Engine
    usage statistics for any environment.
    - prod_only, enables the collection and storage of Lakehouse Engine
    usage statistics for production environment only.
    - disabled, disables the collection and storage of Lakehouse Engine
    usage statistics, for all environments.
    """

    ENABLED = "enabled"
    PROD_ONLY = "prod_only"
    DISABLED = "disabled"


@dataclass
class EngineConfig(object):
    """Definitions that can come from the Engine Config file.

    - dq_bucket: S3 prod bucket used to store data quality related artifacts.
    - dq_dev_bucket: S3 dev bucket used to store data quality related artifacts.
    - notif_disallowed_email_servers: email servers not allowed to be used
        for sending notifications.
    - engine_usage_path: path where the engine prod usage stats are stored.
    - engine_dev_usage_path: path where the engine dev usage stats are stored.
    - collect_engine_usage: whether to enable the collection of lakehouse
        engine usage stats or not.
    - dq_functions_column_list: list of columns to be added to the meta argument
        of GX when using PRISMA.
    """

    dq_bucket: Optional[str] = None
    dq_dev_bucket: Optional[str] = None
    notif_disallowed_email_servers: Optional[list] = None
    engine_usage_path: Optional[str] = None
    engine_dev_usage_path: Optional[str] = None
    collect_engine_usage: str = CollectEngineUsage.ENABLED.value
    dq_functions_column_list: Optional[list] = None
    sharepoint_authority: Optional[str] = None
    sharepoint_company_domain: Optional[str] = None
    sharepoint_api_domain: Optional[str] = None


class EngineStats(Enum):
    """Definitions for collection of Lakehouse Engine Stats.

    !!! note
        whenever the value comes from a key inside a Spark Config
        that returns an array, it can be specified with a '#' so that it
        is adequately processed.
    """

    CLUSTER_USAGE_TAGS = "spark.databricks.clusterUsageTags"
    DEF_SPARK_CONFS = {
        "dp_name": f"{CLUSTER_USAGE_TAGS}.clusterAllTags#accountName",
        "environment": f"{CLUSTER_USAGE_TAGS}.clusterAllTags#environment",
        "workspace_id": f"{CLUSTER_USAGE_TAGS}.orgId",
        "job_id": f"{CLUSTER_USAGE_TAGS}.clusterAllTags#JobId",
        "job_name": f"{CLUSTER_USAGE_TAGS}.clusterAllTags#RunName",
        "run_id": f"{CLUSTER_USAGE_TAGS}.clusterAllTags#ClusterName",
    }


class InputFormat(Enum):
    """Formats of algorithm input."""

    JDBC = "jdbc"
    AVRO = "avro"
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    DELTAFILES = "delta"
    CLOUDFILES = "cloudfiles"
    KAFKA = "kafka"
    SQL = "sql"
    SAP_BW = "sap_bw"
    SAP_B4 = "sap_b4"
    DATAFRAME = "dataframe"
    SFTP = "sftp"

    @classmethod
    def values(cls):  # type: ignore
        """Generates a list containing all enum values.

        Returns:
            A list with all enum values.
        """
        return (c.value for c in cls)

    @classmethod
    def exists(cls, input_format: str) -> bool:
        """Checks if the input format exists in the enum values.

        Args:
            input_format: format to check if exists.

        Returns:
            If the input format exists in our enum.
        """
        return input_format in cls.values()


# Formats of input that are considered files.
FILE_INPUT_FORMATS = [
    InputFormat.AVRO.value,
    InputFormat.JSON.value,
    InputFormat.PARQUET.value,
    InputFormat.CSV.value,
    InputFormat.DELTAFILES.value,
    InputFormat.CLOUDFILES.value,
]


class OutputFormat(Enum):
    """Formats of algorithm output."""

    JDBC = "jdbc"
    AVRO = "avro"
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    DELTAFILES = "delta"
    KAFKA = "kafka"
    CONSOLE = "console"
    NOOP = "noop"
    DATAFRAME = "dataframe"
    REST_API = "rest_api"
    FILE = "file"  # Internal use only
    TABLE = "table"  # Internal use only
    SHAREPOINT = "sharepoint"

    @classmethod
    def values(cls):  # type: ignore
        """Generates a list containing all enum values.

        Returns:
            A list with all enum values.
        """
        return (c.value for c in cls)

    @classmethod
    def exists(cls, output_format: str) -> bool:
        """Checks if the output format exists in the enum values.

        Args:
            output_format: format to check if exists.

        Returns:
            If the output format exists in our enum.
        """
        return output_format in cls.values()


# Formats of output that are considered files.
FILE_OUTPUT_FORMATS = [
    OutputFormat.AVRO.value,
    OutputFormat.JSON.value,
    OutputFormat.PARQUET.value,
    OutputFormat.CSV.value,
    OutputFormat.DELTAFILES.value,
]


class NotifierType(Enum):
    """Type of notifier available."""

    EMAIL = "email"


class NotificationRuntimeParameters(Enum):
    """Parameters to be replaced in runtime."""

    DATABRICKS_JOB_NAME = "databricks_job_name"
    DATABRICKS_WORKSPACE_ID = "databricks_workspace_id"


NOTIFICATION_RUNTIME_PARAMETERS = [
    NotificationRuntimeParameters.DATABRICKS_JOB_NAME.value,
    NotificationRuntimeParameters.DATABRICKS_WORKSPACE_ID.value,
]


class ReadType(Enum):
    """Define the types of read operations.

    - BATCH - read the data in batch mode (e.g., Spark batch).
    - STREAMING - read the data in streaming mode (e.g., Spark streaming).
    """

    BATCH = "batch"
    STREAMING = "streaming"


class ReadMode(Enum):
    """Different modes that control how we handle compliance to the provided schema.

    These read modes map to Spark's read modes at the moment.
    """

    PERMISSIVE = "PERMISSIVE"
    FAILFAST = "FAILFAST"
    DROPMALFORMED = "DROPMALFORMED"


class DQDefaults(Enum):
    """Defaults used on the data quality process."""

    FILE_SYSTEM_STORE = "file_system"
    FILE_SYSTEM_S3_STORE = "s3"
    DQ_BATCH_IDENTIFIERS = ["spec_id", "input_id", "timestamp"]
    DATASOURCE_CLASS_NAME = "Datasource"
    DATASOURCE_EXECUTION_ENGINE = "SparkDFExecutionEngine"
    DATA_CONNECTORS_CLASS_NAME = "RuntimeDataConnector"
    DATA_CONNECTORS_MODULE_NAME = "great_expectations.datasource.data_connector"
    DATA_CHECKPOINTS_CLASS_NAME = "SimpleCheckpoint"
    DATA_CHECKPOINTS_CONFIG_VERSION = 1.0
    STORE_BACKEND = "s3"
    EXPECTATIONS_STORE_PREFIX = "dq/expectations/"
    VALIDATIONS_STORE_PREFIX = "dq/validations/"
    DATA_DOCS_PREFIX = "dq/data_docs/site/"
    CHECKPOINT_STORE_PREFIX = "dq/checkpoints/"
    VALIDATION_COLUMN_IDENTIFIER = "validationresultidentifier"
    CUSTOM_EXPECTATION_LIST = [
        "expect_column_values_to_be_date_not_older_than",
        "expect_column_pair_a_to_be_smaller_or_equal_than_b",
        "expect_multicolumn_column_a_must_equal_b_or_c",
        "expect_queried_column_agg_value_to_be",
        "expect_column_pair_date_a_to_be_greater_than_or_equal_to_date_b",
        "expect_column_pair_a_to_be_not_equal_to_b",
        "expect_column_values_to_not_be_null_or_empty_string",
    ]
    DQ_VALIDATIONS_SCHEMA = StructType(
        [
            StructField(
                "dq_validations",
                StructType(
                    [
                        StructField("run_name", StringType()),
                        StructField("run_success", BooleanType()),
                        StructField("raised_exceptions", BooleanType()),
                        StructField("run_row_success", BooleanType()),
                        StructField(
                            "dq_failure_details",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("expectation_type", StringType()),
                                        StructField("kwargs", StringType()),
                                    ]
                                ),
                            ),
                        ),
                    ]
                ),
            )
        ]
    )


class WriteType(Enum):
    """Types of write operations."""

    OVERWRITE = "overwrite"
    COMPLETE = "complete"
    APPEND = "append"
    UPDATE = "update"
    MERGE = "merge"
    ERROR_IF_EXISTS = "error"
    IGNORE_IF_EXISTS = "ignore"


@dataclass
class InputSpec(object):
    """Specification of an algorithm input.

    This is very aligned with the way the execution environment connects to the sources
    (e.g., spark sources).

    - spec_id: spec_id of the input specification read_type: ReadType type of read
        operation.
    - data_format: format of the input.
    - sftp_files_format: format of the files (csv, fwf, json, xml...) in a sftp
        directory.
    - df_name: dataframe name.
    - db_table: table name in the form of `<db>.<table>`.
    - location: uri that identifies from where to read data in the specified format.
    - enforce_schema_from_table: if we want to enforce the table schema or not, by
        providing a table name in the form of `<db>.<table>`.
    - query: sql query to execute and return the dataframe. Use it if you do not want to
        read from a file system nor from a table, but rather from a sql query instead.
    - schema: dict representation of a schema of the input (e.g., Spark struct type
        schema).
    - schema_path: path to a file with a representation of a schema of the input (e.g.,
        Spark struct type schema).
    - disable_dbfs_retry: optional flag to disable file storage dbfs.
    - with_filepath: if we want to include the path of the file that is being read. Only
        works with the file reader (batch and streaming modes are supported).
    - options: dict with other relevant options according to the execution
        environment (e.g., spark) possible sources.
    - calculate_upper_bound: when to calculate upper bound to extract from SAP BW
        or not.
    - calc_upper_bound_schema: specific schema for the calculated upper_bound.
    - generate_predicates: when to generate predicates to extract from SAP BW or not.
    - predicates_add_null: if we want to include is null on partition by predicates.
    - temp_view: optional name of a view to point to the input dataframe to be used
        to create or replace a temp view on top of the dataframe.
    """

    spec_id: str
    read_type: str
    data_format: Optional[str] = None
    sftp_files_format: Optional[str] = None
    df_name: Optional[DataFrame] = None
    db_table: Optional[str] = None
    location: Optional[str] = None
    query: Optional[str] = None
    enforce_schema_from_table: Optional[str] = None
    schema: Optional[dict] = None
    schema_path: Optional[str] = None
    disable_dbfs_retry: bool = False
    with_filepath: bool = False
    options: Optional[dict] = None
    jdbc_args: Optional[dict] = None
    calculate_upper_bound: bool = False
    calc_upper_bound_schema: Optional[str] = None
    generate_predicates: bool = False
    predicates_add_null: bool = True
    temp_view: Optional[str] = None


@dataclass
class TransformerSpec(object):
    """Transformer Specification, i.e., a single transformation amongst many.

    - function: name of the function (or callable function) to be executed.
    - args: (not applicable if using a callable function) dict with the arguments
        to pass to the function `<k,v>` pairs with the name of the parameter of
        the function and the respective value.
    """

    function: str
    args: dict


@dataclass
class TransformSpec(object):
    """Transformation Specification.

    I.e., the specification that defines the many transformations to be done to the data
    that was read.

    - spec_id: id of the terminate specification
    - input_id: id of the corresponding input
    specification.
    - transformers: list of transformers to execute.
    - force_streaming_foreach_batch_processing: sometimes, when using streaming, we want
        to force the transform to be executed in the foreachBatch function to ensure
        non-supported streaming operations can be properly executed.
    """

    spec_id: str
    input_id: str
    transformers: List[TransformerSpec]
    force_streaming_foreach_batch_processing: bool = False


class DQType(Enum):
    """Available data quality tasks."""

    VALIDATOR = "validator"
    PRISMA = "prisma"


class DQExecutionPoint(Enum):
    """Available data quality execution points."""

    IN_MOTION = "in_motion"
    AT_REST = "at_rest"


class DQTableBaseParameters(Enum):
    """Base parameters for importing DQ rules from a table."""

    PRISMA_BASE_PARAMETERS = ["arguments", "dq_tech_function"]


@dataclass
class DQFunctionSpec(object):
    """Defines a data quality function specification.

    - function - name of the data quality function (expectation) to execute.
    It follows the great_expectations api https://greatexpectations.io/expectations/.
    - args - args of the function (expectation). Follow the same api as above.
    """

    function: str
    args: Optional[dict] = None


@dataclass
class DQSpec(object):
    """Data quality overall specification.

    - spec_id - id of the specification.
    - input_id - id of the input specification.
    - dq_type - type of DQ process to execute (e.g. validator).
    - dq_functions - list of function specifications to execute.
    - dq_db_table - name of table to derive the dq functions from.
    - dq_table_table_filter - name of the table which rules are to be applied in the
        validations (Only used when deriving dq functions).
    - dq_table_extra_filters - extra filters to be used when deriving dq functions.
        This is a sql expression to be applied to the dq_db_table.
    - execution_point - execution point of the dq functions. [at_rest, in_motion].
        This is set during the load_data or dq_validator functions.
    - unexpected_rows_pk - the list of columns composing the primary key of the
        source data to identify the rows failing the DQ validations. Note: only one
        of tbl_to_derive_pk or unexpected_rows_pk arguments need to be provided. It
        is mandatory to provide one of these arguments when using tag_source_data
        as True. When tag_source_data is False, this is not mandatory, but still
        recommended.
    - tbl_to_derive_pk - db.table to automatically derive the unexpected_rows_pk from.
        Note: only one of tbl_to_derive_pk or unexpected_rows_pk arguments need to
        be provided. It is mandatory to provide one of these arguments when using
        tag_source_data as True. hen tag_source_data is False, this is not
        mandatory, but still recommended.
    - sort_processed_keys - when using the `prisma` `dq_type`, a column `processed_keys`
        is automatically added to give observability over the PK values that were
        processed during a run. This parameter (`sort_processed_keys`) controls whether
        the processed keys column value should be sorted or not. Default: False.
    - gx_result_format - great expectations result format. Default: "COMPLETE".
    - tag_source_data - when set to true, this will ensure that the DQ process ends by
        tagging the source data with an additional column with information about the
        DQ results. This column makes it possible to identify if the DQ run was
        succeeded in general and, if not, it unlocks the insights to know what
        specific rows have made the DQ validations fail and why. Default: False.
        Note: it only works if result_sink_explode is True, gx_result_format is
        COMPLETE, fail_on_error is False (which is done automatically when
        you specify tag_source_data as True) and tbl_to_derive_pk or
        unexpected_rows_pk is configured.
    - store_backend - which store_backend to use (e.g. s3 or file_system).
    - local_fs_root_dir - path of the root directory. Note: only applicable for
        store_backend file_system.
    - data_docs_local_fs - the path for data docs only for store_backend
        file_system.
    - bucket - the bucket name to consider for the store_backend (store DQ artefacts).
        Note: only applicable for store_backend s3.
    - data_docs_bucket - the bucket name for data docs only. When defined, it will
        supersede bucket parameter. Note: only applicable for store_backend s3.
    - expectations_store_prefix - prefix where to store expectations' data. Note: only
        applicable for store_backend s3.
    - validations_store_prefix - prefix where to store validations' data. Note: only
        applicable for store_backend s3.
    - data_docs_prefix - prefix where to store data_docs' data.
    - checkpoint_store_prefix - prefix where to store checkpoints' data. Note: only
        applicable for store_backend s3.
    - data_asset_name - name of the data asset to consider when configuring the great
        expectations' data source.
    - expectation_suite_name - name to consider for great expectations' suite.
    - result_sink_db_table - db.table_name indicating the database and table in which
        to save the results of the DQ process.
    - result_sink_location - file system location in which to save the results of the
        DQ process.
    - data_product_name - name of the data product.
    - result_sink_partitions - the list of partitions to consider.
    - result_sink_format - format of the result table (e.g. delta, parquet, kafka...).
    - result_sink_options - extra spark options for configuring the result sink.
        E.g: can be used to configure a Kafka sink if result_sink_format is kafka.
    - result_sink_explode - flag to determine if the output table/location should have
        the columns exploded (as True) or not (as False). Default: True.
    - result_sink_extra_columns - list of extra columns to be exploded (following
        the pattern "<name>.*") or columns to be selected. It is only used when
        result_sink_explode is set to True.
    - source - name of data source, to be easier to identify in analysis. If not
        specified, it is set as default <input_id>. This will be only used
        when result_sink_explode is set to True.
    - fail_on_error - whether to fail the algorithm if the validations of your data in
        the DQ process failed.
    - cache_df - whether to cache the dataframe before running the DQ process or not.
    - critical_functions - functions that should not fail. When this argument is
        defined, fail_on_error is nullified.
    - max_percentage_failure - percentage of failure that should be allowed.
        This argument has priority over both fail_on_error and critical_functions.
    """

    spec_id: str
    input_id: str
    dq_type: str
    dq_functions: Optional[List[DQFunctionSpec]] = None
    dq_db_table: Optional[str] = None
    dq_table_table_filter: Optional[str] = None
    dq_table_extra_filters: Optional[str] = None
    execution_point: Optional[str] = None
    unexpected_rows_pk: Optional[List[str]] = None
    tbl_to_derive_pk: Optional[str] = None
    sort_processed_keys: Optional[bool] = False
    gx_result_format: Optional[str] = "COMPLETE"
    tag_source_data: Optional[bool] = False
    store_backend: str = DQDefaults.STORE_BACKEND.value
    local_fs_root_dir: Optional[str] = None
    data_docs_local_fs: Optional[str] = None
    bucket: Optional[str] = None
    data_docs_bucket: Optional[str] = None
    expectations_store_prefix: str = DQDefaults.EXPECTATIONS_STORE_PREFIX.value
    validations_store_prefix: str = DQDefaults.VALIDATIONS_STORE_PREFIX.value
    data_docs_prefix: str = DQDefaults.DATA_DOCS_PREFIX.value
    checkpoint_store_prefix: str = DQDefaults.CHECKPOINT_STORE_PREFIX.value
    data_asset_name: Optional[str] = None
    expectation_suite_name: Optional[str] = None
    result_sink_db_table: Optional[str] = None
    result_sink_location: Optional[str] = None
    data_product_name: Optional[str] = None
    result_sink_partitions: Optional[List[str]] = None
    result_sink_format: str = OutputFormat.DELTAFILES.value
    result_sink_options: Optional[dict] = None
    result_sink_explode: bool = True
    result_sink_extra_columns: Optional[List[str]] = None
    source: Optional[str] = None
    fail_on_error: bool = True
    cache_df: bool = False
    critical_functions: Optional[List[DQFunctionSpec]] = None
    max_percentage_failure: Optional[float] = None


@dataclass
class MergeOptions(object):
    """Options for a merge operation.

    - merge_predicate: predicate to apply to the merge operation so that we can
        check if a new record corresponds to a record already included in the
        historical data.
    - insert_only: indicates if the merge should only insert data (e.g., deduplicate
        scenarios).
    - delete_predicate: predicate to apply to the delete operation.
    - update_predicate: predicate to apply to the update operation.
    - insert_predicate: predicate to apply to the insert operation.
    - update_column_set: rules to apply to the update operation which allows to
        set the value for each column to be updated.
        (e.g. {"data": "new.data", "count": "current.count + 1"} )
    - insert_column_set: rules to apply to the insert operation which allows to
        set the value for each column to be inserted.
        (e.g. {"date": "updates.date", "count": "1"} )
    """

    merge_predicate: str
    insert_only: bool = False
    delete_predicate: Optional[str] = None
    update_predicate: Optional[str] = None
    insert_predicate: Optional[str] = None
    update_column_set: Optional[dict] = None
    insert_column_set: Optional[dict] = None


@dataclass
class SharepointOptions(object):
    """options for a sharepoint write operation.

    - client_id (str): azure client ID application.
    - tenant_id (str): tenant ID associated with the SharePoint site.
    - site_name (str): name of the SharePoint site where the document library resides.
    - drive_name (str): name of the document library where the file will be uploaded.
    - file_name (str): name of the file to be uploaded to local path and to SharePoint.
    - secret (str): client secret for authentication.
    - local_path (str): local path (or similar, e.g. mounted file systems like
        databricks volumes or dbfs) where files will be temporarily stored during
        the SharePoint write operation.
    - api_version (str): version of the Graph SharePoint API to be used for operations.
    - folder_relative_path (Optional[str]): relative folder path within the document
        library to upload the file.
    - chunk_size (Optional[int]): Optional; size (in Bytes) of the file chunks for
        uploading to SharePoint. Default is 100 Mb.
    - local_options (Optional[dict]): Optional; additional options for customizing
        write to csv action to local path. You can check the available options
        here: https://spark.apache.org/docs/3.5.3/sql-data-sources-csv.html
    - conflict_behaviour (Optional[str]): Optional; behavior to adopt in case
        of a conflict (e.g., 'replace', 'fail').
    """

    client_id: str
    tenant_id: str
    site_name: str
    drive_name: str
    file_name: str
    secret: str
    local_path: str
    api_version: str = "v1.0"
    folder_relative_path: Optional[str] = None
    chunk_size: Optional[int] = 100 * 1024 * 1024  # 100 MB
    local_options: Optional[dict] = None
    conflict_behaviour: Optional[str] = None


@dataclass
class OutputSpec(object):
    """Specification of an algorithm output.

    This is very aligned with the way the execution environment connects to the output
    systems (e.g., spark outputs).

    - spec_id: id of the output specification.
    - input_id: id of the corresponding input specification.
    - write_type: type of write operation.
    - data_format: format of the output. Defaults to DELTA.
    - db_table: table name in the form of `<db>.<table>`.
    - location: uri that identifies from where to write data in the specified format.
    - sharepoint_opts: options to apply on writing on sharepoint operations.
    - partitions: list of partition input_col names.
    - merge_opts: options to apply to the merge operation.
    - streaming_micro_batch_transformers: transformers to invoke for each streaming
        micro batch, before writing (i.e., in Spark's foreachBatch structured
        streaming function). Note: the lakehouse engine manages this for you, so
        you don't have to manually specify streaming transformations here, so we don't
        advise you to manually specify transformations through this parameter. Supply
        them as regular transformers in the transform_specs sections of an ACON.
    - streaming_once: if the streaming query is to be executed just once, or not,
        generating just one micro batch.
    - streaming_processing_time: if streaming query is to be kept alive, this indicates
        the processing time of each micro batch.
    - streaming_available_now: if set to True, set a trigger that processes all
        available data in multiple batches then terminates the query.
        When using streaming, this is the default trigger that the lakehouse-engine will
        use, unless you configure a different one.
    - streaming_continuous: set a trigger that runs a continuous query with a given
        checkpoint interval.
    - streaming_await_termination: whether to wait (True) for the termination of the
        streaming query (e.g. timeout or exception) or not (False). Default: True.
    - streaming_await_termination_timeout: a timeout to set to the
        streaming_await_termination. Default: None.
    - with_batch_id: whether to include the streaming batch id in the final data,
        or not. It only takes effect in streaming mode.
    - options: dict with other relevant options according to the execution environment
        (e.g., spark) possible outputs.  E.g.,: JDBC options, checkpoint location for
        streaming, etc.
    - streaming_micro_batch_dq_processors: similar to streaming_micro_batch_transformers
        but for the DQ functions to be executed. Used internally by the lakehouse
        engine, so you don't have to supply DQ functions through this parameter. Use the
        dq_specs of the acon instead.
    """

    spec_id: str
    input_id: str
    write_type: str
    data_format: str = OutputFormat.DELTAFILES.value
    db_table: Optional[str] = None
    location: Optional[str] = None
    sharepoint_opts: Optional[SharepointOptions] = None
    merge_opts: Optional[MergeOptions] = None
    partitions: Optional[List[str]] = None
    streaming_micro_batch_transformers: Optional[List[TransformerSpec]] = None
    streaming_once: Optional[bool] = None
    streaming_processing_time: Optional[str] = None
    streaming_available_now: bool = True
    streaming_continuous: Optional[str] = None
    streaming_await_termination: bool = True
    streaming_await_termination_timeout: Optional[int] = None
    with_batch_id: bool = False
    options: Optional[dict] = None
    streaming_micro_batch_dq_processors: Optional[List[DQSpec]] = None


@dataclass
class TerminatorSpec(object):
    """Terminator Specification.

    I.e., the specification that defines a terminator operation to be executed. Examples
    are compute statistics, vacuum, optimize, etc.

    - function: terminator function to execute.
    - args: arguments of the terminator function.
    - input_id: id of the corresponding output specification (Optional).
    """

    function: str
    args: Optional[dict] = None
    input_id: Optional[str] = None


@dataclass
class ReconciliatorSpec(object):
    """Reconciliator Specification.

    - metrics: list of metrics in the form of:
        [{
            metric: name of the column present in both truth and current datasets,
            aggregation: sum, avg, max, min, ...,
            type: percentage or absolute,
            yellow: value,
            red: value
        }].
    - recon_type: reconciliation type (percentage or absolute). Percentage calculates
        the difference between truth and current results as a percentage (x-y/x), and
        absolute calculates the raw difference (x - y).
    - truth_input_spec: input specification of the truth data.
    - current_input_spec: input specification of the current results data
    - truth_preprocess_query: additional query on top of the truth input data to
        preprocess the truth data before it gets fueled into the reconciliation process.
        Important note: you need to assume that the data out of
        the truth_input_spec is referencable by a table called 'truth'.
    - truth_preprocess_query_args: optional dict having the functions/transformations to
        apply on top of the truth_preprocess_query and respective arguments. Note: cache
        is being applied on the Dataframe, by default. For turning the default behavior
        off, pass `"truth_preprocess_query_args": []`.
    - current_preprocess_query: additional query on top of the current results input
        data to preprocess the current results data before it gets fueled into the
        reconciliation process. Important note: you need to assume that the data out of
        the current_results_input_spec is referencable by a table called 'current'.
    - current_preprocess_query_args: optional dict having the
        functions/transformations to apply on top of the current_preprocess_query
        and respective arguments. Note: cache is being applied on the Dataframe,
        by default. For turning the default behavior off, pass
        `"current_preprocess_query_args": []`.
    - ignore_empty_df: optional boolean, to ignore the recon process if source & target
       dataframes are empty, recon will exit success code (passed)
    """

    metrics: List[dict]
    truth_input_spec: InputSpec
    current_input_spec: InputSpec
    truth_preprocess_query: Optional[str] = None
    truth_preprocess_query_args: Optional[List[dict]] = None
    current_preprocess_query: Optional[str] = None
    current_preprocess_query_args: Optional[List[dict]] = None
    ignore_empty_df: Optional[bool] = False


@dataclass
class DQValidatorSpec(object):
    """Data Quality Validator Specification.

    - input_spec: input specification of the data to be checked/validated.
    - dq_spec: data quality specification.
    - restore_prev_version: specify if, having
        delta table/files as input, they should be restored to the
        previous version if the data quality process fails. Note: this
        is only considered if fail_on_error is kept as True.
    """

    input_spec: InputSpec
    dq_spec: DQSpec
    restore_prev_version: Optional[bool] = False


class SQLDefinitions(Enum):
    """SQL definitions statements."""

    compute_table_stats = "ANALYZE TABLE {} COMPUTE STATISTICS"
    drop_table_stmt = "DROP TABLE IF EXISTS"
    drop_view_stmt = "DROP VIEW IF EXISTS"
    truncate_stmt = "TRUNCATE TABLE"
    describe_stmt = "DESCRIBE TABLE"
    optimize_stmt = "OPTIMIZE"
    show_tbl_props_stmt = "SHOW TBLPROPERTIES"
    delete_where_stmt = "DELETE FROM {} WHERE {}"


class FileManagerAPIKeys(Enum):
    """File Manager s3 api keys."""

    CONTENTS = "Contents"
    KEY = "Key"
    CONTINUATION = "NextContinuationToken"
    BUCKET = "Bucket"
    OBJECTS = "Objects"


@dataclass
class SensorSpec(object):
    """Sensor Specification.

    - sensor_id: sensor id.
    - assets: a list of assets that are considered as available to
        consume downstream after this sensor has status
        PROCESSED_NEW_DATA.
    - control_db_table_name: db.table to store sensor metadata.
    - input_spec: input specification of the source to be checked for new data.
    - preprocess_query: SQL query to transform/filter the result from the
        upstream. Consider that we should refer to 'new_data' whenever
        we are referring to the input of the sensor. E.g.:
            "SELECT dummy_col FROM new_data WHERE ..."
    - checkpoint_location: optional location to store checkpoints to resume
        from. These checkpoints use the same as Spark checkpoint strategy.
        For Spark readers that do not support checkpoints, use the
        preprocess_query parameter to form a SQL query to filter the result
        from the upstream accordingly.
    - fail_on_empty_result: if the sensor should throw an error if there is no new
        data in the upstream. Default: True.
    """

    sensor_id: str
    assets: List[str]
    control_db_table_name: str
    input_spec: InputSpec
    preprocess_query: Optional[str]
    checkpoint_location: Optional[str]
    fail_on_empty_result: bool = True

    @classmethod
    def create_from_acon(cls, acon: dict):  # type: ignore
        """Create SensorSpec from acon.

        Args:
            acon: sensor ACON.
        """
        checkpoint_location = acon.get("base_checkpoint_location")
        if checkpoint_location:
            checkpoint_location = (
                f"{checkpoint_location.rstrip('/')}/lakehouse_engine/"
                f"sensors/{acon['sensor_id']}"
            )

        return cls(
            sensor_id=acon["sensor_id"],
            assets=acon["assets"],
            control_db_table_name=acon["control_db_table_name"],
            input_spec=InputSpec(**acon["input_spec"]),
            preprocess_query=acon.get("preprocess_query"),
            checkpoint_location=checkpoint_location,
            fail_on_empty_result=acon.get("fail_on_empty_result", True),
        )


class SensorStatus(Enum):
    """Status for a sensor."""

    ACQUIRED_NEW_DATA = "ACQUIRED_NEW_DATA"
    PROCESSED_NEW_DATA = "PROCESSED_NEW_DATA"


SENSOR_SCHEMA = StructType(
    [
        StructField("sensor_id", StringType(), False),
        StructField("assets", ArrayType(StringType(), False), True),
        StructField("status", StringType(), False),
        StructField("status_change_timestamp", TimestampType(), False),
        StructField("checkpoint_location", StringType(), True),
        StructField("upstream_key", StringType(), True),
        StructField("upstream_value", StringType(), True),
    ]
)

SENSOR_UPDATE_SET: dict = {
    "sensors.sensor_id": "updates.sensor_id",
    "sensors.status": "updates.status",
    "sensors.status_change_timestamp": "updates.status_change_timestamp",
}

SENSOR_ALLOWED_DATA_FORMATS = {
    ReadType.STREAMING.value: [InputFormat.KAFKA.value, *FILE_INPUT_FORMATS],
    ReadType.BATCH.value: [
        InputFormat.DELTAFILES.value,
        InputFormat.JDBC.value,
    ],
}


class SAPLogchain(Enum):
    """Defaults used on consuming data from SAP Logchain."""

    DBTABLE = "SAPPHA.RSPCLOGCHAIN"
    GREEN_STATUS = "G"
    ENGINE_TABLE = "sensor_new_data"


class RestoreType(Enum):
    """Archive types."""

    BULK = "Bulk"
    STANDARD = "Standard"
    EXPEDITED = "Expedited"

    @classmethod
    def values(cls):  # type: ignore
        """Generates a list containing all enum values.

        Returns:
            A list with all enum values.
        """
        return (c.value for c in cls)

    @classmethod
    def exists(cls, restore_type: str) -> bool:
        """Checks if the restore type exists in the enum values.

        Args:
            restore_type: restore type to check if exists.

        Returns:
            If the restore type exists in our enum.
        """
        return restore_type in cls.values()


class RestoreStatus(Enum):
    """Archive types."""

    NOT_STARTED = "not_started"
    ONGOING = "ongoing"
    RESTORED = "restored"


ARCHIVE_STORAGE_CLASS = [
    "GLACIER",
    "DEEP_ARCHIVE",
    "GLACIER_IR",
]


class SQLParser(Enum):
    """Defaults to use for parsing."""

    DOUBLE_QUOTES = '"'
    SINGLE_QUOTES = "'"
    BACKSLASH = "\\"
    SINGLE_TRACE = "-"
    DOUBLE_TRACES = "--"
    SLASH = "/"
    OPENING_MULTIPLE_LINE_COMMENT = "/*"
    CLOSING_MULTIPLE_LINE_COMMENT = "*/"
    PARAGRAPH = "\n"
    STAR = "*"

    MULTIPLE_LINE_COMMENT = [
        OPENING_MULTIPLE_LINE_COMMENT,
        CLOSING_MULTIPLE_LINE_COMMENT,
    ]


class GABDefaults(Enum):
    """Defaults used on the GAB process."""

    DATE_FORMAT = "%Y-%m-%d"
    DIMENSIONS_DEFAULT_COLUMNS = ["from_date", "to_date"]
    DEFAULT_DIMENSION_CALENDAR_TABLE = "dim_calendar"
    DEFAULT_LOOKUP_QUERY_BUILDER_TABLE = "lkp_query_builder"


class GABStartOfWeek(Enum):
    """Representation of start of week values on GAB."""

    SUNDAY = "S"
    MONDAY = "M"

    @classmethod
    def get_start_of_week(cls) -> dict:
        """Get the start of week enum as a dict.

        Returns:
            dict containing all enum entries as `{name:value}`.
        """
        return {
            start_of_week.name: start_of_week.value
            for start_of_week in list(GABStartOfWeek)
        }

    @classmethod
    def get_values(cls) -> set[str]:
        """Get the start of week enum values as set.

        Returns:
            set containing all possible values `{value}`.
        """
        return {start_of_week.value for start_of_week in list(GABStartOfWeek)}


@dataclass
class GABSpec(object):
    """Gab Specification.

    - query_label_filter: query use-case label to execute.
    - queue_filter: queue to execute the job.
    - cadence_filter: selected cadences to build the asset.
    - target_database: target database to write.
    - curr_date: current date.
    - start_date: period start date.
    - end_date: period end date.
    - rerun_flag: rerun flag.
    - target_table: target table to write.
    - source_database: source database.
    - gab_base_path: base path to read the use cases.
    - lookup_table: gab configuration table.
    - calendar_table: gab calendar table.
    """

    query_label_filter: list[str]
    queue_filter: list[str]
    cadence_filter: list[str]
    target_database: str
    current_date: datetime
    start_date: datetime
    end_date: datetime
    rerun_flag: str
    target_table: str
    source_database: str
    gab_base_path: str
    lookup_table: str
    calendar_table: str

    @classmethod
    def create_from_acon(cls, acon: dict):  # type: ignore
        """Create GabSpec from acon.

        Args:
            acon: gab ACON.
        """
        lookup_table = f"{acon['source_database']}." + (
            acon.get(
                "lookup_table", GABDefaults.DEFAULT_LOOKUP_QUERY_BUILDER_TABLE.value
            )
        )

        calendar_table = f"{acon['source_database']}." + (
            acon.get(
                "calendar_table", GABDefaults.DEFAULT_DIMENSION_CALENDAR_TABLE.value
            )
        )

        def format_date(date_to_format: Union[datetime, str]) -> datetime:
            if isinstance(date_to_format, str):
                return datetime.strptime(date_to_format, GABDefaults.DATE_FORMAT.value)
            else:
                return date_to_format

        return cls(
            query_label_filter=acon["query_label_filter"],
            queue_filter=acon["queue_filter"],
            cadence_filter=acon["cadence_filter"],
            target_database=acon["target_database"],
            current_date=datetime.now(),
            start_date=format_date(acon["start_date"]),
            end_date=format_date(acon["end_date"]),
            rerun_flag=acon["rerun_flag"],
            target_table=acon["target_table"],
            source_database=acon["source_database"],
            gab_base_path=acon["gab_base_path"],
            lookup_table=lookup_table,
            calendar_table=calendar_table,
        )


class GABCadence(Enum):
    """Representation of the supported cadences on GAB."""

    DAY = 1
    WEEK = 2
    MONTH = 3
    QUARTER = 4
    YEAR = 5

    @classmethod
    def get_ordered_cadences(cls) -> dict:
        """Get the cadences ordered by the value.

        Returns:
            dict containing ordered cadences as `{name:value}`.
        """
        cadences = list(GABCadence)
        return {
            cadence.name: cadence.value
            for cadence in sorted(cadences, key=lambda gab_cadence: gab_cadence.value)
        }

    @classmethod
    def get_cadences(cls) -> set[str]:
        """Get the cadences values as set.

        Returns:
            set containing all possible cadence values as `{value}`.
        """
        return {cadence.name for cadence in list(GABCadence)}

    @classmethod
    def order_cadences(cls, cadences_to_order: list[str]) -> list[str]:
        """Order a list of cadences by value.

        Returns:
            ordered set containing the received cadences.
        """
        return sorted(
            cadences_to_order,
            key=lambda item: cls.get_ordered_cadences().get(item),  # type: ignore
        )


class GABKeys:
    """Constants used to update pre-configured gab dict key."""

    JOIN_SELECT = "join_select"
    PROJECT_START = "project_start"
    PROJECT_END = "project_end"


class GABReplaceableKeys:
    """Constants used to replace pre-configured gab dict values."""

    CADENCE = "${cad}"
    DATE_COLUMN = "${date_column}"
    CONFIG_WEEK_START = "${config_week_start}"
    RECONCILIATION_CADENCE = "${rec_cadence}"


class GABCombinedConfiguration(Enum):
    """GAB combined configuration.

    Based on the use case configuration return the values to override in the SQL file.
    This enum aims to exhaustively map each combination of `cadence`, `reconciliation`,
        `week_start` and `snap_flag` return the corresponding values `join_select`,
        `project_start` and `project_end` to replace this values in the stages SQL file.

    Return corresponding configuration (join_select, project_start, project_end) for
        each combination (cadence x recon x week_start x snap_flag).
    """

    _PROJECT_DATE_COLUMN_TRUNCATED_BY_CADENCE = (
        "date(date_trunc('${cad}',${date_column}))"
    )
    _DEFAULT_PROJECT_START = "df_cal.cadence_start_date"
    _DEFAULT_PROJECT_END = "df_cal.cadence_end_date"

    COMBINED_CONFIGURATION = {
        # Combination of:
        # - cadence: `DAY`
        # - reconciliation_window: `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`
        # - week_start: `S`, `M`
        # - snapshot_flag: `Y`, `N`
        1: {
            "cadence": GABCadence.DAY.name,
            "recon": GABCadence.get_cadences(),
            "week_start": GABStartOfWeek.get_values(),
            "snap_flag": {"Y", "N"},
            "join_select": "",
            "project_start": _PROJECT_DATE_COLUMN_TRUNCATED_BY_CADENCE,
            "project_end": _PROJECT_DATE_COLUMN_TRUNCATED_BY_CADENCE,
        },
        # Combination of:
        # - cadence: `WEEK`
        # - reconciliation_window: `DAY`
        # - week_start: `S`, `M`
        # - snapshot_flag: `Y`
        2: {
            "cadence": GABCadence.WEEK.name,
            "recon": GABCadence.DAY.name,
            "week_start": GABStartOfWeek.get_values(),
            "snap_flag": "Y",
            "join_select": """
            select distinct case
                when '${config_week_start}' = 'Monday' then weekstart_mon
                when '${config_week_start}' = 'Sunday' then weekstart_sun
            end as cadence_start_date,
            calendar_date as cadence_end_date
        """,
            "project_start": _DEFAULT_PROJECT_START,
            "project_end": _DEFAULT_PROJECT_END,
        },
        # Combination of:
        # - cadence: `WEEK`
        # - reconciliation_window: `DAY, `MONTH`, `QUARTER`, `YEAR`
        # - week_start: `M`
        # - snapshot_flag: `Y`, `N`
        3: {
            "cadence": GABCadence.WEEK.name,
            "recon": {
                GABCadence.DAY.name,
                GABCadence.MONTH.name,
                GABCadence.QUARTER.name,
                GABCadence.YEAR.name,
            },
            "week_start": "M",
            "snap_flag": {"Y", "N"},
            "join_select": """
            select distinct case
                when '${config_week_start}'  = 'Monday' then weekstart_mon
                when '${config_week_start}' = 'Sunday' then weekstart_sun
            end as cadence_start_date,
            case
                when '${config_week_start}' = 'Monday' then weekend_mon
                when '${config_week_start}' = 'Sunday' then weekend_sun
            end as cadence_end_date""",
            "project_start": _DEFAULT_PROJECT_START,
            "project_end": _DEFAULT_PROJECT_END,
        },
        4: {
            "cadence": GABCadence.MONTH.name,
            "recon": GABCadence.DAY.name,
            "week_start": GABStartOfWeek.get_values(),
            "snap_flag": "Y",
            "join_select": """
            select distinct month_start as cadence_start_date,
            calendar_date as cadence_end_date
        """,
            "project_start": _DEFAULT_PROJECT_START,
            "project_end": _DEFAULT_PROJECT_END,
        },
        5: {
            "cadence": GABCadence.MONTH.name,
            "recon": GABCadence.WEEK.name,
            "week_start": GABStartOfWeek.MONDAY.value,
            "snap_flag": "Y",
            "join_select": """
            select distinct month_start as cadence_start_date,
            case
                when date(
                    date_trunc('MONTH',add_months(calendar_date, 1))
                )-1 < weekend_mon
                    then date(date_trunc('MONTH',add_months(calendar_date, 1)))-1
                else weekend_mon
            end as cadence_end_date""",
            "project_start": _DEFAULT_PROJECT_START,
            "project_end": _DEFAULT_PROJECT_END,
        },
        6: {
            "cadence": GABCadence.MONTH.name,
            "recon": GABCadence.WEEK.name,
            "week_start": GABStartOfWeek.SUNDAY.value,
            "snap_flag": "Y",
            "join_select": """
            select distinct month_start as cadence_start_date,
            case
                when date(
                    date_trunc('MONTH',add_months(calendar_date, 1))
                )-1 < weekend_sun
                    then date(date_trunc('MONTH',add_months(calendar_date, 1)))-1
                else weekend_sun
            end as cadence_end_date""",
            "project_start": _DEFAULT_PROJECT_START,
            "project_end": _DEFAULT_PROJECT_END,
        },
        7: {
            "cadence": GABCadence.MONTH.name,
            "recon": GABCadence.get_cadences(),
            "week_start": GABStartOfWeek.get_values(),
            "snap_flag": {"Y", "N"},
            "join_select": "",
            "project_start": _PROJECT_DATE_COLUMN_TRUNCATED_BY_CADENCE,
            "project_end": "date(date_trunc('MONTH',add_months(${date_column}, 1)))-1",
        },
        8: {
            "cadence": GABCadence.QUARTER.name,
            "recon": GABCadence.DAY.name,
            "week_start": GABStartOfWeek.get_values(),
            "snap_flag": "Y",
            "join_select": """
            select distinct quarter_start as cadence_start_date,
            calendar_date as cadence_end_date
        """,
            "project_start": _DEFAULT_PROJECT_START,
            "project_end": _DEFAULT_PROJECT_END,
        },
        9: {
            "cadence": GABCadence.QUARTER.name,
            "recon": GABCadence.WEEK.name,
            "week_start": GABStartOfWeek.MONDAY.value,
            "snap_flag": "Y",
            "join_select": """
            select distinct quarter_start as cadence_start_date,
            case
                when weekend_mon > date(
                    date_trunc('QUARTER',add_months(calendar_date, 3))
                )-1
                    then date(date_trunc('QUARTER',add_months(calendar_date, 3)))-1
                else weekend_mon
            end as cadence_end_date""",
            "project_start": _DEFAULT_PROJECT_START,
            "project_end": _DEFAULT_PROJECT_END,
        },
        10: {
            "cadence": GABCadence.QUARTER.name,
            "recon": GABCadence.WEEK.name,
            "week_start": GABStartOfWeek.SUNDAY.value,
            "snap_flag": "Y",
            "join_select": """
            select distinct quarter_start as cadence_start_date,
            case
                when weekend_sun > date(
                    date_trunc('QUARTER',add_months(calendar_date, 3))
                )-1
                    then date(date_trunc('QUARTER',add_months(calendar_date, 3)))-1
                else weekend_sun
            end as cadence_end_date""",
            "project_start": _DEFAULT_PROJECT_START,
            "project_end": _DEFAULT_PROJECT_END,
        },
        11: {
            "cadence": GABCadence.QUARTER.name,
            "recon": GABCadence.MONTH.name,
            "week_start": GABStartOfWeek.get_values(),
            "snap_flag": "Y",
            "join_select": """
            select distinct quarter_start as cadence_start_date,
            month_end as cadence_end_date
        """,
            "project_start": _DEFAULT_PROJECT_START,
            "project_end": _DEFAULT_PROJECT_END,
        },
        12: {
            "cadence": GABCadence.QUARTER.name,
            "recon": GABCadence.YEAR.name,
            "week_start": GABStartOfWeek.get_values(),
            "snap_flag": "N",
            "join_select": "",
            "project_start": _PROJECT_DATE_COLUMN_TRUNCATED_BY_CADENCE,
            "project_end": """
            date(
                date_trunc(
                    '${cad}',add_months(date(date_trunc('${cad}',${date_column})), 3)
                )
            )-1
        """,
        },
        13: {
            "cadence": GABCadence.QUARTER.name,
            "recon": GABCadence.get_cadences(),
            "week_start": GABStartOfWeek.get_values(),
            "snap_flag": "N",
            "join_select": "",
            "project_start": _PROJECT_DATE_COLUMN_TRUNCATED_BY_CADENCE,
            "project_end": """
            date(
                date_trunc(
                    '${cad}',add_months( date(date_trunc('${cad}',${date_column})), 3)
                )
            )-1
        """,
        },
        14: {
            "cadence": GABCadence.YEAR.name,
            "recon": GABCadence.WEEK.name,
            "week_start": GABStartOfWeek.MONDAY.value,
            "snap_flag": "Y",
            "join_select": """
            select distinct year_start as cadence_start_date,
            case
                when weekend_mon > date(
                    date_trunc('YEAR',add_months(calendar_date, 12))
                )-1
                    then date(date_trunc('YEAR',add_months(calendar_date, 12)))-1
                else weekend_mon
            end as cadence_end_date""",
            "project_start": _DEFAULT_PROJECT_START,
            "project_end": _DEFAULT_PROJECT_END,
        },
        15: {
            "cadence": GABCadence.YEAR.name,
            "recon": GABCadence.WEEK.name,
            "week_start": GABStartOfWeek.SUNDAY.value,
            "snap_flag": "Y",
            "join_select": """
            select distinct year_start as cadence_start_date,
            case
                when weekend_sun > date(
                    date_trunc('YEAR',add_months(calendar_date, 12))
                )-1
                    then date(date_trunc('YEAR',add_months(calendar_date, 12)))-1
                else weekend_sun
            end as cadence_end_date""",
            "project_start": _DEFAULT_PROJECT_START,
            "project_end": _DEFAULT_PROJECT_END,
        },
        16: {
            "cadence": GABCadence.YEAR.name,
            "recon": GABCadence.get_cadences(),
            "week_start": GABStartOfWeek.get_values(),
            "snap_flag": "N",
            "inverse_flag": "Y",
            "join_select": "",
            "project_start": _PROJECT_DATE_COLUMN_TRUNCATED_BY_CADENCE,
            "project_end": """
            date(
                date_trunc(
                    '${cad}',add_months(date(date_trunc('${cad}',${date_column})), 12)
                )
            )-1
        """,
        },
        17: {
            "cadence": GABCadence.YEAR.name,
            "recon": {
                GABCadence.DAY.name,
                GABCadence.MONTH.name,
                GABCadence.QUARTER.name,
            },
            "week_start": GABStartOfWeek.get_values(),
            "snap_flag": "Y",
            "join_select": """
            select distinct year_start as cadence_start_date,
            case
                when '${rec_cadence}' = 'DAY' then calendar_date
                when '${rec_cadence}' = 'MONTH' then month_end
                when '${rec_cadence}' = 'QUARTER' then quarter_end
            end as cadence_end_date
        """,
            "project_start": _DEFAULT_PROJECT_START,
            "project_end": _DEFAULT_PROJECT_END,
        },
        18: {
            "cadence": GABCadence.get_cadences(),
            "recon": GABCadence.get_cadences(),
            "week_start": GABStartOfWeek.get_values(),
            "snap_flag": {"Y", "N"},
            "join_select": """
            select distinct
            case
                when '${cad}' = 'WEEK' and '${config_week_start}' = 'Monday'
                    then weekstart_mon
                when  '${cad}' = 'WEEK' and '${config_week_start}' = 'Sunday'
                    then weekstart_sun
                else
                    date(date_trunc('${cad}',calendar_date))
            end as cadence_start_date,
            case
                when '${cad}' = 'WEEK' and '${config_week_start}' = 'Monday'
                    then weekend_mon
                when  '${cad}' = 'WEEK' and '${config_week_start}' = 'Sunday'
                    then weekend_sun
                when '${cad}' = 'DAY'
                    then date(date_trunc('${cad}',calendar_date))
                when '${cad}' = 'MONTH'
                    then date(
                        date_trunc(
                            'MONTH',
                            add_months(date(date_trunc('${cad}',calendar_date)), 1)
                        )
                    )-1
                when '${cad}' = 'QUARTER'
                    then date(
                        date_trunc(
                            'QUARTER',
                            add_months(date(date_trunc('${cad}',calendar_date)) , 3)
                        )
                    )-1
                when '${cad}' = 'YEAR'
                    then date(
                        date_trunc(
                            'YEAR',
                            add_months(date(date_trunc('${cad}',calendar_date)), 12)
                        )
                    )-1
            end as cadence_end_date
        """,
            "project_start": _DEFAULT_PROJECT_START,
            "project_end": _DEFAULT_PROJECT_END,
        },
    }
