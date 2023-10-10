"""Module containing the class definition of the Data Quality Factory."""
import importlib.util
from datetime import datetime, timezone
from json import dumps, loads
from typing import Any, Dict, List, Optional, OrderedDict, Tuple, Union

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    AnonymizedUsageStatisticsConfig,
    DataContextConfig,
    FilesystemStoreBackendDefaults,
    S3StoreBackendDefaults,
)
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array,
    col,
    dayofmonth,
    explode,
    lit,
    month,
    struct,
    to_json,
    to_timestamp,
    transform,
    year,
)

from lakehouse_engine.core.definitions import (
    DQDefaults,
    DQSpec,
    DQType,
    OutputSpec,
    WriteType,
)
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.core.table_manager import TableManager
from lakehouse_engine.dq_processors.assistant import Assistant
from lakehouse_engine.dq_processors.exceptions import (
    DQCheckpointsResultsException,
    DQValidationsFailedException,
)
from lakehouse_engine.dq_processors.validator import Validator
from lakehouse_engine.io.writer_factory import WriterFactory
from lakehouse_engine.utils.logging_handler import LoggingHandler


class DQFactory(object):
    """Class for the Data Quality Factory."""

    _LOGGER = LoggingHandler(__name__).get_logger()
    _TIMESTAMP = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    @classmethod
    def run_dq_process(cls, dq_spec: DQSpec, data: DataFrame) -> DataFrame:
        """Run the specified data quality process on a dataframe.

        Based on the dq_specs we apply the defined expectations on top of the dataframe
        in order to apply the necessary validations and then output the result of
        the data quality process.

        Args:
            dq_spec: data quality specification.
            data: input dataframe to run the dq process on.

        Returns:
            The DataFrame containing the results of the DQ process.
        """
        # import custom expectations for them to be available to be used.
        for expectation in DQDefaults.CUSTOM_EXPECTATION_LIST.value:
            importlib.__import__(
                "lakehouse_engine.dq_processors.custom_expectations." + expectation
            )

        context = BaseDataContext(project_config=cls._get_data_context_config(dq_spec))
        context.add_datasource(**cls._get_data_source_defaults(dq_spec))

        expectation_suite_name = (
            dq_spec.expectation_suite_name
            if dq_spec.expectation_suite_name
            else f"{dq_spec.spec_id}-{dq_spec.input_id}-{dq_spec.dq_type}"
        )
        context.add_or_update_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )

        batch_request = cls._get_batch_request(dq_spec, data)

        # We only run the checkpoint for the Validator, as we don't want to allow
        # running this for all the expectations suggested by the Assistant.
        if dq_spec.dq_type == DQType.VALIDATOR.value:
            Validator.get_dq_validator(
                context,
                batch_request,
                expectation_suite_name,
                dq_spec.dq_functions,
                dq_spec.critical_functions,
            )

            source_pk = cls._get_unexpected_rows_pk(dq_spec)
            results, results_df = cls._configure_and_run_checkpoint(
                dq_spec, context, batch_request, expectation_suite_name, source_pk
            )

            cls._write_to_result_sink(dq_spec, results_df)

            cls._log_or_fail(results, dq_spec)

            if (
                dq_spec.tag_source_data
                and dq_spec.result_sink_explode
                and dq_spec.fail_on_error is not True
            ):
                data = Validator.tag_source_with_dq(source_pk, data, results_df)
        elif dq_spec.dq_type == DQType.ASSISTANT.value:
            Assistant.run_data_assistant(
                context,
                batch_request,
                expectation_suite_name,
                dq_spec.assistant_options or {},
                data,
                f"{cls._TIMESTAMP}_{dq_spec.spec_id}_{dq_spec.input_id}_profile",
            )
        else:
            raise TypeError(
                f"Type of Data Quality '{dq_spec.dq_type}' is not supported."
            )

        return data

    @classmethod
    def _check_critical_functions_tags(cls, failed_expectations: List[Any]) -> list:
        critical_failure = []

        for expectation in failed_expectations:
            meta = expectation["meta"]
            if meta and (
                ("notes" in meta.keys() and "Critical function" in meta["notes"])
                or (
                    "content" in meta["notes"].keys()
                    and "Critical function" in meta["notes"]["content"]
                )
            ):
                critical_failure.append(expectation["expectation_type"])

        return critical_failure

    @classmethod
    def _configure_and_run_checkpoint(
        cls,
        dq_spec: DQSpec,
        context: BaseDataContext,
        batch_request: RuntimeBatchRequest,
        expectation_suite_name: str,
        source_pk: List[str],
    ) -> Tuple[CheckpointResult, DataFrame]:
        """Configure, run and return checkpoint results.

        A checkpoint is what enables us to run the validations of the expectations'
        suite on the batches of data.

        Args:
            dq_spec: data quality specification.
            context: the BaseDataContext containing the configurations for the data
                source and store backend.
            batch_request: run time batch request to be able to query underlying data.
            expectation_suite_name: name of the expectation suite.
            source_pk: the primary key of the source data.

        Returns:
            The checkpoint results in two types: CheckpointResult and Dataframe.
        """
        checkpoint_name = f"{dq_spec.spec_id}-{dq_spec.input_id}-checkpoint"
        context.add_or_update_checkpoint(
            name=checkpoint_name,
            class_name=DQDefaults.DATA_CHECKPOINTS_CLASS_NAME.value,
            config_version=DQDefaults.DATA_CHECKPOINTS_CONFIG_VERSION.value,
            run_name_template=f"%Y%m%d-%H%M%S-{checkpoint_name}",
        )

        result_format: Dict[str, Any] = {
            "result_format": dq_spec.gx_result_format,
        }
        if source_pk:
            result_format = {
                **result_format,
                "unexpected_index_column_names": source_pk,
            }

        results = context.run_checkpoint(
            checkpoint_name=checkpoint_name,
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": expectation_suite_name,
                }
            ],
            result_format=result_format,
        )

        return results, cls._transform_checkpoint_results(
            results.to_json_dict(), dq_spec
        )

    @classmethod
    def _explode_results(
        cls,
        df: DataFrame,
        dq_spec: DQSpec,
    ) -> DataFrame:
        """Transform dq results dataframe exploding a set of columns.

        Args:
            df: dataframe with dq results to be exploded.
            dq_spec: data quality specification.
        """
        df = df.withColumn(
            "validation_results", explode("run_results.validation_result.results")
        ).withColumn("source", lit(dq_spec.source))

        new_columns = [
            "validation_results.expectation_config.kwargs.*",
            "run_results.validation_result.statistics.*",
            "validation_results.expectation_config.expectation_type",
            "validation_results.success as expectation_success",
            "validation_results.exception_info",
        ] + dq_spec.result_sink_extra_columns

        df_exploded = df.selectExpr(*df.columns, *new_columns).drop(
            *[c.replace(".*", "").split(" as")[0] for c in new_columns]
        )

        schema = df_exploded.schema.simpleString()
        if "unexpected_index_list" in schema:
            df_exploded = (
                df_exploded.withColumn(
                    "unexpected_index_list",
                    array(struct(lit(True).alias("run_success"))),
                )
                if df.select(
                    col("validation_results.result.unexpected_index_list")
                ).dtypes[0][1]
                == "array<string>"
                else df_exploded.withColumn(
                    "unexpected_index_list",
                    transform(
                        col("validation_results.result.unexpected_index_list"),
                        lambda x: x.withField("run_success", lit(False)),
                    ),
                )
            )

        if "observed_value" in schema:
            df_exploded = df_exploded.withColumn(
                "observed_value", col("validation_results.result.observed_value")
            )

        return (
            df_exploded.withColumn("run_time_year", year(to_timestamp("run_time")))
            .withColumn("run_time_month", month(to_timestamp("run_time")))
            .withColumn("run_time_day", dayofmonth(to_timestamp("run_time")))
            .withColumn("checkpoint_config", to_json(col("checkpoint_config")))
            .withColumn("run_results", to_json(col("run_results")))
            .withColumn(
                "kwargs", to_json(col("validation_results.expectation_config.kwargs"))
            )
            .withColumn("validation_results", to_json(col("validation_results")))
        )

    @classmethod
    def _get_batch_request(
        cls, dq_spec: DQSpec, data: DataFrame
    ) -> RuntimeBatchRequest:
        """Get run time batch request to be able to query underlying data.

        Args:
            dq_spec: data quality process specification.
            data: input dataframe to run the dq process on.

        Returns:
            The RuntimeBatchRequest object configuration.
        """
        return RuntimeBatchRequest(
            datasource_name=f"{dq_spec.spec_id}-{dq_spec.input_id}-datasource",
            data_connector_name=f"{dq_spec.spec_id}-{dq_spec.input_id}-data_connector",
            data_asset_name=dq_spec.data_asset_name
            if dq_spec.data_asset_name
            else f"{dq_spec.spec_id}-{dq_spec.input_id}",
            batch_identifiers={
                "spec_id": dq_spec.spec_id,
                "input_id": dq_spec.input_id,
                "timestamp": cls._TIMESTAMP,
            },
            runtime_parameters={"batch_data": data},
        )

    @classmethod
    def _get_data_context_config(cls, dq_spec: DQSpec) -> DataContextConfig:
        """Get the configuration of the data context.

        Based on the configuration it is possible to define the backend to be
        the file system (e.g. local file system) or S3, meaning that the DQ artefacts
        will be stored according to this configuration.

        Args:
            dq_spec: data quality process specification.

        Returns:
            The DataContextConfig object configuration.
        """
        store_backend: Union[FilesystemStoreBackendDefaults, S3StoreBackendDefaults]
        data_docs_site = None

        if dq_spec.store_backend == DQDefaults.FILE_SYSTEM_STORE.value:
            store_backend = FilesystemStoreBackendDefaults(
                root_directory=dq_spec.local_fs_root_dir
            )
            data_docs_site = cls._get_data_docs_sites(
                "local_site", store_backend.data_docs_sites, dq_spec
            )
        elif dq_spec.store_backend == DQDefaults.FILE_SYSTEM_S3_STORE.value:
            store_backend = S3StoreBackendDefaults(
                default_bucket_name=dq_spec.bucket,
                validations_store_prefix=dq_spec.validations_store_prefix,
                checkpoint_store_prefix=dq_spec.checkpoint_store_prefix,
                expectations_store_prefix=dq_spec.expectations_store_prefix,
                data_docs_prefix=dq_spec.data_docs_prefix,
                data_docs_bucket_name=dq_spec.data_docs_bucket
                if dq_spec.data_docs_bucket
                else dq_spec.bucket,
            )
            data_docs_site = cls._get_data_docs_sites(
                "s3_site", store_backend.data_docs_sites, dq_spec
            )

        return DataContextConfig(
            store_backend_defaults=store_backend,
            data_docs_sites=data_docs_site,
            anonymous_usage_statistics=AnonymizedUsageStatisticsConfig(enabled=False),
        )

    @classmethod
    def _get_data_docs_sites(
        cls, site_name: str, data_docs_site: dict, dq_spec: DQSpec
    ) -> dict:
        """Get the custom configuration of the data_docs_sites.

        Args:
            site_name: the name to give to the site.
            data_docs_site: the default configuration for the data_docs_site.
            dq_spec: data quality specification.

        Returns:
            Modified data_docs_site.
        """
        data_docs_site[site_name]["show_how_to_buttons"] = False

        if site_name == "local_site":
            data_docs_site[site_name]["store_backend"][
                "base_directory"
            ] = dq_spec.data_docs_prefix

        return data_docs_site

    @classmethod
    def _get_data_source_defaults(cls, dq_spec: DQSpec) -> dict:
        """Get the configuration for a datasource.

        Args:
            dq_spec: data quality specification.

        Returns:
            The python dictionary with the datasource configuration.
        """
        return {
            "name": f"{dq_spec.spec_id}-{dq_spec.input_id}-datasource",
            "class_name": DQDefaults.DATASOURCE_CLASS_NAME.value,
            "execution_engine": {
                "class_name": DQDefaults.DATASOURCE_EXECUTION_ENGINE.value,
                "force_reuse_spark_context": True,
                "persist": False,
            },
            "data_connectors": {
                f"{dq_spec.spec_id}-{dq_spec.input_id}-data_connector": {
                    "module_name": DQDefaults.DATA_CONNECTORS_MODULE_NAME.value,
                    "class_name": DQDefaults.DATA_CONNECTORS_CLASS_NAME.value,
                    "assets": {
                        dq_spec.data_asset_name
                        if dq_spec.data_asset_name
                        else f"{dq_spec.spec_id}-{dq_spec.input_id}": {
                            "batch_identifiers": DQDefaults.DQ_BATCH_IDENTIFIERS.value
                        }
                    },
                }
            },
        }

    @classmethod
    def _get_failed_expectations(
        cls, results: CheckpointResult, dq_spec: DQSpec
    ) -> List[Any]:
        """Get the failed expectations of a Checkpoint result.

        Args:
            results: the results of the DQ process.
            dq_spec: data quality specification.

        Returns: a list of failed expectations.
        """
        failed_expectations = []
        for validation_result in results.list_validation_results():
            expectations_results = validation_result["results"]
            for result in expectations_results:
                if not result["success"]:
                    failed_expectations.append(result["expectation_config"])
                    if result["exception_info"]["raised_exception"]:
                        cls._LOGGER.error(
                            f"""The expectation {str(result["expectation_config"])}
                            raised the following exception:
                            {result["exception_info"]["exception_message"]}"""
                        )
            cls._LOGGER.error(
                f"{len(failed_expectations)} out of {len(expectations_results)} "
                f"Data Quality Expectation(s) have failed! Failed Expectations: "
                f"{failed_expectations}"
            )

            percentage_failure = 1 - (
                validation_result["statistics"]["success_percent"] / 100
            )

            if (
                dq_spec.max_percentage_failure
                and dq_spec.max_percentage_failure < percentage_failure
            ):
                raise DQValidationsFailedException(
                    f"Max error threshold is being surpassed! "
                    f"Expected: {dq_spec.max_percentage_failure} "
                    f"Got: {percentage_failure}"
                )
        return failed_expectations

    @classmethod
    def _get_unexpected_rows_pk(cls, dq_spec: DQSpec) -> Optional[List[str]]:
        """Get primary key for using on rows failing DQ validations.

        Args:
            dq_spec: data quality specification.

        Returns: the list of columns that are part of the primary key.
        """
        if dq_spec.unexpected_rows_pk:
            return dq_spec.unexpected_rows_pk
        elif dq_spec.tbl_to_derive_pk:
            return TableManager(
                {"function": "get_tbl_pk", "table_or_view": dq_spec.tbl_to_derive_pk}
            ).get_tbl_pk()
        elif dq_spec.tag_source_data:
            raise ValueError(
                "You need to provide either the argument "
                "'unexpected_rows_pk' or 'tbl_to_derive_pk'."
            )
        else:
            return None

    @classmethod
    def _log_or_fail(cls, results: CheckpointResult, dq_spec: DQSpec) -> None:
        """Log the execution of the Data Quality process.

        Args:
            results: the results of the DQ process.
            dq_spec: data quality specification.
        """
        if results["success"]:
            cls._LOGGER.info(
                "The data passed all the expectations defined. Everything looks good!"
            )
        else:
            failed_expectations = cls._get_failed_expectations(results, dq_spec)
            if dq_spec.critical_functions:
                critical_failure = cls._check_critical_functions_tags(
                    failed_expectations
                )

                if critical_failure:
                    raise DQValidationsFailedException(
                        f"Data Quality Validations Failed, the following critical "
                        f"expectations failed: {critical_failure}."
                    )
            elif dq_spec.fail_on_error:
                raise DQValidationsFailedException("Data Quality Validations Failed!")

    @classmethod
    def _transform_checkpoint_results(
        cls, checkpoint_results: dict, dq_spec: DQSpec
    ) -> DataFrame:
        """Transforms the checkpoint results and creates new entries.

        All the items of the dictionary are cast to a json like format.
        The validation_result_identifier is extracted from the run_results column
        into a separated column. All columns are cast to json like format.
        After that the dictionary is converted into a dataframe.

        Args:
            checkpoint_results: dict with results of the checkpoint run.
            dq_spec: data quality specification.

        Returns:
            Transformed results dataframe.
        """
        results_json_dict = loads(dumps(checkpoint_results))

        results_dict = {}
        for key, value in results_json_dict.items():
            if key == "run_results":
                checkpoint_result_identifier = list(value.keys())[0]
                # check if the grabbed identifier is correct
                if (
                    str(checkpoint_result_identifier)
                    .lower()
                    .startswith(DQDefaults.VALIDATION_COLUMN_IDENTIFIER.value)
                ):
                    results_dict[
                        "validation_result_identifier"
                    ] = checkpoint_result_identifier
                    results_dict["run_results"] = value[checkpoint_result_identifier]
                else:
                    raise DQCheckpointsResultsException(
                        "The checkpoint result identifier format is not "
                        "in accordance to what is expected"
                    )
            else:
                results_dict[key] = value

        rdd = ExecEnv.SESSION.sparkContext.parallelize([dumps(results_dict)])
        df = ExecEnv.SESSION.read.json(rdd)

        cols_to_expand = ["run_id"]
        df = (
            df.select(
                [
                    col(c) if c not in cols_to_expand else col(f"{c}.*")
                    for c in df.columns
                ]
            )
            .drop(*cols_to_expand)
            .withColumn("spec_id", lit(dq_spec.spec_id))
            .withColumn("input_id", lit(dq_spec.input_id))
        )

        return (
            cls._explode_results(df, dq_spec)
            if dq_spec.result_sink_explode
            else df.withColumn(
                "checkpoint_config", to_json(col("checkpoint_config"))
            ).withColumn("run_results", to_json(col("run_results")))
        )

    @classmethod
    def _write_to_result_sink(
        cls,
        dq_spec: DQSpec,
        df: DataFrame,
        data: OrderedDict = None,
    ) -> None:
        """Write dq results dataframe to a table or location.

        It can be written:
        - a raw output (having result_sink_explode set as False)
        - an exploded output (having result_sink_explode set as True), which
        is more prepared for analysis, with some columns exploded, flatten and
        transformed. It can also be set result_sink_extra_columns with other
        columns desired to have in the output table or location.

        Args:
            dq_spec: data quality specification.
            df: dataframe with dq results to write.
            data: list of all dfs generated on previous steps before writer.
        """
        if dq_spec.result_sink_db_table or dq_spec.result_sink_location:
            options = {"mergeSchema": "true"} if dq_spec.result_sink_explode else {}

            WriterFactory.get_writer(
                spec=OutputSpec(
                    spec_id="dq_result_sink",
                    input_id="dq_result",
                    db_table=dq_spec.result_sink_db_table,
                    location=dq_spec.result_sink_location,
                    partitions=dq_spec.result_sink_partitions
                    if dq_spec.result_sink_partitions
                    else [],
                    write_type=WriteType.APPEND.value,
                    data_format=dq_spec.result_sink_format,
                    options=options
                    if dq_spec.result_sink_options is None
                    else {**dq_spec.result_sink_options, **options},
                ),
                df=df,
                data=data,
            ).write()
