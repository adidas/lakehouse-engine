"""Module containing the class definition of the Data Quality Factory."""

import importlib
import json
import random
from copy import deepcopy
from datetime import datetime, timezone
from json import dumps, loads
from typing import Optional, Tuple, Union

import great_expectations as gx
from great_expectations import ExpectationSuite
from great_expectations.checkpoint import CheckpointResult
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context import EphemeralDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
    S3StoreBackendDefaults,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    dayofmonth,
    explode,
    from_json,
    lit,
    month,
    schema_of_json,
    struct,
    to_json,
    to_timestamp,
    transform,
    year,
)
from pyspark.sql.types import FloatType, StringType

from lakehouse_engine.core.definitions import (
    DQDefaults,
    DQFunctionSpec,
    DQResultFormat,
    DQSpec,
    DQType,
    OutputSpec,
    WriteType,
)
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.core.table_manager import TableManager
from lakehouse_engine.dq_processors.exceptions import DQValidationsFailedException
from lakehouse_engine.dq_processors.validator import Validator
from lakehouse_engine.io.writer_factory import WriterFactory
from lakehouse_engine.utils.logging_handler import LoggingHandler


class DQFactory(object):
    """Class for the Data Quality Factory."""

    _LOGGER = LoggingHandler(__name__).get_logger()
    _TIMESTAMP = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    @classmethod
    def _add_critical_function_tag(cls, args: dict) -> dict:
        """Add tags to function considered critical.

        Adds a tag to each of the functions passed on the dq_specs to
        denote that they are critical_functions. This means that if any
        of them fails, the dq process will fail, even if the threshold
        is not surpassed.
        This is done by adding a tag to the meta dictionary of the
        expectation configuration.

        Args:
            args: arguments passed on the dq_spec

        Returns:
            A dictionary with the args with the critical function tag.
        """
        if "meta" in args.keys():
            meta = args["meta"]

            if isinstance(meta["notes"], str):
                meta["notes"] = meta["notes"] + " **Critical function**."
            else:
                meta["notes"]["content"] = (
                    meta["notes"]["content"] + " **Critical function**."
                )

            args["meta"] = meta
            return args

        else:
            args["meta"] = {
                "notes": {
                    "format": "markdown",
                    "content": "**Critical function**.",
                }
            }
            return args

    @classmethod
    def _configure_checkpoint(
        cls,
        context: EphemeralDataContext,
        dataframe_bd: BatchDefinition,
        suite: ExpectationSuite,
        dq_spec: DQSpec,
        data: DataFrame,
        checkpoint_run_time: str,
    ) -> Tuple[CheckpointResult, Optional[list]]:
        """Create and configure the validation checkpoint.

        Creates and configures a validation definition based on the suite
        and then creates, configures and runs the checkpoint returning,
        at the end, the result as well as the primary key from the dq_specs.

        Args:
            context: The data context from GX
            dataframe_bd: The dataframe with the batch definition to validate
            suite: A group of expectations to validate
            dq_spec: The arguments directly passed from the acon in the dq_spec key
            data: Input dataframe to run the dq process on.
            checkpoint_run_time: A string with the time in miliseconds

        Returns:
            A tuple with the result from the checkpoint run and the primary key
            from the dq_spec.
        """
        validation_definition = context.validation_definitions.add(
            gx.ValidationDefinition(
                data=dataframe_bd,
                suite=suite,
                name=f"{dq_spec.spec_id}-{dq_spec.input_id}"
                f"-validation-{checkpoint_run_time}",
            )
        )

        source_pk = cls._get_unexpected_rows_pk(dq_spec)
        result_format: dict = {
            "result_format": DQResultFormat.COMPLETE.value,
        }

        # If the source primary key is defined, we add it to the result format
        # so that it is included in the results from GX.
        if source_pk:
            result_format = {
                **result_format,
                "unexpected_index_column_names": source_pk,
            }

        checkpoint = context.checkpoints.add(
            gx.Checkpoint(
                name=f"{dq_spec.spec_id}-{dq_spec.input_id}"
                f"-checkpoint-{checkpoint_run_time}"
                f"-{str(random.randint(1, 100))}",  # nosec B311
                validation_definitions=[validation_definition],
                actions=[],
                result_format=result_format,
            )
        )

        result = checkpoint.run(
            batch_parameters={"dataframe": data},
            run_id=RunIdentifier(
                run_name=f"{checkpoint_run_time}"
                f"-{dq_spec.spec_id}-{dq_spec.input_id}"
                f"-{str(random.randint(1, 100))}-checkpoint",  # nosec B311
                run_time=datetime.strptime(checkpoint_run_time, "%Y%m%d-%H%M%S%f"),
            ),
        )

        return result, source_pk

    @classmethod
    def _check_row_condition(
        cls, dq_spec: DQSpec, dq_function: DQFunctionSpec
    ) -> DQFunctionSpec:
        """Enables/disables row_conditions.

        Checks for row_codition arguments in the definition of expectations
        and enables/disables their usage based on the enable_row_condition
        argument. row_conditions allow you to filter the rows that are
        processed by the DQ functions. This is useful when you want to run the
        DQ functions only on a subset of the data.

        Args:
            dq_spec: The arguments directly passed from the acon in the dq_spec key
            dq_function: A DQFunctionSpec with the definition of a dq function.

        Returns:
            The definition of a dq_function with or without the row_condition key.
        """
        if (
            not dq_spec.enable_row_condition
            and "row_condition" in dq_function.args.keys()
        ):
            del dq_function.args["row_condition"]
            cls._LOGGER.info(
                f"Disabling row_condition for function: {dq_function.function}"
            )
        return dq_function

    @classmethod
    def _add_suite(
        cls, context: EphemeralDataContext, dq_spec: DQSpec, checkpoint_run_time: str
    ) -> ExpectationSuite:
        """Create and configure an ExpectationSuite.

        Creates and configures an expectation suite, adding the dq functions
        passed on the dq_spec as well as the dq_critical_functions also passed
        on the dq_spec, if they exist. Finally return the configured suite.

        Args:
            context: The data context from GX
            dq_spec: The arguments directly passed from the acon in the dq_spec key
            checkpoint_run_time: A string with the time in miliseconds

        Returns:
            A configured ExpectationSuite object.
        """
        expectation_suite_name = (
            dq_spec.expectation_suite_name
            if dq_spec.expectation_suite_name
            else f"{dq_spec.spec_id}-{dq_spec.input_id}"
            f"-{dq_spec.dq_type}-{checkpoint_run_time}"
        )
        suite = context.suites.add(gx.ExpectationSuite(name=expectation_suite_name))

        for dq_function in dq_spec.dq_functions:
            dq_function = cls._check_row_condition(dq_spec, dq_function)
            suite.add_expectation_configuration(
                ExpectationConfiguration(
                    type=dq_function.function,
                    kwargs=dq_function.args if dq_function.args else {},
                    meta=dq_function.args.get("meta") if dq_function.args else {},
                )
            )
        if dq_spec.critical_functions:
            for critical_function in dq_spec.critical_functions:
                meta_args = cls._add_critical_function_tag(critical_function.args)
                suite.add_expectation_configuration(
                    ExpectationConfiguration(
                        type=critical_function.function,
                        kwargs=(
                            critical_function.args if critical_function.args else {}
                        ),
                        meta=meta_args,
                    )
                )

        suite.save()
        return suite

    @classmethod
    def _check_expectation_result(cls, result_dict: dict) -> dict:
        """Add an empty dict if the unexpected_index_list key is empty.

        Checks if the unexpected_index_list key has any element, if it doesn't,
        add an empty dictionary to the result key. This is needed due to some
        edge cases that appeared due to the GX update to version 1.3.13 where
        the unexpected_index_list would sometimes exist even for successful
        validation runs.

        Args:
            result_dict: A dict with the result_dict from a checkpoint run.

        Returns:
            The configured result_dict
        """
        for expectation_result in result_dict["results"]:
            if "unexpected_index_list" in expectation_result["result"].keys():
                if len(expectation_result["result"]["unexpected_index_list"]) < 1:
                    expectation_result["result"] = {}
        return result_dict

    @classmethod
    def run_dq_process(cls, dq_spec: DQSpec, data: DataFrame) -> DataFrame:
        """Run the specified data quality process on a dataframe.

        Based on the dq_specs we apply the defined expectations on top of the dataframe
        in order to apply the necessary validations and then output the result of
        the data quality process.

        The logic of the function is as follows:
        1. Import the custom expectations defined in the engine.
        2. Create the context based on the dq_spec. - The context is the base class for
        the GX, an ephemeral context means that it does not store/load the
        configuration of the environment in a configuration file.
        3. Add the data source to the context. - This is the data source that will be
        used to run the dq process, in our case Spark.
        4. Create the dataframe asset and batch definition. - The asset represents the
        data where the expectations are applied and the batch definition is the
        way how the data should be split, in the case of dataframes it is always
        the whole dataframe.
        5. Create the expectation suite. - This is the group of expectations that will
        be applied to the data.
        6. Create the checkpoint and run it. - The checkpoint is the object that will
        run the expectations on the data and return the results.
        7. Transform the results and write them to the result sink. - The results are
        transformed to a more readable format and then written to the result sink.
        8. Log the results and raise an exception if needed. - The results are logged
        and if there are any failed expectations the process will raise an exception
        based on the dq_spec.
        9. Tag the source data if needed. - If the dq_spec has the tag_source_data
        argument set to True, the source data will be tagged with the dq results.

        Args:
            dq_spec: data quality specification.
            data: input dataframe to run the dq process on.

        Returns:
            The DataFrame containing the results of the DQ process.
        """
        # Creating the context
        if dq_spec.dq_type == "validator" or dq_spec.dq_type == "prisma":

            for expectation in DQDefaults.CUSTOM_EXPECTATION_LIST.value:
                importlib.__import__(
                    "lakehouse_engine.dq_processors.custom_expectations." + expectation
                )

            context = gx.get_context(
                cls._get_data_context_config(dq_spec), mode="ephemeral"
            )

            # Adding data source to context
            dataframe_data_source = context.data_sources.add_spark(
                name=f"{dq_spec.spec_id}-{dq_spec.input_id}-datasource",
                persist=False,
            )
            dataframe_asset = dataframe_data_source.add_dataframe_asset(
                name=f"{dq_spec.spec_id}-{dq_spec.input_id}-asset"
            )
            dataframe_bd = dataframe_asset.add_batch_definition_whole_dataframe(
                name=f"{dq_spec.spec_id}-{dq_spec.input_id}-batch"
            )

            checkpoint_run_time = datetime.today().strftime("%Y%m%d-%H%M%S%f")

            suite = cls._add_suite(context, dq_spec, checkpoint_run_time)

            result, source_pk = cls._configure_checkpoint(
                context, dataframe_bd, suite, dq_spec, data, checkpoint_run_time
            )

            expectation_result_key = list(result.run_results.keys())[0]

            result_dict = result.run_results[expectation_result_key].to_json_dict()

            result_dict = cls._check_expectation_result(result_dict)

            data = cls._transform_checkpoint_results(
                data, source_pk, result_dict, dq_spec
            )

            # Processed keys are only added for the PRISMA dq type
            # because they are being used to calculate the good
            # records that were processed in a run.
            if dq_spec.dq_type == DQType.PRISMA.value:

                keys = data.select(
                    [col(c).cast(StringType()).alias(c) for c in source_pk]
                )
                keys = keys.withColumn(
                    "run_name", lit(result_dict["meta"]["run_id"]["run_name"])
                )

                cls._write_to_location(dq_spec, keys, processed_keys=True)

        else:
            raise TypeError(
                f"Type of Data Quality '{dq_spec.dq_type}' is not supported."
            )

        return data

    @classmethod
    def _check_critical_functions_tags(cls, failed_expectations: dict) -> list:
        critical_failure = []

        for expectation in failed_expectations.values():
            meta = expectation["meta"]
            if meta and (
                ("notes" in meta.keys() and "Critical function" in meta["notes"])
                or (
                    "content" in meta["notes"].keys()
                    and "Critical function" in meta["notes"]["content"]
                )
            ):
                critical_failure.append(expectation["type"])

        return critical_failure

    @classmethod
    def _check_chunk_usage(cls, results_dict: dict, dq_spec: DQSpec) -> bool:
        """Check if the results should be split into chunks.

        If the size of the results dictionary is too big, we will split it into
        smaller chunks. This is needed to avoid memory issues when processing
        large datasets.

        Args:
            results_dict: The results dictionary to be checked.
            dq_spec: data quality specification.

        Returns:
            True if the results dictionary is too big, False otherwise.
        """
        for ele in results_dict["results"]:
            if (
                "unexpected_index_list" in ele["result"].keys()
                and len(ele["result"]["unexpected_index_list"])
                > dq_spec.result_sink_chunk_size
            ):
                return True

        return False

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
        df = df.withColumn("validation_results", explode("results")).withColumn(
            "source", lit(dq_spec.source)
        )

        if (
            not df.schema["validation_results"]
            .dataType.fieldNames()  # type: ignore
            .__contains__("result")
        ):
            df = df.withColumn(
                "validation_results",
                col("validation_results").withField(
                    "result", struct(lit(None).alias("observed_value"))
                ),
            )

        kwargs_columns = [
            f"validation_results.expectation_config.kwargs.{col_name}"
            for col_name in df.select(
                "validation_results.expectation_config.kwargs.*"
            ).columns
        ]

        cols_to_cast = ["max_value", "min_value", "sum_total"]
        for col_name in kwargs_columns:
            if col_name.split(".")[-1] in cols_to_cast:
                df = df.withColumn(
                    "validation_results",
                    col("validation_results").withField(
                        "expectation_config",
                        col("validation_results.expectation_config").withField(
                            "kwargs",
                            col(
                                "validation_results.expectation_config.kwargs"
                            ).withField(
                                col_name.split(".")[-1],
                                col(col_name).cast(FloatType()),
                            ),
                        ),
                    ),
                )

        new_columns = [
            "validation_results.expectation_config.kwargs.*",
            "validation_results.expectation_config.type as expectation_type",
            "validation_results.success as expectation_success",
            "validation_results.exception_info",
            "statistics.*",
        ] + dq_spec.result_sink_extra_columns

        df_exploded = df.selectExpr(*df.columns, *new_columns).drop(
            *[c.replace(".*", "").split(" as")[0] for c in new_columns]
        )

        df_exploded = df_exploded.drop(
            "statistics", "id", "results", "meta", "suite_name"
        )

        if (
            "meta"
            in df_exploded.select("validation_results.expectation_config.*").columns
        ):
            df_exploded = df_exploded.withColumn(
                "meta", col("validation_results.expectation_config.meta")
            )

        schema = df_exploded.schema.simpleString()

        if (
            dq_spec.gx_result_format.upper() == DQResultFormat.COMPLETE.value
            and "unexpected_index_list" in schema
        ):
            df_exploded = df_exploded.withColumn(
                "unexpected_index_list",
                transform(
                    col("validation_results.result.unexpected_index_list"),
                    lambda y: y.withField("run_success", lit(False)),
                ),
            )

        if "observed_value" in schema:
            df_exploded = df_exploded.withColumn(
                "observed_value", col("validation_results.result.observed_value")
            )

        return (
            df_exploded.withColumn("run_time_year", year(to_timestamp("run_time")))
            .withColumn("run_time_month", month(to_timestamp("run_time")))
            .withColumn("run_time_day", dayofmonth(to_timestamp("run_time")))
            .withColumn(
                "kwargs", to_json(col("validation_results.expectation_config.kwargs"))
            )
            .withColumn("validation_results", to_json(col("validation_results")))
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

        if dq_spec.store_backend == DQDefaults.FILE_SYSTEM_STORE.value:
            store_backend = FilesystemStoreBackendDefaults(
                root_directory=dq_spec.local_fs_root_dir
            )
        elif dq_spec.store_backend == DQDefaults.FILE_SYSTEM_S3_STORE.value:
            store_backend = S3StoreBackendDefaults(
                default_bucket_name=dq_spec.bucket,
                validation_results_store_prefix=dq_spec.validations_store_prefix,
                checkpoint_store_prefix=dq_spec.checkpoint_store_prefix,
                expectations_store_prefix=dq_spec.expectations_store_prefix,
            )

        # @todo we should find a way to create a datacontextconfig without
        # passing a local_fs_root_dir so that we wont have problems with
        # changing versions of the lakehouse-engine due to the marshmallow
        # library identifiyng new fields on the checkpoints

        return DataContextConfig(
            store_backend_defaults=store_backend,
            analytics_enabled=False,
        )

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
                "persist": False,
            },
            "data_connectors": {
                f"{dq_spec.spec_id}-{dq_spec.input_id}-data_connector": {
                    "module_name": DQDefaults.DATA_CONNECTORS_MODULE_NAME.value,
                    "class_name": DQDefaults.DATA_CONNECTORS_CLASS_NAME.value,
                    "assets": {
                        (
                            dq_spec.data_asset_name
                            if dq_spec.data_asset_name
                            else f"{dq_spec.spec_id}-{dq_spec.input_id}"
                        ): {"batch_identifiers": DQDefaults.DQ_BATCH_IDENTIFIERS.value}
                    },
                }
            },
        }

    @classmethod
    def _get_failed_expectations(
        cls,
        results: dict,
        dq_spec: DQSpec,
        failed_expectations: dict,
        evaluated_expectations: dict,
        is_final_chunk: bool,
    ) -> Tuple[dict, dict]:
        """Get the failed expectations of a Checkpoint result.

        Args:
            results: the results of the DQ process.
            dq_spec: data quality specification.
            failed_expectations: dict of failed expectations.
            evaluated_expectations: dict of evaluated expectations.
            is_final_chunk: boolean indicating if this is the final chunk.

        Returns: a tuple with a dict of failed expectations
                and a dict of evaluated expectations.
        """
        expectations_results = results["results"]
        for result in expectations_results:
            evaluated_expectations[result["expectation_config"]["id"]] = result[
                "expectation_config"
            ]
            if not result["success"]:
                failed_expectations[result["expectation_config"]["id"]] = result[
                    "expectation_config"
                ]
                if result["exception_info"]["raised_exception"]:
                    cls._LOGGER.error(
                        f"""The expectation {str(result["expectation_config"])}
                        raised the following exception:
                        {result["exception_info"]["exception_message"]}"""
                    )
        cls._LOGGER.error(
            f"{len(failed_expectations)} out of {len(evaluated_expectations)} "
            f"Data Quality Expectation(s) have failed! Failed Expectations: "
            f"{failed_expectations}"
        )

        percentage_failure = 1 - (results["statistics"]["success_percent"] / 100)

        if (
            dq_spec.max_percentage_failure is not None
            and dq_spec.max_percentage_failure < percentage_failure
            and is_final_chunk
        ):
            raise DQValidationsFailedException(
                f"Max error threshold is being surpassed! "
                f"Expected: {dq_spec.max_percentage_failure} "
                f"Got: {percentage_failure}"
            )

        return failed_expectations, evaluated_expectations

    @classmethod
    def _get_unexpected_rows_pk(cls, dq_spec: DQSpec) -> Optional[list]:
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
    def _log_or_fail(
        cls,
        results: dict,
        dq_spec: DQSpec,
        failed_expectations: dict,
        evaluated_expectations: dict,
        is_final_chunk: bool,
    ) -> Tuple[dict, dict]:
        """Log the execution of the Data Quality process.

        Args:
            results: the results of the DQ process.
            dq_spec: data quality specification.
            failed_expectations: list of failed expectations.
            evaluated_expectations: list of evaluated expectations.
            is_final_chunk: boolean indicating if this is the final chunk.

        Returns: a tuple with a dict of failed expectations
                and a dict of evaluated expectations.
        """
        if results["success"]:
            cls._LOGGER.info(
                "The data passed all the expectations defined. Everything looks good!"
            )
        else:
            failed_expectations, evaluated_expectations = cls._get_failed_expectations(
                results,
                dq_spec,
                failed_expectations,
                evaluated_expectations,
                is_final_chunk,
            )

        if dq_spec.critical_functions and is_final_chunk:
            critical_failure = cls._check_critical_functions_tags(failed_expectations)

            if critical_failure:
                raise DQValidationsFailedException(
                    f"Data Quality Validations Failed, the following critical "
                    f"expectations failed: {critical_failure}."
                )
        if dq_spec.fail_on_error and is_final_chunk and failed_expectations:
            raise DQValidationsFailedException("Data Quality Validations Failed!")

        return failed_expectations, evaluated_expectations

    @classmethod
    def _transform_checkpoint_results(
        cls,
        data: DataFrame,
        source_pk: list,
        checkpoint_results: dict,
        dq_spec: DQSpec,
    ) -> DataFrame:
        """Transforms the checkpoint results and creates new entries.

        All the items of the dictionary are cast to a json like format.
        All columns are cast to json like format.
        After that the dictionary is converted into a dataframe.

        Args:
            data: input dataframe to run the dq process on.
            source_pk: list of columns that are part of the primary key.
            checkpoint_results: dict with results of the checkpoint run.
            dq_spec: data quality specification.
            checkpoint_run_time: A string with the time in miliseconds.

        Returns:
            Transformed results dataframe.
        """
        results_dict = loads(dumps(checkpoint_results))

        # Check the size of the results dictionary, if it is too big
        # we will split it into smaller chunks.
        results_dict_list = cls._generate_chunks(results_dict, dq_spec)

        index = 0

        failed_expectations: dict = {}
        evaluated_expectations: dict = {}

        # The processed chunk is removed from the list of results
        # so the memory is freed as soon as possible.
        while index < len(results_dict_list):
            is_final_chunk = len(results_dict_list) == 1
            data, failed_expectations, evaluated_expectations = cls._process_chunk(
                dq_spec,
                source_pk,
                results_dict_list[index],
                data,
                failed_expectations,
                evaluated_expectations,
                is_final_chunk,
            )
            del results_dict_list[index]

        return data

    @classmethod
    def _process_chunk(
        cls,
        dq_spec: DQSpec,
        source_pk: list[str],
        ele: dict,
        data: DataFrame,
        failed_expectations: dict,
        evaluated_expectations: dict,
        is_final_chunk: bool,
    ) -> Tuple[DataFrame, dict, dict]:
        """Process a chunk of the results.

        Args:
            dq_spec: data quality specification.
            source_pk: list of columns that are part of the primary key.
            ele: dictionary with the results of the dq process.
            data: input dataframe to run the dq process on.
            failed_expectations: list of failed expectations.
            evaluated_expectations: list of evaluated expectations.
            is_final_chunk: boolean indicating if this is the final chunk.

        Returns:
            A tuple with the processed data, failed expectations and evaluated
            expectations.
        """
        df = ExecEnv.SESSION.createDataFrame([json.dumps(ele)], schema=StringType())
        schema = schema_of_json(lit(json.dumps(ele)))
        df = (
            df.withColumn("value", from_json("value", schema))
            .select("value.*")
            .withColumn("spec_id", lit(dq_spec.spec_id))
            .withColumn("input_id", lit(dq_spec.input_id))
            .withColumn("run_name", col("meta.run_id.run_name"))
            .withColumn("run_time", col("meta.run_id.run_time"))
        )
        exploded_df = (
            cls._explode_results(df, dq_spec)
            if dq_spec.result_sink_explode
            else df.withColumn("validation_results", to_json(col("results"))).drop(
                "statistics", "meta", "suite_name", "results", "id"
            )
        )

        exploded_df = exploded_df.withColumn("source_primary_key", lit(source_pk))

        exploded_df = cls._cast_columns_to_string(exploded_df)

        cls._write_to_location(dq_spec, exploded_df)

        failed_expectations, evaluated_expectations = cls._log_or_fail(
            ele, dq_spec, failed_expectations, evaluated_expectations, is_final_chunk
        )
        if (
            dq_spec.tag_source_data
            and dq_spec.result_sink_explode
            and dq_spec.fail_on_error is not True
        ):
            data = Validator.tag_source_with_dq(source_pk, data, exploded_df)
            return data, failed_expectations, evaluated_expectations
        return data, failed_expectations, evaluated_expectations

    @classmethod
    def _cast_columns_to_string(cls, df: DataFrame) -> DataFrame:
        """Cast selected columns of the dataframe to string type.

        Args:
            df: The input dataframe.

        Returns:
            A new dataframe with selected columns cast to string type.
        """
        for col_name in df.columns:
            if col_name not in DQDefaults.DQ_COLUMNS_TO_KEEP_TYPES.value:
                df = df.withColumn(col_name, df[col_name].cast(StringType()))
        return df

    @classmethod
    def _generate_chunks(cls, results_dict: dict, dq_spec: DQSpec) -> list:
        """Split the results dictionary into smaller chunks.

        This is needed to avoid memory issues when processing large datasets.
        The size of the chunks is defined by the dq_spec.result_sink_chunk_size.

        Args:
            results_dict: The results dictionary to be split.
            dq_spec: data quality specification.

        Returns:
            A list of dictionaries, where each dictionary is a chunk of the original
            results dictionary.
        """
        results_dict_list = []

        split = cls._check_chunk_usage(results_dict, dq_spec)

        if split:
            # Here we are splitting the results into chunks per expectation
            # and then we are splitting the unexpected_index_list into
            # chunks of size dq_spec.result_sink_chunk_size.
            results_dict_list = cls._split_into_chunks(results_dict, dq_spec)
        else:
            # If the results are not too big, we can process them all at once.
            results_dict_list = [results_dict]

        return results_dict_list

    @classmethod
    def _split_into_chunks(cls, results_dict: dict, dq_spec: DQSpec) -> list:
        """Split the results into smaller chunks.

        This is needed to avoid memory issues when processing large datasets.
        The size of the chunks is defined by the dq_spec.result_sink_chunk_size.

        Args:
            results: The results to be split.
            dq_spec: data quality specification.

        Returns:
            A list of dictionaries, where each dictionary is a chunk of the original
            results.
        """
        results_dict_list = []

        for ele in results_dict["results"]:
            base_result = deepcopy(results_dict)

            if "unexpected_index_list" in ele["result"].keys():
                for key in ExecEnv.ENGINE_CONFIG.dq_result_sink_columns_to_delete:
                    del ele["result"][key]

                unexpected_index_list = ele["result"]["unexpected_index_list"]
                unexpected_index_list_chunks = cls.split_into_chunks(
                    unexpected_index_list, dq_spec.result_sink_chunk_size
                )

                del ele["result"]["unexpected_index_list"]

                for chunk in unexpected_index_list_chunks:
                    ele["result"]["unexpected_index_list"] = chunk
                    base_result["results"] = [ele]
                    results_dict_list.append(deepcopy(base_result))
            else:
                base_result["results"] = [ele]
                results_dict_list.append(base_result)

        return results_dict_list

    @classmethod
    def _write_to_location(
        cls,
        dq_spec: DQSpec,
        df: DataFrame,
        processed_keys: bool = False,
    ) -> None:
        """Write dq results dataframe to a table or location.

        It can be written:
        - a raw output (having result_sink_explode set as False)
        - an exploded output (having result_sink_explode set as True), which
        is more prepared for analysis, with some columns exploded, flatten and
        transformed. It can also be set result_sink_extra_columns with other
        columns desired to have in the output table or location.
        - processed keys when running the dq process with the dq_type set as
        'prisma'.

        Args:
            dq_spec: data quality specification.
            df: dataframe with dq results to write.
            processed_keys: boolean indicating if the dataframe contains
                the processed keys.
        """
        if processed_keys:
            table = None
            location = dq_spec.processed_keys_location
            options = {"mergeSchema": "true"}
        else:
            table = dq_spec.result_sink_db_table
            location = dq_spec.result_sink_location
            options = {"mergeSchema": "true"} if dq_spec.result_sink_explode else {}

        if table or location:
            WriterFactory.get_writer(
                spec=OutputSpec(
                    spec_id="dq_result_sink",
                    input_id="dq_result",
                    db_table=table,
                    location=location,
                    partitions=(
                        dq_spec.result_sink_partitions
                        if dq_spec.result_sink_partitions
                        else []
                    ),
                    write_type=WriteType.APPEND.value,
                    data_format=dq_spec.result_sink_format,
                    options=(
                        options
                        if dq_spec.result_sink_options is None
                        else {**dq_spec.result_sink_options, **options}
                    ),
                ),
                df=df,
                data=None,
            ).write()

    @staticmethod
    def split_into_chunks(lst: list, chunk_size: int) -> list:
        """Split a list into chunks of a specified size.

        Args:
            lst: The list to be split.
            chunk_size: Number of records in each chunk.

        Returns:
            A list of lists, where each inner list is a chunk of the original list.
        """
        if chunk_size <= 0:
            raise ValueError("Chunk size must be a positive integer.")
        chunk_list = []
        for i in range(0, len(lst), chunk_size):
            chunk_list.append(lst[i : i + chunk_size])
        return chunk_list
