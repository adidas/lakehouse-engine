"""Module containing the definition of a data quality validator."""
from typing import Any, List

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, collect_set, explode, first, lit, struct, when

from lakehouse_engine.core.definitions import DQDefaults, DQFunctionSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.logging_handler import LoggingHandler


class Validator(object):
    """Class containing the data quality validator."""

    _LOGGER = LoggingHandler(__name__).get_logger()

    @classmethod
    def get_dq_validator(
        cls,
        context: BaseDataContext,
        batch_request: RuntimeBatchRequest,
        expectation_suite_name: str,
        dq_functions: List[DQFunctionSpec],
        critical_functions: List[DQFunctionSpec],
    ) -> Any:
        """Get a validator according to the specification.

        We use getattr to dynamically execute any expectation available.
        getattr(validator, function) is similar to validator.function(). With this
        approach, we can execute any expectation supported.

        Args:
            context: the BaseDataContext containing the configurations for the data
            source and store backend.
            batch_request: run time batch request to be able to query underlying data.
            expectation_suite_name: name of the expectation suite.
            dq_functions: a list of DQFunctionSpec to consider in the expectation suite.
            critical_functions: list of critical expectations in the expectation suite.

        Returns:
            The validator with the expectation suite stored.
        """
        validator = context.get_validator(
            batch_request=batch_request, expectation_suite_name=expectation_suite_name
        )
        if dq_functions:
            for dq_function in dq_functions:
                getattr(validator, dq_function.function)(
                    **dq_function.args if dq_function.args else {}
                )

        if critical_functions:
            for critical_function in critical_functions:
                meta_args = cls._add_critical_function_tag(critical_function.args)

                getattr(validator, critical_function.function)(**meta_args)

        return validator.save_expectation_suite(discard_failed_expectations=False)

    @classmethod
    def tag_source_with_dq(
        cls, source_pk: List[str], source_df: DataFrame, results_df: DataFrame
    ) -> DataFrame:
        """Tags the source dataframe with a new column having the DQ results.

        Args:
            source_pk: the primary key of the source data.
            source_df: the source dataframe to be tagged with DQ results.
            results_df: dq results dataframe.

        Returns: a dataframe tagged with the DQ results.
        """
        run_success = True
        run_name = results_df.select("run_name").first()[0]
        raised_exceptions = (
            True
            if results_df.filter("exception_info.raised_exception == True").count() > 0
            else False
        )

        failures_df = (
            results_df.filter(
                "expectation_success == False and size(unexpected_index_list) > 0"
            )
            if "unexpected_index_list" in results_df.schema.simpleString()
            else results_df.filter("expectation_success == False")
        )

        if failures_df.rdd.isEmpty() is not True:
            run_success = False

            source_df = cls._get_row_tagged_fail_df(
                failures_df, raised_exceptions, source_df, source_pk
            )

        return cls._join_complementary_data(
            run_name, run_success, raised_exceptions, source_df
        )

    @classmethod
    def _add_critical_function_tag(cls, args: dict) -> dict:
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

    @staticmethod
    def _get_row_tagged_fail_df(
        failures_df: DataFrame,
        raised_exceptions: bool,
        source_df: DataFrame,
        source_pk: List[str],
    ) -> DataFrame:
        """Get the source_df DataFrame tagged with the row level failures.

        Args:
            failures_df: dataframe having all failed expectations from the DQ execution.
            raised_exceptions: whether there was at least one expectation raising
            exceptions (True) or not (False).
            source_df: the source dataframe being tagged with DQ results.
            source_pk: the primary key of the source data.

        Returns: the source_df tagged with the row level failures.
        """
        if "unexpected_index_list" in failures_df.schema.simpleString():
            row_failures_df = (
                failures_df.alias("a")
                .withColumn("exploded_list", explode(col("unexpected_index_list")))
                .selectExpr("a.*", "exploded_list.*")
                .groupBy(*source_pk)
                .agg(
                    struct(
                        first(col("run_name")).alias("run_name"),
                        first(col("success")).alias("run_success"),
                        lit(raised_exceptions).alias("raised_exceptions"),
                        first(col("expectation_success")).alias("run_row_success"),
                        collect_set(
                            struct(
                                col("expectation_type"),
                                col("kwargs"),
                            )
                        ).alias("dq_failure_details"),
                    ).alias("dq_validations")
                )
            )

            if all(item in row_failures_df.columns for item in source_pk):
                join_cond = [
                    col(f"a.{key}").eqNullSafe(col(f"b.{key}")) for key in source_pk
                ]
                source_df = (
                    source_df.alias("a")
                    .join(row_failures_df.alias("b"), join_cond, "left")
                    .select("a.*", "b.dq_validations")
                )

        return source_df

    @staticmethod
    def _join_complementary_data(
        run_name: str, run_success: bool, raised_exceptions: bool, source_df: DataFrame
    ) -> DataFrame:
        """Join the source_df DataFrame with complementary data.

        The source_df was already tagged/joined with the row level DQ failures, in case
        there were any. However, there might be cases for which we don't have any
        failure (everything succeeded) or cases for which only not row level failures
        happened (e.g. table level expectations or column level aggregations), and, for
        those we need to join the source_df with complementary data.

        Args:
            run_name: the name of the DQ execution in great expectations.
            run_success: whether the general execution of the DQ was succeeded (True)
            or not (False).
            raised_exceptions: whether there was at least one expectation raising
            exceptions (True) or not (False).
            source_df: the source dataframe being tagged with DQ results.

        Returns: the source_df tagged with complementary data.
        """
        complementary_data = [
            {
                "dq_validations": {
                    "run_name": run_name,
                    "run_success": run_success,
                    "raised_exceptions": raised_exceptions,
                    "run_row_success": True,
                }
            }
        ]
        complementary_df = ExecEnv.SESSION.sparkContext.parallelize(
            complementary_data
        ).toDF(schema=DQDefaults.DQ_VALIDATIONS_SCHEMA.value)

        return (
            source_df.crossJoin(
                complementary_df.withColumnRenamed(
                    "dq_validations", "tmp_dq_validations"
                )
            )
            .withColumn(
                "dq_validations",
                when(
                    col("dq_validations").isNotNull(), col("dq_validations")
                ).otherwise(col("tmp_dq_validations"))
                if "dq_validations" in source_df.columns
                else col("tmp_dq_validations"),
            )
            .drop("tmp_dq_validations")
        )
