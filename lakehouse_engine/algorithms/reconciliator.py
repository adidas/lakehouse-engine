"""Module containing the Reconciliator class."""
from enum import Enum
from typing import List

import pyspark.sql.functions as spark_fns
from pyspark.sql import DataFrame
from pyspark.sql.functions import abs, coalesce, col, lit, when
from pyspark.sql.types import FloatType

from lakehouse_engine.algorithms.exceptions import ReconciliationFailedException
from lakehouse_engine.core.definitions import InputSpec, ReconciliatorSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.core.executable import Executable
from lakehouse_engine.io.reader_factory import ReaderFactory
from lakehouse_engine.transformers.optimizers import Optimizers
from lakehouse_engine.utils.logging_handler import LoggingHandler


class ReconciliationType(Enum):
    """Type of Reconciliation."""

    PCT = "percentage"
    ABS = "absolute"


class ReconciliationTransformers(Enum):
    """Transformers Available for the Reconciliation Algorithm."""

    AVAILABLE_TRANSFORMERS: dict = {
        "cache": Optimizers.cache,
        "persist": Optimizers.persist,
    }


class Reconciliator(Executable):
    """Class to define the behavior of an algorithm that checks if data reconciles.

    Checking if data reconciles, using this algorithm, is a matter of reading the
    'truth' data and the 'current' data. You can use any input specification compatible
    with the lakehouse engine to read 'truth' or 'current' data. On top of that, you
    can pass a 'truth_preprocess_query' and a 'current_preprocess_query' so you can
    preprocess the data before it goes into the actual reconciliation process.
    Moreover, you can use the 'truth_preprocess_query_args' and
    'current_preprocess_query_args' to pass additional arguments to be used to apply
    additional operations on top of the dataframe, resulting from the previous steps.
    With these arguments you can apply additional operations like caching or persisting
    the Dataframe. The way to pass the additional arguments for the operations is
    similar to the TransformSpec, but only a few operations are allowed. Those are
    defined in ReconciliationTransformers.AVAILABLE_TRANSFORMERS.

    The reconciliation process is focused on joining 'truth' with 'current' by all
    provided columns except the ones passed as 'metrics'. After that it calculates the
    differences in the metrics attributes (either percentage or absolute difference).
    Finally, it aggregates the differences, using the supplied aggregation function
    (e.g., sum, avg, min, max, etc).

    All of this configurations are passed via the ACON to instantiate a
    ReconciliatorSpec object.

    Notes:
        - It is crucial that both the current and truth datasets have exactly the same
            structure.
        - You should not use 0 as yellow or red threshold, as the algorithm will verify
            if the difference between the truth and current values is bigger or bigger
            or equal than those thresholds.
        - The reconciliation does not produce any negative values or percentages, as we
            use the absolute value of the differences. This means that the recon result
            will not indicate if it was the current values that were bigger or smaller
            than the truth values, or vice versa.
    """

    _logger = LoggingHandler(__name__).get_logger()

    def __init__(self, acon: dict):
        """Construct Algorithm instances.

        Args:
            acon: algorithm configuration.
        """
        self.spec: ReconciliatorSpec = ReconciliatorSpec(
            metrics=acon["metrics"],
            truth_input_spec=InputSpec(**acon["truth_input_spec"]),
            current_input_spec=InputSpec(**acon["current_input_spec"]),
            truth_preprocess_query=acon.get("truth_preprocess_query", None),
            truth_preprocess_query_args=acon.get("truth_preprocess_query_args", None),
            current_preprocess_query=acon.get("current_preprocess_query", None),
            current_preprocess_query_args=acon.get(
                "current_preprocess_query_args", None
            ),
            ignore_empty_df=acon.get("ignore_empty_df", False),
        )

    def get_source_of_truth(self) -> DataFrame:
        """Get the source of truth (expected result) for the reconciliation process.

        Returns:
            DataFrame containing the source of truth.
        """
        truth_df = ReaderFactory.get_data(self.spec.truth_input_spec)
        if self.spec.truth_preprocess_query:
            truth_df.createOrReplaceTempView("truth")
            truth_df = ExecEnv.SESSION.sql(self.spec.truth_preprocess_query)

        return truth_df

    def get_current_results(self) -> DataFrame:
        """Get the current results from the table that we are checking if it reconciles.

        Returns:
            DataFrame containing the current results.
        """
        current_df = ReaderFactory.get_data(self.spec.current_input_spec)
        if self.spec.current_preprocess_query:
            current_df.createOrReplaceTempView("current")
            current_df = ExecEnv.SESSION.sql(self.spec.current_preprocess_query)

        return current_df

    def execute(self) -> None:
        """Reconcile the current results against the truth dataset."""
        truth_df = self.get_source_of_truth()
        self._apply_preprocess_query_args(
            truth_df, self.spec.truth_preprocess_query_args
        )
        self._logger.info("Source of truth:")
        truth_df.show(1000, truncate=False)

        current_results_df = self.get_current_results()
        self._apply_preprocess_query_args(
            current_results_df, self.spec.current_preprocess_query_args
        )
        self._logger.info("Current results:")
        current_results_df.show(1000, truncate=False)

        status = "green"

        # if ignore_empty_df is true, run empty check on truth_df and current_results_df
        # if both the dataframe is empty then exit with green
        if (
            self.spec.ignore_empty_df
            and truth_df.isEmpty()
            and current_results_df.isEmpty()
        ):
            self._logger.info(
                f"ignore_empty_df is {self.spec.ignore_empty_df}, "
                f"truth_df and current_results_df are empty, "
                f"hence ignoring reconciliation"
            )
            self._logger.info("The Reconciliation process has succeeded.")
            return

        recon_results = self._get_recon_results(
            truth_df, current_results_df, self.spec.metrics
        )
        self._logger.info(f"Reconciliation result: {recon_results}")

        for m in self.spec.metrics:
            metric_name = f"{m['metric']}_{m['type']}_diff_{m['aggregation']}"
            if m["yellow"] <= recon_results[metric_name] < m["red"]:
                if status == "green":
                    # only switch to yellow if it was green before, otherwise we want
                    # to preserve 'red' as the final status.
                    status = "yellow"
            elif m["red"] <= recon_results[metric_name]:
                status = "red"

        if status != "green":
            raise ReconciliationFailedException(
                f"The Reconciliation process has failed with status: {status}."
            )
        else:
            self._logger.info("The Reconciliation process has succeeded.")

    @staticmethod
    def _apply_preprocess_query_args(
        df: DataFrame, preprocess_query_args: List[dict]
    ) -> DataFrame:
        """Apply transformers on top of the preprocessed query.

        Args:
            df: dataframe being transformed.
            preprocess_query_args: dict having the functions/transformations to
        apply and respective arguments.

        Returns: the transformed Dataframe.
        """
        transformed_df = df

        if preprocess_query_args is None:
            transformed_df = df.transform(Optimizers.cache())
        elif len(preprocess_query_args) > 0:
            for transformation in preprocess_query_args:
                function = ReconciliationTransformers.AVAILABLE_TRANSFORMERS.value[
                    transformation["function"]
                ](**transformation.get("args", {}))

                transformed_df = df.transform(function)
        else:
            transformed_df = df

        return transformed_df

    def _get_recon_results(
        self, truth_df: DataFrame, current_results_df: DataFrame, metrics: List[dict]
    ) -> dict:
        """Get the reconciliation results by comparing truth_df with current_results_df.

        Args:
            truth_df: dataframe with the truth data to reconcile against. It is
                typically an aggregated dataset to use as baseline and then we match the
                current_results_df (Aggregated at the same level) against this truth.
            current_results_df: dataframe with the current results of the dataset we
                are trying to reconcile.
            metrics: list of dicts containing metric, aggregation, yellow threshold and
                red threshold.

        Return:
            dictionary with the results (difference between truth and current results)
        """
        if len(truth_df.head(1)) == 0 or len(current_results_df.head(1)) == 0:
            raise ReconciliationFailedException(
                "The reconciliation has failed because either the truth dataset or the "
                "current results dataset was empty."
            )

        # truth and current are joined on all columns except the metrics
        joined_df = truth_df.alias("truth").join(
            current_results_df.alias("current"),
            [
                truth_df[c] == current_results_df[c]
                for c in current_results_df.columns
                if c not in [m["metric"] for m in metrics]
            ],
            how="full",
        )

        for m in metrics:
            if m["type"] == ReconciliationType.PCT.value:
                joined_df = joined_df.withColumn(
                    f"{m['metric']}_{m['type']}_diff",
                    coalesce(
                        (
                            # we need to make sure we don't produce negative values
                            # because our thresholds only accept > or >= comparisons.
                            abs(
                                (
                                    col(f"current.{m['metric']}")
                                    - col(f"truth.{m['metric']}")
                                )
                                / abs(col(f"truth.{m['metric']}"))
                            )
                        ),
                        # if the formula above produces null, we need to consider where
                        # it came from: we check below if the values were the same,
                        # and if so the diff is 0, if not the diff is 1 (e.g., the null
                        # result might have come from a division by 0).
                        when(
                            col(f"current.{m['metric']}").eqNullSafe(
                                col(f"truth.{m['metric']}")
                            ),
                            lit(0),
                        ).otherwise(lit(1)),
                    ),
                )
            elif m["type"] == ReconciliationType.ABS.value:
                joined_df = joined_df.withColumn(
                    f"{m['metric']}_{m['type']}_diff",
                    abs(
                        coalesce(col(f"current.{m['metric']}"), lit(0))
                        - coalesce(col(f"truth.{m['metric']}"), lit(0))
                    ),
                )
            else:
                raise NotImplementedError(
                    "The requested reconciliation type is not yet implemented."
                )

            joined_df = joined_df.withColumn(
                f"{m['metric']}_{m['type']}_diff",
                col(f"{m['metric']}_{m['type']}_diff").cast(FloatType()),
            )

        results_df = joined_df.agg(
            *[
                getattr(spark_fns, m["aggregation"])(
                    f"{m['metric']}_{m['type']}_diff"
                ).alias(f"{m['metric']}_{m['type']}_diff_{m['aggregation']}")
                for m in metrics
            ]
        )

        return results_df.collect()[0].asDict()
