"""Module with the factory pattern to return terminators."""
from typing import Optional

from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import TerminatorSpec
from lakehouse_engine.terminators.notifier import Notifier
from lakehouse_engine.terminators.notifier_factory import NotifierFactory
from lakehouse_engine.utils.logging_handler import LoggingHandler


class TerminatorFactory(object):
    """TerminatorFactory class following the factory pattern."""

    _logger = LoggingHandler(__name__).get_logger()

    @staticmethod
    def execute_terminator(
        spec: TerminatorSpec, df: Optional[DataFrame] = None
    ) -> None:
        """Execute a terminator following the factory pattern.

        Args:
            spec: terminator specification.
            df: dataframe to be used in the terminator. Needed when a
                terminator requires one dataframe as input.

        Returns:
            Transformer function to be executed in .transform() spark function.
        """
        if spec.function == "optimize_dataset":
            from lakehouse_engine.terminators.dataset_optimizer import DatasetOptimizer

            DatasetOptimizer.optimize_dataset(**spec.args)
        elif spec.function == "terminate_spark":
            from lakehouse_engine.terminators.spark_terminator import SparkTerminator

            SparkTerminator.terminate_spark()
        elif spec.function == "expose_cdf":
            from lakehouse_engine.terminators.cdf_processor import CDFProcessor

            CDFProcessor.expose_cdf(spec)
        elif spec.function == "notify":
            if not Notifier.check_if_notification_is_failure_notification(spec):
                notifier = NotifierFactory.get_notifier(spec)
                notifier.create_notification()
                notifier.send_notification()
        else:
            raise NotImplementedError(
                f"The requested terminator {spec.function} is not implemented."
            )
