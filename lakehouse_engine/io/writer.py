"""Defines abstract writer behaviour."""
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, OrderedDict

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from lakehouse_engine.core.definitions import DQSpec, OutputSpec
from lakehouse_engine.transformers.transformer_factory import TransformerFactory
from lakehouse_engine.utils.logging_handler import LoggingHandler


class Writer(ABC):
    """Abstract Writer class."""

    def __init__(
        self, output_spec: OutputSpec, df: DataFrame, data: OrderedDict = None
    ):
        """Construct Writer instances.

        Args:
            output_spec: output specification to write data.
            df: dataframe to write.
            data: list of all dfs generated on previous steps before writer.
        """
        self._logger = LoggingHandler(self.__class__.__name__).get_logger()
        self._output_spec = output_spec
        self._df = df
        self._data = data

    @abstractmethod
    def write(self) -> Optional[OrderedDict]:
        """Abstract write method."""
        raise NotImplementedError

    @staticmethod
    def write_transformed_micro_batch(**kwargs: Any) -> Callable:
        """Define how to write a streaming micro batch after transforming it.

        This function must define an inner function that manipulates a streaming batch,
        and then return that function. Look for concrete implementations of this
        function for more clarity.

        Args:
            kwargs: any keyword arguments.

        Returns:
            A function to be executed in the foreachBatch spark write method.
        """

        def inner(batch_df: DataFrame, batch_id: int) -> None:
            logger = LoggingHandler(__name__).get_logger()
            logger.warning("Skipping transform micro batch... nothing to do.")

        return inner

    @classmethod
    def get_transformed_micro_batch(
        cls,
        output_spec: OutputSpec,
        batch_df: DataFrame,
        batch_id: int,
        data: OrderedDict,
    ) -> DataFrame:
        """Get the result of the transformations applied to a micro batch dataframe.

        Args:
            output_spec: output specification associated with the writer.
            batch_df: batch dataframe (given from streaming foreachBatch).
            batch_id: if of the batch (given from streaming foreachBatch).
            data: list of all dfs generated on previous steps before writer
            to be available on micro batch transforms.

        Returns:
            The transformed dataframe.
        """
        transformed_df = batch_df
        if output_spec.with_batch_id:
            transformed_df = transformed_df.withColumn("lhe_batch_id", lit(batch_id))

        for transformer in output_spec.streaming_micro_batch_transformers:
            transformed_df = transformed_df.transform(
                TransformerFactory.get_transformer(transformer, data)
            )

        return transformed_df

    @classmethod
    def get_streaming_trigger(cls, output_spec: OutputSpec) -> Dict:
        """Define which streaming trigger will be used.

        Args:
            output_spec: output specification.

        Returns:
            A dict containing streaming trigger.
        """
        trigger: Dict[str, Any] = {}

        if output_spec.streaming_available_now:
            trigger["availableNow"] = output_spec.streaming_available_now
        elif output_spec.streaming_once:
            trigger["once"] = output_spec.streaming_once
        elif output_spec.streaming_processing_time:
            trigger["processingTime"] = output_spec.streaming_processing_time
        elif output_spec.streaming_continuous:
            trigger["continuous"] = output_spec.streaming_continuous
        else:
            raise NotImplementedError(
                "The requested output spec streaming trigger is not supported."
            )

        return trigger

    @staticmethod
    def run_micro_batch_dq_process(df: DataFrame, dq_spec: List[DQSpec]) -> DataFrame:
        """Run the data quality process in a streaming micro batch dataframe.

        Iterates over the specs and performs the checks or analysis depending on the
        data quality specification provided in the configuration.

        Args:
            df: the dataframe in which to run the dq process on.
            dq_spec: data quality specification.

        Returns: the validated dataframe.
        """
        from lakehouse_engine.dq_processors.dq_factory import DQFactory

        validated_df = df
        for spec in dq_spec:
            validated_df = DQFactory.run_dq_process(spec, df)

        return validated_df
