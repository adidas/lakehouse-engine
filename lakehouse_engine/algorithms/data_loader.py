"""Module to define DataLoader class."""
from collections import OrderedDict
from copy import deepcopy
from logging import Logger
from typing import List, Optional

from lakehouse_engine.algorithms.algorithm import Algorithm
from lakehouse_engine.core.definitions import (
    DQFunctionSpec,
    DQSpec,
    DQType,
    InputSpec,
    MergeOptions,
    OutputFormat,
    OutputSpec,
    ReadType,
    TerminatorSpec,
    TransformerSpec,
    TransformSpec,
)
from lakehouse_engine.dq_processors.dq_factory import DQFactory
from lakehouse_engine.io.reader_factory import ReaderFactory
from lakehouse_engine.io.writer_factory import WriterFactory
from lakehouse_engine.terminators.notifier_factory import NotifierFactory
from lakehouse_engine.terminators.terminator_factory import TerminatorFactory
from lakehouse_engine.transformers.transformer_factory import TransformerFactory
from lakehouse_engine.utils.logging_handler import LoggingHandler


class DataLoader(Algorithm):
    """Load data using an algorithm configuration (ACON represented as dict).

    This algorithm focuses on the cases where users will be specifying all the algorithm
    steps and configurations through a dict based configuration, which we name ACON
    in our framework.

    Since an ACON is a dict you can pass a custom transformer trough a python function
    and, therefore, the DataLoader can also be used to load data with custom
    transformations not provided in our transformers package.

    As the algorithm base class of the lakehouse-engine framework is based on the
    concept of ACON, this DataLoader algorithm simply inherits from Algorithm,
    without overriding anything. We designed the codebase like this to avoid
    instantiating the Algorithm class directly, which was always meant to be an
    abstraction for any specific algorithm included in the lakehouse-engine framework.
    """

    def __init__(self, acon: dict):
        """Construct DataLoader algorithm instances.

        A data loader needs several specifications to work properly,
        but some of them might be optional. The available specifications are:

            - input specifications (mandatory): specify how to read data.
            - transform specifications (optional): specify how to transform data.
            - data quality specifications (optional): specify how to execute the data
                quality process.
            - output specifications (mandatory): specify how to write data to the
                target.
            - terminate specifications (optional): specify what to do after writing into
                the target (e.g., optimizing target table, vacuum, compute stats, etc).

        Args:
            acon: algorithm configuration.
        """
        self._logger: Logger = LoggingHandler(self.__class__.__name__).get_logger()
        super().__init__(acon)
        self.input_specs: List[InputSpec] = self._get_input_specs()
        # the streaming transformers plan is needed to future change the
        # execution specification to accommodate streaming mode limitations in invoking
        # certain functions (e.g., sort, window, generate row ids/auto increments, ...).
        self._streaming_micro_batch_transformers_plan: dict = {}
        self.transform_specs: List[TransformSpec] = self._get_transform_specs()
        # our data quality process is not compatible with streaming mode, hence we
        # have to run it in micro batches, similar to what happens to certain
        # transformation functions not supported in streaming mode.
        self._streaming_micro_batch_dq_plan: dict = {}
        self.dq_specs: List[DQSpec] = self._get_dq_specs()
        self.output_specs: List[OutputSpec] = self._get_output_specs()
        self.terminate_specs: List[TerminatorSpec] = self._get_terminate_specs()

    def read(self) -> OrderedDict:
        """Read data from an input location into a distributed dataframe.

        Returns:
             An ordered dict with all the dataframes that were read.
        """
        read_dfs: OrderedDict = OrderedDict({})
        for spec in self.input_specs:
            self._logger.info(f"Found input specification: {spec}")
            read_dfs[spec.spec_id] = ReaderFactory.get_data(spec)
        return read_dfs

    def transform(self, data: OrderedDict) -> OrderedDict:
        """Transform (optionally) the data that was read.

        If there isn't a transformation specification this step will be skipped, and the
        original dataframes that were read will be returned.
        Transformations can have dependency from another transformation result, however
        we need to keep in mind if we are using streaming source and for some reason we
        need to enable micro batch processing, this result cannot be used as input to
        another transformation. Micro batch processing in pyspark streaming is only
        available in .write(), which means this transformation with micro batch needs
        to be the end of the process.

        Args:
            data: input dataframes in an ordered dict.

        Returns:
            Another ordered dict with the transformed dataframes, according to the
            transformation specification.
        """
        if not self.transform_specs:
            return data
        else:
            transformed_dfs = OrderedDict(data)
            for spec in self.transform_specs:
                self._logger.info(f"Found transform specification: {spec}")
                transformed_df = transformed_dfs[spec.input_id]
                for transformer in spec.transformers:
                    transformed_df = transformed_df.transform(
                        TransformerFactory.get_transformer(transformer, transformed_dfs)
                    )
                transformed_dfs[spec.spec_id] = transformed_df
            return transformed_dfs

    def process_dq(self, data: OrderedDict) -> OrderedDict:
        """Process the data quality tasks for the data that was read and/or transformed.

        It supports multiple input dataframes. Although just one is advisable.

        It is possible to use data quality validators/expectations that will validate
        your data and fail the process in case the expectations are not met. The DQ
        process also generates and keeps updating a site containing the results of the
        expectations that were done on your data. The location of the site is
        configurable and can either be on file system or S3. If you define it to be
        stored on S3, you can even configure your S3 bucket to serve the site so that
        people can easily check the quality of your data. Moreover, it is also
        possible to store the result of the DQ process into a defined result sink.

        Args:
            data: dataframes from previous steps of the algorithm that we which to
                run the DQ process on.

        Returns:
            Another ordered dict with the validated dataframes.
        """
        if not self.dq_specs:
            return data
        else:
            dq_processed_dfs = OrderedDict(data)
            for spec in self.dq_specs:
                df_processed_df = dq_processed_dfs[spec.input_id]
                self._logger.info(f"Found data quality specification: {spec}")
                if (
                    spec.dq_functions
                    or spec.dq_type == DQType.ASSISTANT.value
                    and spec.spec_id not in self._streaming_micro_batch_dq_plan
                ):
                    if spec.cache_df:
                        df_processed_df.cache()
                    dq_processed_dfs[spec.spec_id] = DQFactory.run_dq_process(
                        spec, df_processed_df
                    )
                else:
                    dq_processed_dfs[spec.spec_id] = df_processed_df
            return dq_processed_dfs

    def write(self, data: OrderedDict) -> OrderedDict:
        """Write the data that was read and transformed (if applicable).

        It supports writing multiple datasets. However, we only recommend to write one
        dataframe. This recommendation is based on easy debugging and reproducibility,
        since if we start mixing several datasets being fueled by the same algorithm, it
        would unleash an infinite sea of reproducibility issues plus tight coupling and
        dependencies between datasets. Having said that, there may be cases where
        writing multiple datasets is desirable according to the use case requirements.
        Use it accordingly.

        Args:
            data: dataframes that were read and transformed (if applicable).

        Returns:
            Dataframes that were written.
        """
        written_dfs: OrderedDict = OrderedDict({})
        for spec in self.output_specs:
            self._logger.info(f"Found output specification: {spec}")

            written_output = WriterFactory.get_writer(
                spec, data[spec.input_id], data
            ).write()
            if written_output:
                written_dfs.update(written_output)
            else:
                written_dfs[spec.spec_id] = data[spec.input_id]

        return written_dfs

    def terminate(self, data: OrderedDict) -> None:
        """Terminate the algorithm.

        Args:
            data: dataframes that were written.
        """
        if self.terminate_specs:
            for spec in self.terminate_specs:
                self._logger.info(f"Found terminate specification: {spec}")
                TerminatorFactory.execute_terminator(
                    spec, data[spec.input_id] if spec.input_id else None
                )

    def execute(self) -> Optional[OrderedDict]:
        """Define the algorithm execution behaviour."""
        try:
            self._logger.info("Starting read stage...")
            read_dfs = self.read()
            self._logger.info("Starting transform stage...")
            transformed_dfs = self.transform(read_dfs)
            self._logger.info("Starting data quality stage...")
            validated_dfs = self.process_dq(transformed_dfs)
            self._logger.info("Starting write stage...")
            written_dfs = self.write(validated_dfs)
            self._logger.info("Starting terminate stage...")
            self.terminate(written_dfs)
            self._logger.info("Execution of the algorithm has finished!")
        except Exception as e:
            NotifierFactory.generate_failure_notification(self.terminate_specs, e)
            raise e

        return written_dfs

    def _get_input_specs(self) -> List[InputSpec]:
        """Get the input specifications from an acon.

        Returns:
            List of input specifications.
        """
        return [InputSpec(**spec) for spec in self.acon["input_specs"]]

    def _get_transform_specs(self) -> List[TransformSpec]:
        """Get the transformation specifications from an acon.

        If we are executing the algorithm in streaming mode and if the
        transformer function is not supported in streaming mode, it is
        important to note that ONLY those unsupported operations will
        go into the streaming_micro_batch_transformers (see if in the function code),
        in the same order that they appear in the list of transformations. This means
        that other supported transformations that appear after an
        unsupported one continue to stay one the normal execution plan,
        i.e., outside the foreachBatch function. Therefore this may
        make your algorithm to execute a different logic than the one you
        originally intended. For this reason:
            1) ALWAYS PLACE UNSUPPORTED STREAMING TRANSFORMATIONS AT LAST;
            2) USE force_streaming_foreach_batch_processing option in transform_spec
            section.
            3) USE THE CUSTOM_TRANSFORMATION AND WRITE ALL YOUR TRANSFORMATION LOGIC
            THERE.

        Check list of unsupported spark streaming operations here:
        https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations

        Returns:
            List of transformation specifications.
        """
        input_read_types = self._get_input_read_types(self.acon["input_specs"])
        transform_input_ids = self._get_transform_input_ids(
            self.acon.get("transform_specs", [])
        )
        prev_spec_read_types = self._get_previous_spec_read_types(
            input_read_types, transform_input_ids
        )
        transform_specs = []
        for spec in self.acon.get("transform_specs", []):
            transform_spec = TransformSpec(
                spec_id=spec["spec_id"],
                input_id=spec["input_id"],
                transformers=[],
                force_streaming_foreach_batch_processing=spec.get(
                    "force_streaming_foreach_batch_processing", False
                ),
            )

            for s in spec["transformers"]:
                transformer_spec = TransformerSpec(
                    function=s["function"], args=s.get("args", {})
                )
                if (
                    prev_spec_read_types[transform_spec.input_id]
                    == ReadType.STREAMING.value
                    and s["function"]
                    in TransformerFactory.UNSUPPORTED_STREAMING_TRANSFORMERS
                ) or (
                    prev_spec_read_types[transform_spec.input_id]
                    == ReadType.STREAMING.value
                    and transform_spec.force_streaming_foreach_batch_processing
                ):
                    self._move_to_streaming_micro_batch_transformers(
                        transform_spec, transformer_spec
                    )
                else:
                    transform_spec.transformers.append(transformer_spec)

            transform_specs.append(transform_spec)

        return transform_specs

    def _get_dq_specs(self) -> List[DQSpec]:
        """Get list of data quality specification objects from acon.

        In streaming mode, we automatically convert the data quality specification in
        the streaming_micro_batch_dq_processors list for the respective output spec.
        This is needed because our dq process cannot be executed using native streaming
        functions.

        Returns:
            List of data quality spec objects.
        """
        input_read_types = self._get_input_read_types(self.acon["input_specs"])
        transform_input_ids = self._get_transform_input_ids(
            self.acon.get("transform_specs", [])
        )
        prev_spec_read_types = self._get_previous_spec_read_types(
            input_read_types, transform_input_ids
        )

        dq_specs = []
        for spec in self.acon.get("dq_specs", []):
            dq_spec, dq_functions, critical_functions = Algorithm.get_dq_spec(spec)

            if prev_spec_read_types[dq_spec.input_id] == ReadType.STREAMING.value:
                # we need to use deepcopy to explicitly create a copy of the dict
                # otherwise python only create binding for dicts, and we would be
                # modifying the original dict, which we don't want to.
                self._move_to_streaming_micro_batch_dq_processors(
                    deepcopy(dq_spec), dq_functions, critical_functions
                )
            else:
                dq_spec.dq_functions = dq_functions
                dq_spec.critical_functions = critical_functions

            self._logger.info(
                f"Streaming Micro Batch DQ Plan: "
                f"{str(self._streaming_micro_batch_dq_plan)}"
            )
            dq_specs.append(dq_spec)

        return dq_specs

    def _get_output_specs(self) -> List[OutputSpec]:
        """Get the output specifications from an acon.

        Returns:
            List of output specifications.
        """
        return [
            OutputSpec(
                spec_id=spec["spec_id"],
                input_id=spec["input_id"],
                write_type=spec.get("write_type", None),
                data_format=spec.get("data_format", OutputFormat.DELTAFILES.value),
                db_table=spec.get("db_table", None),
                location=spec.get("location", None),
                merge_opts=MergeOptions(**spec["merge_opts"])
                if spec.get("merge_opts")
                else None,
                partitions=spec.get("partitions", []),
                streaming_micro_batch_transformers=self._get_streaming_transformer_plan(
                    spec["input_id"], self.dq_specs
                ),
                streaming_once=spec.get("streaming_once", None),
                streaming_processing_time=spec.get("streaming_processing_time", None),
                streaming_available_now=spec.get(
                    "streaming_available_now",
                    False
                    if (
                        spec.get("streaming_once", None)
                        or spec.get("streaming_processing_time", None)
                        or spec.get("streaming_continuous", None)
                    )
                    else True,
                ),
                streaming_continuous=spec.get("streaming_continuous", None),
                streaming_await_termination=spec.get(
                    "streaming_await_termination", True
                ),
                streaming_await_termination_timeout=spec.get(
                    "streaming_await_termination_timeout", None
                ),
                with_batch_id=spec.get("with_batch_id", False),
                options=spec.get("options", None),
                streaming_micro_batch_dq_processors=(
                    self._streaming_micro_batch_dq_plan.get(spec["input_id"], [])
                ),
            )
            for spec in self.acon["output_specs"]
        ]

    def _get_streaming_transformer_plan(
        self, input_id: str, dq_specs: List[DQSpec]
    ) -> List[TransformerSpec]:
        """Gets the plan for transformations to be applied on streaming micro batches.

        When running both DQ processes and transformations in streaming micro batches,
        the _streaming_micro_batch_transformers_plan to consider is the one associated
        with the transformer spec_id and not with the dq spec_id. Thus, on those cases,
        this method maps the input id of the output_spec (which is the spec_id of a
        dq_spec) with the dependent transformer spec_id.

        Args:
            input_id: id of the corresponding input specification.
            dq_specs: data quality specifications.

        Returns: a list of TransformerSpec, representing the transformations plan.
        """
        transformer_id = (
            [dq_spec.input_id for dq_spec in dq_specs if dq_spec.spec_id == input_id][0]
            if self._streaming_micro_batch_dq_plan.get(input_id)
            and self._streaming_micro_batch_transformers_plan
            else input_id
        )

        return self._streaming_micro_batch_transformers_plan.get(transformer_id, [])

    def _get_terminate_specs(self) -> List[TerminatorSpec]:
        """Get the terminate specifications from an acon.

        Returns:
            List of terminate specifications.
        """
        return [TerminatorSpec(**spec) for spec in self.acon.get("terminate_specs", [])]

    def _move_to_streaming_micro_batch_transformers(
        self, transform_spec: TransformSpec, transformer_spec: TransformerSpec
    ) -> None:
        """Move the transformer to the list of streaming micro batch transformations.

        If the transform specs contain functions that cannot be executed in streaming
        mode, this function sends those functions to the output specs
        streaming_micro_batch_transformers, where they will be executed inside the
        stream foreachBatch function.

        To accomplish that we use an instance variable that associates the
        streaming_micro_batch_transformers to each output spec, in order to do reverse
        lookup when creating the OutputSpec.

        Args:
            transform_spec: transform specification (overall
                transformation specification - a transformation may contain multiple
                transformers).
            transformer_spec: the specific transformer function and arguments.
        """
        if transform_spec.spec_id not in self._streaming_micro_batch_transformers_plan:
            self._streaming_micro_batch_transformers_plan[transform_spec.spec_id] = []

        self._streaming_micro_batch_transformers_plan[transform_spec.spec_id].append(
            transformer_spec
        )

    def _move_to_streaming_micro_batch_dq_processors(
        self,
        dq_spec: DQSpec,
        dq_functions: List[DQFunctionSpec],
        critical_functions: List[DQFunctionSpec],
    ) -> None:
        """Move the dq function to the list of streaming micro batch transformations.

        If the dq specs contain functions that cannot be executed in streaming mode,
        this function sends those functions to the output specs
        streaming_micro_batch_dq_processors, where they will be executed inside the
        stream foreachBatch function.

        To accomplish that we use an instance variable that associates the
        streaming_micro_batch_dq_processors to each output spec, in order to do reverse
        lookup when creating the OutputSpec.

        Args:
            dq_spec: dq specification (overall dq process specification).
            dq_functions: the list of dq functions to be considered.
            critical_functions: list of critical functions to be considered.
        """
        if dq_spec.spec_id not in self._streaming_micro_batch_dq_plan:
            self._streaming_micro_batch_dq_plan[dq_spec.spec_id] = []

        dq_spec.dq_functions = dq_functions
        dq_spec.critical_functions = critical_functions
        self._streaming_micro_batch_dq_plan[dq_spec.spec_id].append(dq_spec)

    @staticmethod
    def _get_input_read_types(list_of_specs: List) -> dict:
        """Get a dict of spec ids and read types from a list of input specs.

        Args:
            list_of_specs: list of input specs ([{k:v}]).

        Returns:
            Dict of {input_spec_id: read_type}.
        """
        return {item["spec_id"]: item["read_type"] for item in list_of_specs}

    @staticmethod
    def _get_transform_input_ids(list_of_specs: List) -> dict:
        """Get a dict of transform spec ids and input ids from list of transform specs.

        Args:
            list_of_specs: list of transform specs ([{k:v}]).

        Returns:
            Dict of {transform_spec_id: input_id}.
        """
        return {item["spec_id"]: item["input_id"] for item in list_of_specs}

    @staticmethod
    def _get_previous_spec_read_types(
        input_read_types: dict, transform_input_ids: dict
    ) -> dict:
        """Get the read types of the previous specification: input and/or transform.

        For the chaining transformations and for DQ process to work seamlessly in batch
        and streaming mode, we have to figure out if the previous spec to the transform
        or dq spec(e.g., input spec or transform spec) refers to a batch read type or
        a streaming read type.

        Args:
            input_read_types: dict of {input_spec_id: read_type}.
            transform_input_ids: dict of {transform_spec_id: input_id}.

        Returns:
            Dict of {input_spec_id or transform_spec_id: read_type}
        """
        combined_read_types = input_read_types
        for spec_id, input_id in transform_input_ids.items():
            combined_read_types[spec_id] = combined_read_types[input_id]

        return combined_read_types
