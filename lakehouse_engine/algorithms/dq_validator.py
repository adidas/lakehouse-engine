"""Module to define Data Validator class."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.utils import StreamingQueryException

from lakehouse_engine.algorithms.algorithm import Algorithm
from lakehouse_engine.core.definitions import DQSpec, DQValidatorSpec, InputSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.dq_processors.dq_factory import DQFactory
from lakehouse_engine.dq_processors.exceptions import DQValidationsFailedException
from lakehouse_engine.io.reader_factory import ReaderFactory
from lakehouse_engine.utils.logging_handler import LoggingHandler


class DQValidator(Algorithm):
    """Validate data using an algorithm configuration (ACON represented as dict).

    This algorithm focuses on isolate Data Quality Validations from loading,
    applying a set of data quality functions to a specific input dataset,
    without the need to define any output specification.
    You can use any input specification compatible with the lakehouse engine
    (dataframe, table, files, etc).
    """

    _LOGGER = LoggingHandler(__name__).get_logger()

    def __init__(self, acon: dict):
        """Construct DQValidator algorithm instances.

        A data quality validator needs the following specifications to work
        properly:
            - input specification (mandatory): specify how and what data to
            read.
            - data quality specification (mandatory): specify how to execute
            the data quality process.
            - restore_prev_version (optional): specify if, having
            delta table/files as input, they should be restored to the
            previous version if the data quality process fails. Note: this
            is only considered if fail_on_error is kept as True.

        Args:
            acon: algorithm configuration.
        """
        self.spec: DQValidatorSpec = DQValidatorSpec(
            input_spec=InputSpec(**acon["input_spec"]),
            dq_spec=self._get_dq_spec(acon["dq_spec"]),
            restore_prev_version=acon.get("restore_prev_version", None),
        )

    def read(self) -> DataFrame:
        """Read data from an input location into a distributed dataframe.

        Returns:
             Dataframe with data that was read.
        """
        current_df = ReaderFactory.get_data(self.spec.input_spec)

        return current_df

    def process_dq(self, data: DataFrame) -> DataFrame:
        """Process the data quality tasks for the data that was read.

        It supports a single input dataframe.

        It is possible to use data quality validators/expectations that will validate
        your data and fail the process in case the expectations are not met. The DQ
        process also generates and keeps updating a site containing the results of the
        expectations that were done on your data. The location of the site is
        configurable and can either be on file system or S3. If you define it to be
        stored on S3, you can even configure your S3 bucket to serve the site so that
        people can easily check the quality of your data. Moreover, it is also
        possible to store the result of the DQ process into a defined result sink.

        Args:
            data: input dataframe on which to run the DQ process.

        Returns:
            Validated dataframe.
        """
        return DQFactory.run_dq_process(self.spec.dq_spec, data)

    def execute(self) -> None:
        """Define the algorithm execution behaviour."""
        self._LOGGER.info("Starting read stage...")
        read_df = self.read()

        self._LOGGER.info("Starting data quality validator...")
        try:
            if read_df.isStreaming:
                # To handle streaming, and although we are not interested in
                # writing any data, we still need to start the streaming and
                # execute the data quality process in micro batches of data.
                def write_dq_validator_micro_batch(
                    batch_df: DataFrame, batch_id: int
                ) -> None:
                    self.process_dq(batch_df)

                read_df.writeStream.trigger(once=True).foreachBatch(
                    write_dq_validator_micro_batch
                ).start().awaitTermination()

            else:
                self.process_dq(read_df)
        except (DQValidationsFailedException, StreamingQueryException):
            if not self.spec.input_spec.df_name and self.spec.restore_prev_version:
                self._LOGGER.info("Restoring delta table/files to previous version...")

                self._restore_prev_version()

                raise DQValidationsFailedException(
                    "Data Quality Validations Failed! The delta "
                    "table/files were restored to the previous version!"
                )

            elif self.spec.dq_spec.fail_on_error:
                raise DQValidationsFailedException("Data Quality Validations Failed!")
        else:
            self._LOGGER.info("Execution of the algorithm has finished!")

    @staticmethod
    def _get_dq_spec(input_dq_spec: dict) -> DQSpec:
        """Get data quality specification from acon.

        Args:
            input_dq_spec: data quality specification.

        Returns:
            Data quality spec.
        """
        dq_spec, dq_functions, critical_functions = Algorithm.get_dq_spec(input_dq_spec)

        dq_spec.dq_functions = dq_functions
        dq_spec.critical_functions = critical_functions

        return dq_spec

    def _restore_prev_version(self) -> None:
        """Restore delta table or delta files to previous version."""
        if self.spec.input_spec.db_table:
            delta_table = DeltaTable.forName(
                ExecEnv.SESSION, self.spec.input_spec.db_table
            )
        else:
            delta_table = DeltaTable.forPath(
                ExecEnv.SESSION, self.spec.input_spec.location
            )

        previous_version = (
            delta_table.history().agg({"version": "max"}).collect()[0][0] - 1
        )

        delta_table.restoreToVersion(previous_version)
