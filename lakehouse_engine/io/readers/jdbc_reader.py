"""Module to define behaviour to read from JDBC sources."""
from pyspark.sql import DataFrame

from lakehouse_engine.core.definitions import InputFormat, InputSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.io.reader import Reader
from lakehouse_engine.transformers.exceptions import WrongArgumentsException
from lakehouse_engine.utils.extraction.jdbc_extraction_utils import (
    JDBCExtraction,
    JDBCExtractionUtils,
)


class JDBCReader(Reader):
    """Class to read from JDBC source."""

    def __init__(self, input_spec: InputSpec):
        """Construct JDBCReader instances.

        Args:
            input_spec: input specification.
        """
        super().__init__(input_spec)

    def read(self) -> DataFrame:
        """Read data from JDBC source.

        Returns:
            A dataframe containing the data from the JDBC source.
        """
        if (
            self._input_spec.options is not None
            and self._input_spec.options.get("predicates", None) is not None
        ):
            raise WrongArgumentsException("Predicates can only be used with jdbc_args.")

        options = self._input_spec.options if self._input_spec.options else {}
        if self._input_spec.calculate_upper_bound:
            jdbc_util = JDBCExtractionUtils(
                JDBCExtraction(
                    user=options["user"],
                    password=options["password"],
                    url=options["url"],
                    dbtable=options["dbtable"],
                    extraction_type=options.get(
                        "extraction_type", JDBCExtraction.extraction_type
                    ),
                    partition_column=options["partitionColumn"],
                    calc_upper_bound_schema=self._input_spec.calc_upper_bound_schema,
                    default_upper_bound=options.get(
                        "default_upper_bound", JDBCExtraction.default_upper_bound
                    ),
                )
            )  # type: ignore
            options["upperBound"] = jdbc_util.get_spark_jdbc_optimal_upper_bound()

        if self._input_spec.jdbc_args:
            return ExecEnv.SESSION.read.options(**options).jdbc(
                **self._input_spec.jdbc_args
            )
        else:
            return (
                ExecEnv.SESSION.read.format(InputFormat.JDBC.value)
                .options(**options)
                .load()
            )
