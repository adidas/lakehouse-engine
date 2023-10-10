"""Module with dataset optimizer terminator."""
from typing import List, Optional

from pyspark.sql.utils import AnalysisException, ParseException

from lakehouse_engine.core.table_manager import TableManager
from lakehouse_engine.transformers.exceptions import WrongArgumentsException
from lakehouse_engine.utils.logging_handler import LoggingHandler


class DatasetOptimizer(object):
    """Class with dataset optimizer terminator."""

    _logger = LoggingHandler(__name__).get_logger()

    @classmethod
    def optimize_dataset(
        cls,
        db_table: Optional[str] = None,
        location: Optional[str] = None,
        compute_table_stats: bool = True,
        vacuum: bool = True,
        vacuum_hours: int = 720,
        optimize: bool = True,
        optimize_where: Optional[str] = None,
        optimize_zorder_col_list: Optional[List[str]] = None,
        debug: bool = False,
    ) -> None:
        """Optimize a dataset based on a set of pre-conceived optimizations.

        Most of the times the dataset is a table, but it can be a file-based one only.

        Args:
            db_table: database_name.table_name.
            location: dataset/table filesystem location.
            compute_table_stats: to compute table statistics or not.
            vacuum: (delta lake tables only) whether to vacuum the delta lake
                table or not.
            vacuum_hours: (delta lake tables only) number of hours to consider
                in vacuum operation.
            optimize: (delta lake tables only) whether to optimize the table or
                not. Custom optimize parameters can be supplied through ExecEnv (Spark)
                configs
            optimize_where: expression to use in the optimize function.
            optimize_zorder_col_list: (delta lake tables only) list of
                columns to consider in the zorder optimization process. Custom optimize
                parameters can be supplied through ExecEnv (Spark) configs.
            debug: flag indicating if we are just debugging this for local
                tests and therefore pass through all the exceptions to perform some
                assertions in local tests.
        """
        if optimize:
            if debug:
                try:
                    cls._optimize(
                        db_table, location, optimize_where, optimize_zorder_col_list
                    )
                except ParseException:
                    pass
            else:
                cls._optimize(
                    db_table, location, optimize_where, optimize_zorder_col_list
                )

        if vacuum:
            cls._vacuum(db_table, location, vacuum_hours)

        if compute_table_stats:
            if debug:
                try:
                    cls._compute_table_stats(db_table)
                except AnalysisException:
                    pass
            else:
                cls._compute_table_stats(db_table)

    @classmethod
    def _compute_table_stats(cls, db_table: str) -> None:
        """Compute table statistics.

        Args:
            db_table: <db>.<table> string.
        """
        if not db_table:
            raise WrongArgumentsException("A table needs to be provided.")

        config = {"function": "compute_table_statistics", "table_or_view": db_table}
        cls._logger.info(f"Computing table statistics for {db_table}...")
        TableManager(config).compute_table_statistics()

    @classmethod
    def _vacuum(cls, db_table: str, location: str, hours: int) -> None:
        """Vacuum a delta table.

        Args:
            db_table: <db>.<table> string. Takes precedence over location.
            location: location of the delta table.
            hours: number of hours to consider in vacuum operation.
        """
        if not db_table and not location:
            raise WrongArgumentsException("A table or location need to be provided.")

        table_or_location = db_table if db_table else f"delta.`{location}`"

        config = {
            "function": "compute_table_statistics",
            "table_or_view": table_or_location,
            "vacuum_hours": hours,
        }
        cls._logger.info(f"Vacuuming table {table_or_location}...")
        TableManager(config).vacuum()

    @classmethod
    def _optimize(
        cls, db_table: str, location: str, where: str, zorder_cols: List[str]
    ) -> None:
        """Optimize a delta table.

        Args:
            db_table: <db>.<table> string. Takes precedence over location.
            location: location of the delta table.
            where: expression to use in the optimize function.
            zorder_cols: list of columns to consider in the zorder optimization process.
        """
        if not db_table and not location:
            raise WrongArgumentsException("A table or location needs to be provided.")

        table_or_location = db_table if db_table else f"delta.`{location}`"

        config = {
            "function": "compute_table_statistics",
            "table_or_view": table_or_location,
            "optimize_where": where,
            "optimize_zorder_col_list": ",".join(zorder_cols if zorder_cols else []),
        }
        cls._logger.info(f"Optimizing table {table_or_location}...")
        TableManager(config).optimize()
