"""Table manager module."""
from typing import List

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import translate

from lakehouse_engine.core.definitions import SQLDefinitions
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.configs.config_utils import ConfigUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler


class TableManager(object):
    """Set of actions to manipulate tables/views in several ways."""

    def __init__(self, configs: dict):
        """Construct TableManager algorithm instances.

        Args:
            configs: configurations for the TableManager algorithm.
        """
        self._logger = LoggingHandler(__name__).get_logger()
        self.configs = configs
        self.function = self.configs["function"]

    def get_function(self) -> None:
        """Get a specific function to execute."""
        available_functions = {
            "compute_table_statistics": self.compute_table_statistics,
            "create_table": self.create,
            "create_tables": self.create_many,
            "create_view": self.create,
            "drop_table": self.drop_table,
            "drop_view": self.drop_view,
            "execute_sql": self.execute_sql,
            "truncate": self.truncate,
            "vacuum": self.vacuum,
            "describe": self.describe,
            "optimize": self.optimize,
            "show_tbl_properties": self.show_tbl_properties,
            "get_tbl_pk": self.get_tbl_pk,
            "repair_table": self.repair_table,
            "delete_where": self.delete_where,
        }

        self._logger.info("Function being executed: {}".format(self.function))

        if self.function in available_functions.keys():
            func = available_functions[self.function]
            func()
        else:
            raise NotImplementedError(
                f"The requested function {self.function} is not implemented."
            )

    def create(self) -> None:
        """Create a new table or view on metastore."""
        sql = ConfigUtils.read_sql(self.configs["path"])
        try:
            for command in sql.split(";"):
                if command.strip():
                    self._logger.info(f"sql command: {command}")
                    ExecEnv.SESSION.sql(command)
            self._logger.info(f"{self.function} successfully executed!")
        except Exception as e:
            self._logger.error(e)
            raise

    def create_many(self) -> None:
        """Create multiple tables or views on metastore.

        In this function the path to the ddl files can be separated by comma.
        """
        self.execute_multiple_sql_files()

    def compute_table_statistics(self) -> None:
        """Compute table statistics."""
        sql = SQLDefinitions.compute_table_stats.value.format(
            self.configs["table_or_view"]
        )
        try:
            self._logger.info(f"sql command: {sql}")
            ExecEnv.SESSION.sql(sql)
            self._logger.info(f"{self.function} successfully executed!")
        except Exception as e:
            self._logger.error(e)
            raise

    def drop_table(self) -> None:
        """Delete table function deletes table from metastore and erases all data."""
        drop_stmt = "{} {}".format(
            SQLDefinitions.drop_table_stmt.value,
            self.configs["table_or_view"],
        )

        self._logger.info(f"sql command: {drop_stmt}")
        ExecEnv.SESSION.sql(drop_stmt)
        self._logger.info("Table successfully dropped!")

    def drop_view(self) -> None:
        """Delete view function deletes view from metastore and erases all data."""
        drop_stmt = "{} {}".format(
            SQLDefinitions.drop_view_stmt.value,
            self.configs["table_or_view"],
        )

        self._logger.info(f"sql command: {drop_stmt}")
        ExecEnv.SESSION.sql(drop_stmt)
        self._logger.info("View successfully dropped!")

    def truncate(self) -> None:
        """Truncate function erases all data but keeps metadata."""
        truncate_stmt = "{} {}".format(
            SQLDefinitions.truncate_stmt.value,
            self.configs["table_or_view"],
        )

        self._logger.info(f"sql command: {truncate_stmt}")
        ExecEnv.SESSION.sql(truncate_stmt)
        self._logger.info("Table successfully truncated!")

    def vacuum(self) -> None:
        """Vacuum function erases older versions from Delta Lake tables or locations."""
        if not self.configs.get("table_or_view", None):
            delta_table = DeltaTable.forPath(ExecEnv.SESSION, self.configs["path"])

            self._logger.info(f"Vacuuming location: {self.configs['path']}")
            delta_table.vacuum(self.configs.get("vacuum_hours", 168))
        else:
            delta_table = DeltaTable.forName(
                ExecEnv.SESSION, self.configs["table_or_view"]
            )

            self._logger.info(f"Vacuuming table: {self.configs['table_or_view']}")
            delta_table.vacuum(self.configs.get("vacuum_hours", 168))

    def describe(self) -> None:
        """Describe function describes metadata from some table or view."""
        describe_stmt = "{} {}".format(
            SQLDefinitions.describe_stmt.value,
            self.configs["table_or_view"],
        )

        self._logger.info(f"sql command: {describe_stmt}")
        output = ExecEnv.SESSION.sql(describe_stmt)
        self._logger.info(output)

    def optimize(self) -> None:
        """Optimize function optimizes the layout of Delta Lake data."""
        if self.configs.get("where_clause", None):
            where_exp = "WHERE {}".format(self.configs["where_clause"].strip())
        else:
            where_exp = ""

        if self.configs.get("optimize_zorder_col_list", None):
            zorder_exp = "ZORDER BY ({})".format(
                self.configs["optimize_zorder_col_list"].strip()
            )
        else:
            zorder_exp = ""

        optimize_stmt = "{} {} {} {}".format(
            SQLDefinitions.optimize_stmt.value,
            f"delta.`{self.configs.get('path', None)}`"
            if not self.configs.get("table_or_view", None)
            else self.configs.get("table_or_view", None),
            where_exp,
            zorder_exp,
        )

        self._logger.info(f"sql command: {optimize_stmt}")
        output = ExecEnv.SESSION.sql(optimize_stmt)
        self._logger.info(output)

    def execute_multiple_sql_files(self) -> None:
        """Execute multiple statements in multiple sql files.

        In this function the path to the files is separated by comma.
        """
        for table_metadata_file in self.configs["path"].split(","):
            sql = ConfigUtils.read_sql(table_metadata_file.strip())
            for command in sql.split(";"):
                if command.strip():
                    self._logger.info(f"sql command: {command}")
                    ExecEnv.SESSION.sql(command)
            self._logger.info("sql file successfully executed!")

    def execute_sql(self) -> None:
        """Execute sql commands separated by semicolon (;)."""
        for command in self.configs.get("sql").split(";"):
            if command.strip():
                self._logger.info(f"sql command: {command}")
                ExecEnv.SESSION.sql(command)
        self._logger.info("sql successfully executed!")

    def show_tbl_properties(self) -> DataFrame:
        """Show Table Properties.

        Returns: a dataframe with the table properties.
        """
        show_tbl_props_stmt = "{} {}".format(
            SQLDefinitions.show_tbl_props_stmt.value,
            self.configs["table_or_view"],
        )

        self._logger.info(f"sql command: {show_tbl_props_stmt}")
        output = ExecEnv.SESSION.sql(show_tbl_props_stmt)
        self._logger.info(output)
        return output

    def get_tbl_pk(self) -> List[str]:
        """Get the primary key of a particular table.

        Returns: the list of columns that are part of the primary key.
        """
        output: List[str] = (
            self.show_tbl_properties()
            .filter("key == 'lakehouse.primary_key'")
            .select("value")
            .withColumn("value", translate("value", " `", ""))
            .first()[0]
            .split(",")
        )
        self._logger.info(output)

        return output

    def repair_table(self) -> None:
        """Run the repair table command."""
        table_name = self.configs["table_or_view"]
        sync_metadata = self.configs["sync_metadata"]

        repair_stmt = (
            f"MSCK REPAIR TABLE {table_name} "
            f"{'SYNC METADATA' if sync_metadata else ''}"
        )

        self._logger.info(f"sql command: {repair_stmt}")
        output = ExecEnv.SESSION.sql(repair_stmt)
        self._logger.info(output)

    def delete_where(self) -> None:
        """Run the delete where command."""
        table_name = self.configs["table_or_view"]
        delete_where = self.configs["where_clause"].strip()

        delete_stmt = SQLDefinitions.delete_where_stmt.value.format(
            table_name, delete_where
        )

        self._logger.info(f"sql command: {delete_stmt}")
        output = ExecEnv.SESSION.sql(delete_stmt)
        self._logger.info(output)
