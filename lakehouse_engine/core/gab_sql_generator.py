"""Module to define GAB SQL classes."""

import ast
import json
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, struct, to_json

from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.utils.gab_utils import GABUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler


def _execute_sql(func) -> Callable:  # type: ignore
    """Execute the SQL resulting from the function.

    This function is protected to be used just in this module.
    It's used to decorate functions that returns a SQL statement.

    Args:
        func: function that will return the sql to execute
    """

    def inner(*args: Any) -> None:
        generated_sql = func(*args)
        if generated_sql:
            ExecEnv.SESSION.sql(generated_sql)

    return inner


class GABSQLGenerator(ABC):
    """Abstract class defining the behaviour of a GAB SQL Generator."""

    @abstractmethod
    def generate_sql(self) -> Optional[str]:
        """Define the generate sql command.

        E.g., the behaviour of gab generate sql inheriting from this.
        """
        pass


class GABInsertGenerator(GABSQLGenerator):
    """GAB insert generator.

    Creates the insert statement based on the dimensions and metrics provided in
    the configuration table.
    """

    _LOGGER = LoggingHandler(__name__).get_logger()

    def __init__(
        self,
        query_id: str,
        cadence: str,
        final_stage_table: str,
        lookup_query_builder: DataFrame,
        target_database: str,
        target_table: str,
    ):
        """Construct GABInsertGenerator instances.

        Args:
            query_id: gab configuration table use case identifier.
            cadence:  inputted cadence to process.
            final_stage_table: stage view name.
            lookup_query_builder: gab configuration data.
            target_database: target database to write.
            target_table: target table to write.
        """
        self.query_id = query_id
        self.cadence = cadence
        self.final_stage_table = final_stage_table
        self.lookup_query_builder = lookup_query_builder
        self.target_database = target_database
        self.target_table = target_table

    def generate_sql(self) -> Optional[str]:
        """Generate insert sql statement to the insights table."""
        insert_sql_statement = self._insert_statement_generator()

        return insert_sql_statement

    def _insert_statement_generator(self) -> str:
        """Generate GAB insert statement.

        Creates the insert statement based on the dimensions and metrics provided in
        the configuration table.
        """
        result = GABUtils.get_json_column_as_dict(
            self.lookup_query_builder, self.query_id, "mappings"
        )

        for result_key in result.keys():
            joined_dimensions, joined_metrics = self._get_mapping_columns(
                mapping=result[result_key]
            )
            gen_ins = f"""
                INSERT INTO {self.target_database}.{self.target_table}
                SELECT
                    {self.query_id} as query_id,
                    '{self.cadence}' as cadence,
                    {joined_dimensions},
                    {joined_metrics},
                    current_timestamp() as lh_created_on
                FROM {self.final_stage_table}
                """  # nosec: B608

        return gen_ins

    @classmethod
    def _get_mapping_columns(cls, mapping: dict) -> tuple[str, str]:
        """Get mapping columns(dimensions and metrics) as joined string.

        Args:
            mapping: use case mappings configuration.
        """
        dimensions_mapping = mapping["dimensions"]
        metrics_mapping = mapping["metric"]

        joined_dimensions = cls._join_extracted_column_with_filled_columns(
            columns=dimensions_mapping, is_dimension=True
        )
        joined_metrics = cls._join_extracted_column_with_filled_columns(
            columns=metrics_mapping, is_dimension=False
        )

        return joined_dimensions, joined_metrics

    @classmethod
    def _join_extracted_column_with_filled_columns(
        cls, columns: dict, is_dimension: bool
    ) -> str:
        """Join extracted columns with empty filled columns.

        Args:
            columns: use case columns and values.
            is_dimension: flag identifying if is a dimension or a metric.
        """
        extracted_columns_with_alias = (
            GABUtils.extract_columns_from_mapping(  # type: ignore
                columns=columns, is_dimension=is_dimension
            )
        )

        filled_columns = cls._fill_empty_columns(
            extracted_columns=extracted_columns_with_alias,  # type: ignore
            is_dimension=is_dimension,
        )

        joined_columns = [*extracted_columns_with_alias, *filled_columns]

        return ",".join(joined_columns)

    @classmethod
    def _fill_empty_columns(
        cls, extracted_columns: list[str], is_dimension: bool
    ) -> list[str]:
        """Fill empty columns as null.

        As the data is expected to have 40 columns we have to fill the unused columns.

        Args:
            extracted_columns: use case extracted columns.
            is_dimension: flag identifying if is a dimension or a metric.
        """
        filled_columns = []

        for ins in range(
            (
                len(extracted_columns) - 1
                if is_dimension
                else len(extracted_columns) + 1
            ),
            41,
        ):
            filled_columns.append(
                " null as {}{}".format("d" if is_dimension else "m", ins)
            )

        return filled_columns


class GABViewGenerator(GABSQLGenerator):
    """GAB view generator.

    Creates the use case view statement to be consumed.
    """

    _LOGGER = LoggingHandler(__name__).get_logger()

    def __init__(
        self,
        cadence_snapshot_status: dict,
        target_database: str,
        view_name: str,
        final_cols: str,
        target_table: str,
        dimensions_and_metrics_with_alias: str,
        dimensions: str,
        dimensions_and_metrics: str,
        final_calculated_script: str,
        query_id: str,
        view_filter: str,
        final_calculated_script_snapshot: str,
        without_snapshot_cadences: list[str],
        with_snapshot_cadences: list[str],
    ):
        """Construct GABViewGenerator instances.

        Args:
            cadence_snapshot_status: each cadence with the corresponding snapshot
                status.
            target_database: target database to write.
            view_name: name of the view to be generated.
            final_cols: columns to return in the view.
            target_table: target table to write.
            dimensions_and_metrics_with_alias: configured dimensions and metrics with
                alias to compute in the view.
            dimensions: use case configured dimensions.
            dimensions_and_metrics: use case configured dimensions and metrics.
            final_calculated_script: use case calculated metrics.
            query_id: gab configuration table use case identifier.
            view_filter: filter to add in the view.
            final_calculated_script_snapshot: use case calculated metrics with snapshot.
            without_snapshot_cadences: cadences without snapshot.
            with_snapshot_cadences: cadences with snapshot.
        """
        self.cadence_snapshot_status = cadence_snapshot_status
        self.target_database = target_database
        self.result_key = view_name
        self.final_cols = final_cols
        self.target_table = target_table
        self.dimensions_and_metrics_with_alias = dimensions_and_metrics_with_alias
        self.dimensions = dimensions
        self.dimensions_and_metrics = dimensions_and_metrics
        self.final_calculated_script = final_calculated_script
        self.query_id = query_id
        self.view_filter = view_filter
        self.final_calculated_script_snapshot = final_calculated_script_snapshot
        self.without_snapshot_cadences = without_snapshot_cadences
        self.with_snapshot_cadences = with_snapshot_cadences

    @_execute_sql
    def generate_sql(self) -> Optional[str]:
        """Generate use case view sql statement."""
        consumption_view_sql = self._create_consumption_view()

        return consumption_view_sql

    def _create_consumption_view(self) -> str:
        """Create consumption view."""
        final_view_query = self._generate_consumption_view_statement(
            self.cadence_snapshot_status,
            self.target_database,
            self.final_cols,
            self.target_table,
            self.dimensions_and_metrics_with_alias,
            self.dimensions,
            self.dimensions_and_metrics,
            self.final_calculated_script,
            self.query_id,
            self.view_filter,
            self.final_calculated_script_snapshot,
            without_snapshot_cadences=",".join(
                f'"{w}"' for w in self.without_snapshot_cadences
            ),
            with_snapshot_cadences=",".join(
                f'"{w}"' for w in self.with_snapshot_cadences
            ),
        )

        rendered_query = """
            CREATE OR REPLACE VIEW {database}.{view_name} AS {final_view_query}
            """.format(
            database=self.target_database,
            view_name=self.result_key,
            final_view_query=final_view_query,
        )
        self._LOGGER.info(f"Consumption view statement: {rendered_query}")
        return rendered_query

    @classmethod
    def _generate_consumption_view_statement(
        cls,
        cadence_snapshot_status: dict,
        target_database: str,
        final_cols: str,
        target_table: str,
        dimensions_and_metrics_with_alias: str,
        dimensions: str,
        dimensions_and_metrics: str,
        final_calculated_script: str,
        query_id: str,
        view_filter: str,
        final_calculated_script_snapshot: str,
        without_snapshot_cadences: str,
        with_snapshot_cadences: str,
    ) -> str:
        """Generate consumption view.

        Args:
            cadence_snapshot_status: cadences to execute with the information if it has
                snapshot.
            target_database: target database to write.
            final_cols: use case columns exposed in the consumption view.
            target_table: target table to write.
            dimensions_and_metrics_with_alias: dimensions and metrics as string columns
                with alias.
            dimensions: dimensions as string columns.
            dimensions_and_metrics: dimensions and metrics as string columns
                without alias.
            final_calculated_script: final calculated metrics script.
            query_id: gab configuration table use case identifier.
            view_filter: filter to execute on the view.
            final_calculated_script_snapshot: final calculated metrics with snapshot
                script.
            without_snapshot_cadences: cadences without snapshot.
            with_snapshot_cadences: cadences with snapshot.
        """
        cls._LOGGER.info("Generating consumption view statement...")
        cls._LOGGER.info(
            f"""
            {{
                target_database: {target_database},
                target_table: {target_table},
                query_id: {query_id},
                cadence_and_snapshot_status: {cadence_snapshot_status},
                cadences_without_snapshot: [{without_snapshot_cadences}],
                cadences_with_snapshot: [{with_snapshot_cadences}],
                final_cols: {final_cols},
                dimensions_and_metrics_with_alias: {dimensions_and_metrics_with_alias},
                dimensions: {dimensions},
                dimensions_with_metrics: {dimensions_and_metrics},
                final_calculated_script: {final_calculated_script},
                final_calculated_script_snapshot: {final_calculated_script_snapshot},
                view_filter: {view_filter}
            }}"""
        )
        if (
            "Y" in cadence_snapshot_status.values()
            and "N" in cadence_snapshot_status.values()
        ):
            consumption_view_query = f"""
                WITH TEMP1 AS (
                    SELECT
                        a.cadence,
                        {dimensions_and_metrics_with_alias}{final_calculated_script}
                    FROM {target_database}.{target_table} a
                    WHERE a.query_id = {query_id}
                    AND cadence IN ({without_snapshot_cadences})
                    {view_filter}
                ),
                TEMP_RN AS (
                    SELECT
                        a.cadence,
                        a.from_date,
                        a.to_date,
                        {dimensions_and_metrics},
                        row_number() over(
                            PARTITION BY
                                a.cadence,
                                {dimensions},
                                a.from_date
                            order by to_date
                        ) as rn
                    FROM {target_database}.{target_table} a
                    WHERE a.query_id = {query_id}
                    AND cadence IN ({with_snapshot_cadences})
                    {view_filter}
                ),
                TEMP2 AS (
                    SELECT
                        a.cadence,
                        {dimensions_and_metrics_with_alias}{final_calculated_script_snapshot}
                    FROM TEMP_RN a
                ),
                TEMP3 AS (SELECT * FROM TEMP1 UNION SELECT * from TEMP2)
                SELECT {final_cols} FROM TEMP3
            """  # nosec: B608
        elif "N" in cadence_snapshot_status.values():
            consumption_view_query = f"""
                WITH TEMP1 AS (
                    SELECT
                        a.cadence,
                        {dimensions_and_metrics_with_alias}{final_calculated_script}
                    FROM {target_database}.{target_table} a
                    WHERE a.query_id = {query_id}
                    AND cadence IN ({without_snapshot_cadences})  {view_filter}
                )
                SELECT {final_cols} FROM TEMP1
            """  # nosec: B608
        else:
            consumption_view_query = f"""
                WITH TEMP_RN AS (
                    SELECT
                        a.cadence,
                        a.from_date,
                        a.to_date,
                        {dimensions_and_metrics},
                        row_number() over(
                            PARTITION BY
                                a.cadence,
                                a.from_date,
                                a.to_date,
                                {dimensions},
                                a.from_date
                        order by to_date) as rn
                    FROM {target_database}.{target_table} a
                    WHERE a.query_id = {query_id}
                    AND cadence IN ({with_snapshot_cadences})
                    {view_filter}
                ),
                TEMP2 AS (
                    SELECT
                        a.cadence,
                        {dimensions_and_metrics_with_alias}{final_calculated_script_snapshot}
                    FROM TEMP_RN a
                )
                SELECT {final_cols} FROM TEMP2
            """  # nosec: B608

        return consumption_view_query


class GABDeleteGenerator(GABSQLGenerator):
    """GAB delete generator.

    Creates the delete statement to clean the use case base data on the insights table.
    """

    _LOGGER = LoggingHandler(__name__).get_logger()

    def __init__(
        self,
        query_id: str,
        cadence: str,
        temp_stage_view_name: str,
        lookup_query_builder: DataFrame,
        target_database: str,
        target_table: str,
    ):
        """Construct GABViewGenerator instances.

        Args:
            query_id: gab configuration table use case identifier.
            cadence:  inputted cadence to process.
            temp_stage_view_name: stage view name.
            lookup_query_builder: gab configuration data.
            target_database: target database to write.
            target_table: target table to write.
        """
        self.query_id = query_id
        self.cadence = cadence
        self.temp_stage_view_name = temp_stage_view_name
        self.lookup_query_builder = lookup_query_builder
        self.target_database = target_database
        self.target_table = target_table

    @_execute_sql
    def generate_sql(self) -> Optional[str]:
        """Generate delete sql statement.

        This statement is to clean the insights table for the corresponding use case.
        """
        delete_sql_statement = self._delete_statement_generator()

        return delete_sql_statement

    def _delete_statement_generator(self) -> str:
        df_filtered = self.lookup_query_builder.filter(
            col("query_id") == lit(self.query_id)
        )

        df_map = df_filtered.select(col("mappings"))
        view_df = df_map.select(
            to_json(struct([df_map[x] for x in df_map.columns]))
        ).collect()[0][0]
        line = json.loads(view_df)

        for line_v in line.values():
            result = ast.literal_eval(line_v)

        for result_key in result.keys():
            result_new = result[result_key]
            dim_from_date = result_new["dimensions"]["from_date"]
            dim_to_date = result_new["dimensions"]["to_date"]

        self._LOGGER.info(f"temp stage view name: {self.temp_stage_view_name}")

        min_from_date = ExecEnv.SESSION.sql(
            """
            SELECT
                MIN({from_date}) as min_from_date
            FROM {iter_stages}""".format(  # nosec: B608
                iter_stages=self.temp_stage_view_name, from_date=dim_from_date
            )
        ).collect()[0][0]
        max_from_date = ExecEnv.SESSION.sql(
            """
            SELECT
                MAX({from_date}) as max_from_date
            FROM {iter_stages}""".format(  # nosec: B608
                iter_stages=self.temp_stage_view_name, from_date=dim_from_date
            )
        ).collect()[0][0]

        min_to_date = ExecEnv.SESSION.sql(
            """
            SELECT
                MIN({to_date}) as min_to_date
            FROM {iter_stages}""".format(  # nosec: B608
                iter_stages=self.temp_stage_view_name, to_date=dim_to_date
            )
        ).collect()[0][0]
        max_to_date = ExecEnv.SESSION.sql(
            """
            SELECT
                MAX({to_date}) as max_to_date
            FROM {iter_stages}""".format(  # nosec: B608
                iter_stages=self.temp_stage_view_name, to_date=dim_to_date
            )
        ).collect()[0][0]

        gen_del = """
        DELETE FROM {target_database}.{target_table} a
            WHERE query_id = {query_id}
            AND cadence = '{cadence}'
            AND from_date BETWEEN '{min_from_date}' AND '{max_from_date}'
            AND to_date BETWEEN '{min_to_date}' AND '{max_to_date}'
        """.format(  # nosec: B608
            target_database=self.target_database,
            target_table=self.target_table,
            query_id=self.query_id,
            cadence=self.cadence,
            min_from_date=min_from_date,
            max_from_date=max_from_date,
            min_to_date=min_to_date,
            max_to_date=max_to_date,
        )

        return gen_del
