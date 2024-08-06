"""Utils for dealing with DQ Rules tables."""

from lakehouse_engine.core.exec_env import ExecEnv
from tests.utils.local_storage import LocalStorage


def _create_dq_functions_source_table(
    test_resources_path: str,
    lakehouse_in_path: str,
    lakehouse_out_path: str,
    test_name: str,
    scenario: str,
    table_name: str,
) -> None:
    """Create test dq functions source table.

    Args:
        test_resources_path: path to the test resources.
        lakehouse_in_path: path to the lakehouse in.
        lakehouse_out_path: path to the lakehouse out.
        test_name: name of the test.
        scenario: name of the test scenario.
        table_name: name of the test table.
    """
    LocalStorage.copy_file(
        f"{test_resources_path}/{test_name}/data/dq_functions/{table_name}.csv",
        f"{lakehouse_in_path}/{test_name}/{scenario}/dq_functions/",
    )

    ExecEnv.SESSION.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            dq_rule_id STRING,
            dq_check_type STRING,
            dq_tech_function STRING,
            execution_point STRING,
            schema STRING,
            table STRING,
            column STRING,
            filters STRING,
            arguments STRING,
            expected_technical_expression STRING,
            dimension STRING
        )
        USING delta
        LOCATION '{lakehouse_out_path}/{test_name}/{scenario}/dq_functions'
        TBLPROPERTIES(
          'lakehouse.primary_key'='dq_rule_id',
          'delta.enableChangeDataFeed'='false'
        )
        """
    )
    dq_functions = (
        ExecEnv.SESSION.read.option("delimiter", "|")
        .option("header", True)
        .csv(
            f"{lakehouse_in_path}/{test_name}/{scenario}/dq_functions/{table_name}.csv"
        )
    )

    dq_functions.write.saveAsTable(
        name=f"{table_name}", format="delta", mode="overwrite"
    )
