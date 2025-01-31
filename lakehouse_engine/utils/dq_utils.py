"""Module containing utils for DQ processing."""

from json import loads

from pyspark.sql.functions import col, from_json, schema_of_json, struct

from lakehouse_engine.core.definitions import DQSpec, DQTableBaseParameters, DQType
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.dq_processors.exceptions import DQSpecMalformedException
from lakehouse_engine.utils.logging_handler import LoggingHandler

_LOGGER = LoggingHandler(__name__).get_logger()


class DQUtils:
    """Utils related to the data quality process."""

    @staticmethod
    def import_dq_rules_from_table(
        spec: dict,
        execution_point: str,
        base_expectation_arguments: list,
        extra_meta_arguments: list,
    ) -> dict:
        """Import dq rules from a table.

        Args:
            spec: data quality specification.
            execution_point: if the execution is in_motion or at_rest.
            base_expectation_arguments: base arguments for dq functions.
            extra_meta_arguments: extra meta arguments for dq functions.

        Returns:
            The dictionary containing the dq spec with dq functions defined.
        """
        dq_db_table = spec["dq_db_table"]
        dq_functions = []

        if spec.get("dq_table_table_filter"):
            dq_table_table_filter = spec["dq_table_table_filter"]
        else:
            raise DQSpecMalformedException(
                "When importing rules from a table "
                "dq_table_table_filter must be defined."
            )

        extra_filters_query = (
            f""" and {spec["dq_table_extra_filters"]}"""
            if spec.get("dq_table_extra_filters")
            else ""
        )

        fields = base_expectation_arguments + extra_meta_arguments

        dq_functions_query = f"""
            SELECT {", ".join(fields)}
            FROM {dq_db_table}
            WHERE
            execution_point='{execution_point}' and table = '{dq_table_table_filter}'
            {extra_filters_query}"""  # nosec: B608

        raw_dq_functions = ExecEnv.SESSION.sql(dq_functions_query)

        arguments = raw_dq_functions.select("arguments").collect()
        parsed_arguments = [loads(argument.arguments) for argument in arguments]
        combined_dict: dict = {}

        for argument in parsed_arguments:
            combined_dict = {**combined_dict, **argument}

        dq_function_arguments_schema = schema_of_json(str(combined_dict))

        processed_dq_functions = (
            raw_dq_functions.withColumn(
                "json_data", from_json(col("arguments"), dq_function_arguments_schema)
            )
            .withColumn(
                "parsed_arguments",
                struct(
                    col("json_data.*"),
                    struct(extra_meta_arguments).alias("meta"),
                ),
            )
            .drop(col("json_data"))
        )

        unique_dq_functions = processed_dq_functions.drop_duplicates(
            ["dq_tech_function", "arguments"]
        )

        duplicated_rows = processed_dq_functions.subtract(unique_dq_functions)

        if duplicated_rows.count() > 0:
            _LOGGER.warning("Found Duplicates Rows:")
            duplicated_rows.show(truncate=False)

        processed_dq_functions_list = unique_dq_functions.collect()
        for processed_dq_function in processed_dq_functions_list:
            dq_functions.append(
                {
                    "function": f"{processed_dq_function.dq_tech_function}",
                    "args": {
                        k: v
                        for k, v in processed_dq_function.parsed_arguments.asDict(
                            recursive=True
                        ).items()
                        if v is not None
                    },
                }
            )

        spec["dq_functions"] = dq_functions

        return spec

    @staticmethod
    def validate_dq_functions(
        spec: dict, execution_point: str = "", extra_meta_arguments: list = None
    ) -> None:
        """Function to validate the dq functions defined in the dq_spec.

        This function validates that the defined dq_functions contain all
        the fields defined in the extra_meta_arguments parameter.

        Args:
            spec: data quality specification.
            execution_point: if the execution is in_motion or at_rest.
            extra_meta_arguments: extra meta arguments for dq functions.

        Raises:
            DQSpecMalformedException: If the dq spec is malformed.
        """
        dq_functions = spec["dq_functions"]
        if not extra_meta_arguments:
            _LOGGER.info(
                "No extra meta parameters defined. "
                "Skipping validation of imported dq rule."
            )
            return

        for dq_function in dq_functions:
            if not dq_function.get("args").get("meta", None):
                raise DQSpecMalformedException(
                    "The dq function must have a meta field containing all "
                    f"the fields defined: {extra_meta_arguments}."
                )
            else:

                meta = dq_function["args"]["meta"]
                given_keys = meta.keys()
                missing_keys = sorted(set(extra_meta_arguments) - set(given_keys))
                if missing_keys:
                    raise DQSpecMalformedException(
                        "The dq function meta field must contain all the "
                        f"fields defined: {extra_meta_arguments}.\n"
                        f"Found fields: {list(given_keys)}.\n"
                        f"Diff: {list(missing_keys)}"
                    )
                if execution_point and meta["execution_point"] != execution_point:
                    raise DQSpecMalformedException(
                        "The dq function execution point must be the same as "
                        "the execution point of the dq spec."
                    )


class PrismaUtils:
    """Prisma related utils."""

    @staticmethod
    def build_prisma_dq_spec(spec: dict, execution_point: str) -> dict:
        """Fetch dq functions from given table.

        Args:
            spec: data quality specification.
            execution_point: if the execution is in_motion or at_rest.

        Returns:
            The dictionary containing the dq spec with dq functions defined.
        """
        if spec.get("dq_db_table"):
            spec = DQUtils.import_dq_rules_from_table(
                spec,
                execution_point,
                DQTableBaseParameters.PRISMA_BASE_PARAMETERS.value,
                ExecEnv.ENGINE_CONFIG.dq_functions_column_list,
            )
        elif spec.get("dq_functions"):
            DQUtils.validate_dq_functions(
                spec,
                execution_point,
                ExecEnv.ENGINE_CONFIG.dq_functions_column_list,
            )
        else:
            raise DQSpecMalformedException(
                "When using PRISMA either dq_db_table or "
                "dq_functions needs to be defined."
            )

        dq_bucket = (
            ExecEnv.ENGINE_CONFIG.dq_bucket
            if ExecEnv.get_environment() == "prod"
            else ExecEnv.ENGINE_CONFIG.dq_dev_bucket
        )

        spec["critical_functions"] = []
        spec["execution_point"] = execution_point
        spec["result_sink_db_table"] = None
        spec["result_sink_explode"] = True
        spec["fail_on_error"] = spec.get("fail_on_error", False)
        spec["max_percentage_failure"] = spec.get("max_percentage_failure", 1)

        if not spec.get("result_sink_extra_columns", None):
            spec["result_sink_extra_columns"] = [
                "validation_results.expectation_config.meta",
            ]
        else:
            spec["result_sink_extra_columns"] = [
                "validation_results.expectation_config.meta",
            ] + spec["result_sink_extra_columns"]
        if not spec.get("data_product_name", None):
            raise DQSpecMalformedException(
                "When using PRISMA DQ data_product_name must be defined."
            )
        spec["result_sink_location"] = (
            f"{dq_bucket}/{spec['data_product_name']}/result_sink/"
        )
        if not spec.get("tbl_to_derive_pk", None) and not spec.get(
            "unexpected_rows_pk", None
        ):
            raise DQSpecMalformedException(
                "When using PRISMA DQ either "
                "tbl_to_derive_pk or unexpected_rows_pk need to be defined."
            )
        return spec

    @staticmethod
    def validate_rule_id_duplication(
        specs: list[DQSpec],
    ) -> dict[str, str]:
        """Verify uniqueness of the dq_rule_id.

        Args:
            specs: a list of DQSpec to be validated

        Returns:
             A dictionary with the spec_id as key and
             rule_id as value for any duplicates.
        """
        error_dict = {}

        for spec in specs:
            dq_db_table = spec.dq_db_table
            dq_functions = spec.dq_functions
            spec_id = spec.spec_id

            if spec.dq_type == DQType.PRISMA.value and dq_db_table:
                dq_rule_id_query = f"""
                    SELECT dq_rule_id, COUNT(*) AS count
                    FROM {dq_db_table}
                    GROUP BY dq_rule_id
                    HAVING COUNT(*) > 1;
                    """  # nosec: B608

                duplicate_rule_id_table = ExecEnv.SESSION.sql(dq_rule_id_query)

                if not duplicate_rule_id_table.isEmpty():
                    rows = duplicate_rule_id_table.collect()
                    df_str = "; ".join([str(row) for row in rows])
                    error_dict[f"dq_spec_id: {spec_id}"] = df_str

            elif spec.dq_type == DQType.PRISMA.value and dq_functions:
                dq_rules_id_list = []
                for dq_function in dq_functions:
                    dq_rules_id_list.append(dq_function.args["meta"]["dq_rule_id"])

                if len(dq_rules_id_list) != len(set(dq_rules_id_list)):
                    error_dict[f"dq_spec_id: {spec_id}"] = "; ".join(
                        [str(dq_rule_id) for dq_rule_id in dq_rules_id_list]
                    )

        return error_dict
