# Databricks notebook source
import json
import re

from databricks.sdk.runtime import *


class QueryBuilderUtils:
    """Class with methods to create GAB use case configuration."""

    def __init__(self):
        """Instantiate objects of the class QueryBuilderUtils."""
        self.regex_no_special_characters = "^[a-zA-Z0-9]+(_[a-zA-Z0-9]+)*$"
        self.cadences = ["DAY", "WEEK", "MONTH", "QUARTER", "YEAR"]

    def check_config_inputs(
            self,
            usecase_name: str,
            from_date: str,
            num_dimensions: str,
            sql_files: str,
            num_of_views: str,
            to_date: str,
            time_offset: str,
            db_schema: str
    ) -> str:
        """
        Check the parameters input.

        Args:
            usecase_name: The use case name.
            from_date: The reference date of the use case.
            num_dimensions: The number of dimensions chosen for analysis.
            sql_files: Name of the SQL files that will be submitted for the framework
                to process (e.g. file1.sql, file2.sql).
            num_of_views: Number of views the use case has.
            to_date: The end date of the snapshot configuration.
            time_offset: Hours related to the timezone (e.g. 8, -8).
            db_schema: Database name that lkp_query_builder is located.

        Returns:
            A message with the status of the validation.
        """
        message = ""
        if (
                usecase_name.strip() == ""
                or from_date.strip() == ""
                or num_dimensions.strip() == ""
                or sql_files.strip() == ""
                or num_of_views.strip() == ""
                or to_date.strip() == ""
                or db_schema.strip() == ""
        ):
            message = "WRONG CONFIGURATION:"
            if usecase_name.strip() == "":
                message += "\n\t - Please, add the Use Case Name."
            if from_date.strip() == "":
                message += "\n\t - Please, add the From Date."
            if num_dimensions.strip() == "":
                message += "\n\t - Please, add the Number of Dimensions."
            if sql_files.strip() == "":
                message += "\n\t - Please, add the SQL File Names."
            if num_of_views.strip() == "":
                message += "\n\t - Please, add the number of views."
            if to_date.strip() == "":
                message += "\n\t - Please, add the to date value. This information is mandatory. "
                message += "Keep it as 'to_date' unless you change its name in your SQL files."
            if db_schema.strip() == "":
                message += "\n\t - Please, add the database schema where the lkp_query_builder table is located."

        if time_offset.strip():
            try:
                int(re.findall('-?\d+\.?\d*',time_offset.strip())[0])
            except Exception:
                if message:
                    message += "\n\t The timezone offset must be a number (e.g. 0, 12 or -8)."
                else:
                    message = "WRONG CONFIGURATION:"
                    message += "\n\t - The timezone offset must be a number (e.g. 0, 12 or -8)."

        if num_dimensions.strip():
            try:
                int(num_dimensions)
                if int(num_dimensions) == 0:
                    message = "WRONG CONFIGURATION:"
                    message += "\n\t - The number of dimensions must be greater than zero."
            except Exception:
                if message:
                    message += "\n\t - The number of dimensions must be an integer."
                else:
                    message = "WRONG CONFIGURATION:"
                    message += "\n\t - The number of dimensions must be an integer."

        if sql_files.strip():
            files_list = self._sort_files(sql_files)
            for file in files_list:
                sql_files_err = f"""\n\t - Check the SQL file name '{file}'. """
                sql_files_err += "It must follow the pattern x_file_name (X is an integer digit)." ""
                try:
                    int(re.match("(.*?)_", file).group()[:-1])
                except Exception:
                    if message:
                        message += sql_files_err
                    else:
                        message = "WRONG CONFIGURATION:"
                        message += sql_files_err
        if not message:
            message = "Validation status: OK"

        return print(message)

    def create_sql_statement(
            self,
            usecase_name: str,
            market: str,
            stages_dict: dict,
            recon_dict: dict,
            time_offset: str,
            week_start: str,
            is_active: str,
            complexity: str,
            db_schema: str,
            dims_dict: dict,
            dimensions: str,
            from_date: str,
            to_date: str,
            metrics_dict: dict,
    ) -> tuple[str, str]:
        """
        Create the SQL statement to insert data into lkp_query_builder_table.

        Args:
            usecase_name: The name of use case.
            market: The market used for the use case (APAC, GLOBAL, NAM, NIGHTLY).
            stages_dict: A dictionary of stages and it's configurations.
            recon_dict: A dictionary of reconciliation setup.
            time_offset: Hours related to the timezone (e.g. 8, -8).
            week_start: Day of the start of the week (e.g. Sunday, Monday)
            is_active: If the use case is active or not. (e.g. Y, N)
            complexity: The categories are directly related to the number of workers in each cluster.
                That is, High = 10 workers, Medium = 6 workers and Low = 4 workers.
            db_schema: Database name that lkp_query_builder is located.
            dims_dict: The dictionary of views and it's setup.
            dimensions: Store supporting information to the fact table.
            from_date: Aggregating date column for the use case.
            to_date: Contains the current date (default value is to_date).
                Information used as template for the framework.
            metrics_dict: The dictionary of metrics and it's setup.

        Returns:
            A tuple with a text formatted with the delete and insert statement.

        """
        dbutils.widgets.removeAll()

        mapping_dict = self._get_mapping(dims_dict, dimensions, from_date, to_date, metrics_dict)

        query_id = self._generate_query_id(usecase_name)
        query_label = f"'{usecase_name}'"
        query_type = f"'{market}'"
        mapping_str = json.dumps(mapping_dict, indent=4)
        mappings = '"""' + mapping_str.replace('"', "'").replace("#+#-#", '\\"') + '"""'
        steps_str = json.dumps(stages_dict, indent=4)
        intermediate_stages = '"""' + steps_str.replace('"', "'") + '"""'
        recon_str = json.dumps(recon_dict)
        recon_window = '"""' + recon_str.replace('"', "'") + '"""'
        col_time_offset = f"'{time_offset}'"
        start_of_week = f"'{week_start}'"
        col_is_active = f"'{is_active}'"
        queue = f"'{complexity}'"

        delete_sttmt = f"""DELETE FROM {db_schema}.lkp_query_builder WHERE QUERY_LABEL = {query_label};"""
        insert_sttmt = f"""INSERT INTO {db_schema}.lkp_query_builder VALUES (
            {query_id},
            {query_label},
            {query_type},
            {mappings},
            {intermediate_stages},
            {recon_window},
            {col_time_offset},
            {start_of_week},
            {col_is_active},
            {queue},
            current_timestamp());"""

        return delete_sttmt, insert_sttmt

    def get_dimensions(self, num_dimensions: str) -> str:
        """
        Get the dimensions set on the widgets and validate.

        Args:
            num_dimensions: The number of dimensions set.

        Returns:
            A string with comma-separated dimensions names.

        """
        dimensions = ""
        list_status = []
        for i in range(int(num_dimensions)):
            i = i + 1
            if re.match(self.regex_no_special_characters, dbutils.widgets.get(f"D{i}").strip()):
                dimensions += "," + dbutils.widgets.get(f"D{i}").strip()
                list_status.append("success")
            else:
                print("WRONG CONFIGURATION:")
                print(f"\t- {dbutils.widgets.get(f'D{i}')} is empty of malformed!")
                print(
                    "\t Names can contain only alphanumeric characters and must begin with "
                    "an alphabetic character or an underscore (_)."
                )
                list_status.append("fail")
        if "fail" not in list_status:
            print("Dimensions validation status: OK")
            return dimensions[1:]

    @classmethod
    def get_recon_choices(cls) -> list:
        """
        Return all possible combinations for cadences, reconciliations and the snapshot flag value (Y,N).

        Returns:
            List used to generate a multiselect widget for the users to interact with.

        """
        return [
            "DAY",
            "DAY-WEEK-N",
            "DAY-MONTH-N",
            "DAY-QUARTER-N",
            "DAY-YEAR-N",
            "WEEK",
            "WEEK-DAY-N",
            "WEEK-DAY-Y",
            "WEEK-MONTH-N",
            "WEEK-QUARTER-N",
            "WEEK-YEAR-N",
            "MONTH",
            "MONTH-DAY-N",
            "MONTH-DAY-Y",
            "MONTH-WEEK-Y",
            "MONTH-WEEK-N",
            "MONTH-QUARTER-N",
            "MONTH-YEAR-N",
            "QUARTER",
            "QUARTER-DAY-N",
            "QUARTER-DAY-Y",
            "QUARTER-WEEK-N",
            "QUARTER-WEEK-Y",
            "QUARTER-MONTH-N",
            "QUARTER-MONTH-Y",
            "QUARTER-YEAR-N",
            "YEAR",
            "YEAR-DAY-N",
            "YEAR-DAY-Y",
            "YEAR-WEEK-N",
            "YEAR-WEEK-Y",
            "YEAR-MONTH-N",
            "YEAR-MONTH-Y",
            "YEAR-QUARTER-N",
            "YEAR-QUARTER-Y",
        ]

    @classmethod
    def get_metric_configuration(cls, num_of_metrics: str) -> dict:
        """
        Get metrics information based on the widget setup.

        Args:
            num_of_metrics: Number of metrics selected.

        Returns:
            metrics_dict: The dictionary of metrics and their setup.

        """
        metrics_dict = {}
        for i in range(int(num_of_metrics)):
            i = i + 1
            if dbutils.widgets.get(f"metric_name{i}"):
                metrics_dict[f"m{i}"] = {
                    "metric_name": dbutils.widgets.get(f"metric_name{i}"),
                    "calculated_metric": {},
                    "derived_metric": {},
                }
                calculated_metric_list = list(filter(None, dbutils.widgets.get(f"calculated_metric{i}").split(",")))
                for calc_metric in calculated_metric_list:
                    if calc_metric == "last_cadence":
                        metrics_dict[f"m{i}"]["calculated_metric"].update({calc_metric: {}})
                        # add label and window for last_cadence
                        dbutils.widgets.text(
                            name=f"{i}_{calc_metric}_label", defaultValue="", label=f"{i}_{calc_metric}.Label"
                        )
                        dbutils.widgets.text(
                            name=f"{i}_{calc_metric}_window", defaultValue="", label=f"{i}_{calc_metric}.Window"
                        )
                    if calc_metric == "last_year_cadence":
                        metrics_dict[f"m{i}"]["calculated_metric"].update({calc_metric: {}})
                        # add label and window for last_cadence
                        dbutils.widgets.text(
                            name=f"{i}_{calc_metric}_label", defaultValue="", label=f"{i}_{calc_metric}.Label"
                        )
                    if calc_metric == "window_function":
                        metrics_dict[f"m{i}"]["calculated_metric"].update({calc_metric: {}})
                        # add label and window for window_function
                        dbutils.widgets.text(
                            name=f"{i}_{calc_metric}_label", defaultValue="", label=f"{i}_{calc_metric}.Label"
                        )
                        dbutils.widgets.text(
                            name=f"{i}_{calc_metric}_window",
                            defaultValue="",
                            label=f"{i}_{calc_metric}.Window Interval",
                        )
                        dbutils.widgets.dropdown(
                            name=f"{i}_{calc_metric}_agg_func",
                            defaultValue="sum",
                            label=f"{i}_{calc_metric}.Agg Func",
                            choices=["sum", "avg", "max", "min", "count"],
                        )
                    # add label and window for derived_metric
                    if calc_metric == "derived_metric":
                        dbutils.widgets.text(
                            name=f"{i}_{calc_metric}_label", defaultValue="", label=f"{i}_{calc_metric}.Label"
                        )
                        dbutils.widgets.text(
                            name=f"{i}_{calc_metric}_formula", defaultValue="", label=f"{i}_{calc_metric}.Formula"
                        )
                print("Metric configuration status: OK")
            else:
                print("WRONG CONFIGURATION:")
                print("\t- The metric name is mandatory!")

        return metrics_dict

    def get_recon_config(self, recon_list: list) -> dict:
        """
        Get reconciliation information based on the widget setup.

        Args:
            recon_list: List of cadences setup for the reconciliation.

        Returns:
            A dictionary of reconciliation setup.

        """
        cadence_list = []
        # create a list with the distinct cadences values.
        for cadence in recon_list:
            cadence_name = cadence.split("-")[0]
            cadence_list.append(cadence_name)

        cadence_list = list(dict.fromkeys(cadence_list))

        # create a dict with the structure of each cadence.
        recon_dict = {}
        for cad in cadence_list:
            recon_dict[f"{cad}"] = {}
            recon_dict[f"{cad}"]["recon_window"] = {}

        # updates the dict of each cadence with the recon configurations selected.
        for cadence in recon_list:
            if cadence in self.cadences:
                recon_dict[f"{cad}"]["recon_window"] = {}
            else:
                cadence_name = cadence.split("-")[0]
                recon = cadence.split("-")[1]
                snapshot = cadence.split("-")[2]
                for cad in cadence_list:
                    if cadence_name == cad:
                        recon_dict[cad]["recon_window"].update({recon: {"snapshot": snapshot}})

        # remove empty recon_window when the selected just cadence.
        for cadence in recon_list:
            if cadence in ["DAY", "WEEK", "MONTH", "QUARTER", "YEAR"]:
                if recon_dict[f"{cadence}"]["recon_window"] == {}:
                    del recon_dict[f"{cadence}"]["recon_window"]

        if recon_dict:
            print("Reconciliation configuration status: OK")
        else:
            print("WRONG CONFIGURATION:")
            print("\t- The recon information is mandatory!")
        return recon_dict

    def get_stages(self, sql_files_list: list, usecase_name: str) -> dict:
        """
        Set stages based on the widget setup.

        Args:
            sql_files_list: A list of sql files and their setup.
            usecase_name: The use case name.

        Returns:
            stages_dict: A dictionary of stages and their setup.

        """
        stages_dict = {}
        i = 0
        list_status = []
        for file in sql_files_list:
            i = i + 1
            if dbutils.widgets.get(name=f"{i}_script_table_alias"):
                stages_dict[f"{i}"] = {
                    "file_path": usecase_name + "/" + file.strip() + ".sql",
                    "table_alias": dbutils.widgets.get(name=f"{i}_script_table_alias"),
                    "storage_level": dbutils.widgets.get(name=f"{i}_script_storage_level"),
                    "project_date_column": dbutils.widgets.get(name=f"{i}_script_project_dt_col"),
                    "filter_date_column": dbutils.widgets.get(name=f"{i}_script_filter_dt_col"),
                }

                repartition_value = self._format_keys_list(dbutils.widgets.get(name=f"{i}_script_repartition_value"))

                stages_dict[f"{i}"]["repartition"] = {}
                if dbutils.widgets.get(name=f"{i}_script_repartition_type") == "NUMBER":
                    try:
                        int(dbutils.widgets.get(name=f"{i}_script_repartition_value").split(",")[0])
                        stages_dict[f"{i}"]["repartition"] = {
                            "numPartitions": dbutils.widgets.get(name=f"{i}_script_repartition_value")
                            .split(",")[0]
                            .replace("'", "")
                        }
                    except Exception:
                        print("The repartition value must be INTEGER when the type is defined as NUMBER.")
                        list_status.append("fail")

                elif dbutils.widgets.get(name=f"{i}_script_repartition_type") == "KEY":
                    stages_dict[f"{i}"]["repartition"] = {"keys": repartition_value}
            else:
                print(f"The field script alias is missing for {i}.Script Table Alias. This field is mandatory!")
                stages_dict = {}
                list_status.append("fail")

        if "fail" not in list_status:
            print("Stages configuration status: OK")
        return stages_dict

    def get_view_information(self, num_of_views: str) -> dict:
        """
        Get the views information based on the widget setup.

        Args:
            num_of_views: Number of views selected.

        Returns:
            The dictionary of views and their setup.

        """
        dims_dict = {}
        for i in range(int(num_of_views)):
            i = i + 1
            if re.match(self.regex_no_special_characters, dbutils.widgets.get(f"view_name{i}")):
                dims_dict[f"view_name{i}"] = {
                    "name": dbutils.widgets.get(f"view_name{i}"),
                    "filter": dbutils.widgets.get(f"view_filter{i}").replace("'", "#+#-#").replace('"', "#+#-#"),
                }
                print("Views validation status: OK")
            else:
                print("WRONG CONFIGURATION:")
                print("\t- View name is empty of malformed!")
                print(
                    "\t Names can contain only alphanumeric characters and must begin with "
                    "an alphabetic character or an underscore (_)."
                )
        return dims_dict

    @classmethod
    def insert_data_into_lkp_query_builder(cls, delete_sttmt: str, insert_sttmt: str):
        """
        Insert data into the lkp query builder table.

        Args:
            delete_sttmt: The delete statement.
            insert_sttmt: The insert statement.

        """
        try:
            spark.sql(f"{delete_sttmt}")
            spark.sql(f"{insert_sttmt}")
            print("CONFIGURATION INSERTED SUCCESSFULLY!")
        except Exception as e:
            print(e)

    def print_definitions(
            self,
            usecase_name,
            market,
            from_date,
            to_date,
            dimensions,
            time_offset,
            week_start,
            is_active,
            num_of_views,
            complexity,
            sql_files,
            db_schema,
            dims_dict: dict = None,
            recon_dict: dict = None,
            metrics_dict: dict = None,
            stages_dict: dict = None,
    ):
        """
        Print the definitions set on widgets.

        Args:
            usecase_name: The name of use case.
            market: The market used for the use case (APAC, GLOBAL, NAM, NIGHTLY).
            from_date: Aggregating date column for the use case.
            to_date: Contains the current date (default value is to_date).
                Information used as template for the framework.
            dimensions: Store supporting information to the fact table
            time_offset: Hours related to the timezone (e.g. 8, -8).
            week_start: Day of the start of the week (e.g. Sunday, Monday)
            is_active: If the use case is active or not. (e.g. Y, N)
            num_of_views: Number of views desired for the use case (e.g. 1, 2, 3).
            complexity: The categories are directly related to the number of workers in each cluster.
            That is, High = 10 workers, Medium = 6 workers and Low = 4 workers
            sql_files: Name of the SQL files that will be submitted for the framework
            to process (e.g. file1.sql, file2.sql).
            Database name that lkp_query_builder is located.
            dims_dict: A dictionary of dimensions.
            recon_dict: A dictionary of reconciliation setup.
            metrics_dict: The dictionary of metrics and their setup.
            stages_dict: A dictionary of stages and their setup.

        """
        print("USE CASE DEFINITIONS:")
        print("Use Case Name:", usecase_name)
        print("Market:", market)
        print("From Date:", from_date)
        print("To Date:", to_date)
        print("Dimensions:", dimensions)
        print("Time Offset:", time_offset)
        print("Week Start:", week_start)
        print("Is Active:", is_active)
        print("How many views?", num_of_views)
        print("Complexity:", complexity)
        print("SQL Files:", sql_files)
        print("Database Schema Name:", db_schema)
        self._print_dims_dict(dims_dict)
        self._print_recon_dict(recon_dict)
        if metrics_dict:
            print("METRICS CONFIGURED:")
            for key_metrics in metrics_dict:
                self._print_metrics_dict(key_metrics, metrics_dict)
        self._print_stages_dict(stages_dict)

    @classmethod
    def set_dimensions(cls, num_dimensions: str):
        """
        Set the dimension mappings based on the widget setup.

        Args:
            num_dimensions: Number of dimensions selected.

        """
        dbutils.widgets.removeAll()

        for i in range(int(num_dimensions)):
            i = i + 1
            dbutils.widgets.text(name=f"D{i}", defaultValue="", label=f"D{i}.Dimension Name")
        print("Please, configure the dimensions using the widgets and proceed to the next cmd.")

    def set_extra_metric_config(self, num_of_metrics: str, metrics_dict: dict):
        """
        Set extra metrics information based on the widget setup.

        Args:
            num_of_metrics: Number of metrics selected.

        """
        for i in range(int(num_of_metrics)):
            i = i + 1
            calculated_metric_list = list(filter(None, dbutils.widgets.get(f"calculated_metric{i}").split(",")))
            if calculated_metric_list:
                for calc_metric in calculated_metric_list:
                    self._validate_metrics_config(calc_metric, metrics_dict, i)
            else:
                print("Extra metrics configuration status: OK")

    @classmethod
    def set_metric(cls, num_of_metrics: str):
        """
        Set metrics information based on the widget setup.

        Args:
            num_of_metrics: Number of metrics selected.

        """
        dbutils.widgets.removeAll()
        for i in range(1, int(num_of_metrics) + 1):
            dbutils.widgets.text(name=f"metric_name{i}", defaultValue="", label=f"{i}.Metric Name")
            dbutils.widgets.multiselect(
                name=f"calculated_metric{i}",
                defaultValue="",
                label=f"{i}.Calculated Metric",
                choices=["", "last_cadence", "last_year_cadence", "window_function", "derived_metric"],
            )
        print("Please, configure the metrics using the widgets and proceed to the next cmd.")

    def set_stages(self, sql_files: list) -> list:
        """
        Set stages based on the widget setup.

        Args:
            sql_files: The SQL file names that will be used in the use case.

        Returns:
            sql_files_list: A list of sql files and their setup.

        """
        dbutils.widgets.removeAll()
        sql_files_list = self._sort_files(sql_files)

        for i in range(1, len(sql_files_list) + 1):
            dbutils.widgets.dropdown(
                name=f"{i}_script_storage_level",
                defaultValue="MEMORY_ONLY",
                label=f"{i}.Storage Level",
                choices=[
                    "DISK_ONLY",
                    "DISK_ONLY_2",
                    "DISK_ONLY_3",
                    "MEMORY_AND_DISK",
                    "MEMORY_AND_DISK_2",
                    "MEMORY_AND_DISK_DESER",
                    "MEMORY_ONLY",
                    "MEMORY_ONLY_2",
                    "OFF_HEAP",
                ],
            )
            dbutils.widgets.text(name=f"{i}_script_table_alias", defaultValue="", label=f"{i}.Table Alias")
            dbutils.widgets.text(name=f"{i}_script_project_dt_col", defaultValue="", label=f"{i}.Project Date Column")
            dbutils.widgets.text(name=f"{i}_script_filter_dt_col", defaultValue="", label=f"{i}.Filter Date Column")
            dbutils.widgets.dropdown(
                name=f"{i}_script_repartition_type",
                defaultValue="",
                label=f"{i}.Repartition Type",
                choices=["", "KEY", "NUMBER"],
            )
            dbutils.widgets.text(name=f"{i}_script_repartition_value", defaultValue="", label=f"{i}.Repartition Value")

        print("Please, configure the stages using the widgets and proceed to the next cmd.")
        return sql_files_list

    @classmethod
    def set_views(cls, num_of_views: str):
        """
        Set views that will be used in the use case.

        Args:
            num_of_views: Number of views selected.

        """
        dbutils.widgets.removeAll()

        for i in range(1, int(num_of_views) + 1):
            dbutils.widgets.text(name=f"view_name{i}", defaultValue="", label=f"{i}.View Name")
            dbutils.widgets.text(name=f"view_filter{i}", defaultValue="", label=f"{i}.View Filter")

        print("Please, configure the views using the widgets and proceed to the next cmd.")

    @classmethod
    def _format_keys_list(cls, key_str: str) -> list:
        """
        Format the list of keys based on the widget keys data provided.

        Args:
            key_str: Input text with key column names.

        Returns:
            A formatted list with the keys selected for repartitioning.

        """
        key_list = key_str.strip().split(",")
        output_list = []
        for key in key_list:
            output_list.append(key.replace("'", "").replace('"', "").strip())
        return output_list

    @classmethod
    def _generate_query_id(cls, usecase_name: str) -> int:
        """
        Generate the query id for the lookup query builder table.

        The logic to create the ID is a hash of the use case name converted to an integer.

        Args:
            usecase_name: The name of use case.

        Returns:
            The use case name hashed.

        """
        hash_val = int(str(hash(usecase_name))[0:9])
        return hash_val if hash_val > 0 else hash_val * -1

    @classmethod
    def _get_mapping(cls, dims_dict: dict, dimensions: str, from_date: str, to_date: str, metrics_dict: dict) -> dict:
        """
        Get mappings based on the dimensions defined on the widget setup.

        Args:
            dims_dict: A dictionary of dimensions.
            dimensions: Store supporting information to the fact table.
            from_date: Aggregating date column for the use case.
            to_date: Contains the current date (default value is to_date).
            Information used as template for the framework.
            metrics_dict: The dictionary of metrics and their setup.

        Returns:
            mapping_dict: A dictionary of mappings configuration.

        """
        mapping_dict = {}
        for key in dims_dict:
            mapping_dict.update({dims_dict[key]["name"]: {"dimensions": {}, "metric": {}, "filter": {}}})
            i = 0
            for d in dimensions.split(","):
                i = i + 1
                mapping_dict[dims_dict[key]["name"]]["dimensions"].update(
                    {"from_date": from_date, "to_date": to_date, f"d{i}": d.strip()}
                )
                mapping_dict[dims_dict[key]["name"]]["metric"].update(metrics_dict)
                if dims_dict[key]["filter"]:
                    mapping_dict[dims_dict[key]["name"]]["filter"] = dims_dict[key]["filter"]

        return mapping_dict

    @classmethod
    def _print_dims_dict(cls, dims_dict: dict):
        """
        Print the dictionary of dimensions and views formatted.

        Args:
            dims_dict: The dictionary of views and their setup.
        """
        if dims_dict:
            print("VIEWS CONFIGURED:")
            for key in dims_dict:
                print(f"{key}:")
                keys = [k for k, v in dims_dict[key].items()]
                for k in keys:
                    print(f"\t{k}:", dims_dict[key][k].replace("#+#-#", '"'))

    @classmethod
    def _print_derived_metrics(cls, key_metrics: str, derived_metric: str, metrics_dict: dict):
        """
        Print the derived dict formatted.

        Args:
            key_metrics: The key name of each metric configured (e.g. m1, m2, m3).
            derived_metric: The name of the derived metric configuration (e.g. last_cadence, last_year_cadence,
                            derived_metric, window_function).
            metrics_dict: The dictionary of metrics and their setup.
        """
        if derived_metric == "derived_metric":
            if metrics_dict[key_metrics][derived_metric]:
                print(f"\t- {derived_metric}:")
                derived_metric_val_list = [k for k, v in metrics_dict[key_metrics][derived_metric][0].items()]
                for derived_metric_val in derived_metric_val_list:
                    print(
                        f"\t  - {derived_metric_val} = "
                        f"{metrics_dict[key_metrics][derived_metric][0][derived_metric_val]}"
                    )

    def _print_metrics_dict(self, key_metrics: str, metrics_dict: dict):
        """
        Print the metrics configured formatted.

        Args:
            key_metrics: The key name of each metric configured (e.g. m1, m2, m3).
            metrics_dict: The dictionary of metrics and their setup.
        """
        print(f"{key_metrics}:")
        list_key_metrics = [k for k, v in metrics_dict[key_metrics].items()]
        if list_key_metrics:
            for metric in list_key_metrics:
                if metric == "metric_name":
                    print(f"  {metric} = {metrics_dict[key_metrics][metric]}")
                else:
                    for derived_metric in metrics_dict[key_metrics][metric]:
                        if derived_metric in ["last_cadence", "last_year_cadence", "window_function"]:
                            print(f"\t- {derived_metric}:")
                            derived_metric_val_list = [
                                k for k, v in metrics_dict[key_metrics][metric][derived_metric][0].items()
                            ]
                            for derived_metric_val in derived_metric_val_list:
                                print(
                                    f"\t  - {derived_metric_val} = "
                                    f"{metrics_dict[key_metrics][metric][derived_metric][0][derived_metric_val]}"
                                )
                        else:
                            self._print_derived_metrics(key_metrics, metric, metrics_dict)

    @classmethod
    def _print_recon_dict(cls, recon_dict: dict):
        """
        Print the recon dict formatted.

        Args:
            recon_dict: A dictionary of reconciliation setup.
        """
        if recon_dict:
            print("RECON CONFIGURED:")
            for key_cadence in recon_dict:
                if recon_dict[f"{key_cadence}"] == {}:
                    print(f"{key_cadence}")
                else:
                    print(f"{key_cadence}:")
                keys_recon = [k for k, v in recon_dict[key_cadence].items()]
                if keys_recon:
                    for k_recon in keys_recon:
                        print(f"  {k_recon}:")
                        keys_recon = [k for k, v in recon_dict[key_cadence][k_recon].items()]
                        for recon_val in keys_recon:
                            print(
                                f"\t- {recon_val}:snapshot = {recon_dict[key_cadence][k_recon][recon_val]['snapshot']}"
                            )

    @classmethod
    def _print_stages_dict(cls, stages_dict: dict):
        """
        Print the dictionary of stages formatted.

        Args:
            stages_dict: A dictionary of stages and their setup.
        """
        if stages_dict:
            print("STEPS CONFIGURED:")
            for key_stages in stages_dict:
                print(f"step {key_stages}:")
                keys_stages = [k for k, v in stages_dict[key_stages].items()]
                for k_stages in keys_stages:
                    if k_stages != "repartition":
                        print(f"  - {k_stages} = {stages_dict[key_stages][k_stages]}")
                    else:
                        repartition_stages = [k for k, v in stages_dict[key_stages][k_stages].items()]
                        for stg in repartition_stages:
                            print("  - repartition_type:")
                            print(f"\t {stg} = {stages_dict[key_stages][k_stages][stg]}")

    @classmethod
    def _sort_files(cls, sql_files: str) -> list:
        """
        Create a list sorted alphabetically based on the sql files provided.

        Args:
            sql_files: Name of the SQL files that will be sent to the framework
            to process (e.g. file1.sql, file2.sql).

        Returns:
            A list of sql files sorted alphabetically.

        """
        fileslist = sql_files.split(",")
        # remove extra spaces from items in the list
        fileslist = [x.strip() for x in fileslist]
        for file in range(len(fileslist)):
            fileslist[file] = fileslist[file].lower().strip()
            # apply bubble sort to sort the words
            for n in range(len(fileslist) - 1, 0, -1):
                for i in range(n):
                    if fileslist[i] > fileslist[i + 1]:
                        # swap data if the element is less than the next element in the array
                        fileslist[i], fileslist[i + 1] = fileslist[i + 1], fileslist[i]
        return fileslist

    @classmethod
    def _validate_metrics_config(cls, calc_metric: str, metrics_dict: dict, widget_index: int):
        """
        Validate the metrics widgets setup.

        Args:
            calc_metric: Name of the metric calculation set (e.g. last_cadence, last_year_cadence).
            metrics_dict: The dictionary of metrics and their setup.
            widget_index: Index of the widget selected to be validated.

        """
        if calc_metric == "last_cadence":
            if dbutils.widgets.get(f"{widget_index}_{calc_metric}_label").strip() != "":
                try:
                    int(dbutils.widgets.get(f"{widget_index}_{calc_metric}_window"))
                    metrics_dict[f"m{widget_index}"]["calculated_metric"].update(
                        {
                            f"{calc_metric}": [
                                {
                                    "label": dbutils.widgets.get(f"{widget_index}_{calc_metric}_label"),
                                    "window": dbutils.widgets.get(f"{widget_index}_{calc_metric}_window"),
                                }
                            ]
                        }
                    )
                    print(f"{calc_metric} configuration status: OK")
                except Exception:
                    print(f"{calc_metric} - WRONG CONFIGURATION:")
                    print(f"\t- The {calc_metric} window value must be INTEGER.")
            else:
                print(f"{calc_metric} - WRONG CONFIGURATION:")
                print(f"\t- The {calc_metric} label is mandatory.")
        elif calc_metric == "last_year_cadence":
            if dbutils.widgets.get(f"{widget_index}_{calc_metric}_label").strip() != "":
                metrics_dict[f"m{widget_index}"]["calculated_metric"].update(
                    {
                        f"{calc_metric}": [
                            {
                                "label": dbutils.widgets.get(f"{widget_index}_{calc_metric}_label"),
                                "window": 1,
                            }
                        ]
                    }
                )
                print(f"{calc_metric} configuration status: OK")
            else:
                print(f"{calc_metric} - WRONG CONFIGURATION:")
                print(f"\t- The {calc_metric} label is mandatory.")
        elif calc_metric == "window_function":
            if dbutils.widgets.get(f"{widget_index}_{calc_metric}_label").strip() != "":
                window_list = dbutils.widgets.get(f"{widget_index}_{calc_metric}_window").split(",")
                if len(window_list) > 1:
                    metrics_dict[f"m{widget_index}"]["calculated_metric"].update(
                        {
                            f"{calc_metric}": [
                                {
                                    "label": dbutils.widgets.get(f"{widget_index}_{calc_metric}_label"),
                                    "window": [int(x.strip()) for x in window_list],
                                    "agg_func": dbutils.widgets.get(name=f"{widget_index}_{calc_metric}_agg_func"),
                                }
                            ]
                        }
                    )
                    print(f"{calc_metric} configuration status: OK")
                else:
                    print(f"{calc_metric} - WRONG CONFIGURATION:")
                    print(
                        "\t- The window function must follow the pattern of "
                        "two integer digits separated with comma (e.g. 3,1)."
                    )
            else:
                print(f"{calc_metric} - WRONG CONFIGURATION:")
                print("\t- The window_function label is mandatory.")
        elif calc_metric == "derived_metric":
            if (
                    dbutils.widgets.get(name=f"{widget_index}_{calc_metric}_label").strip() != ""
                    and dbutils.widgets.get(name=f"{widget_index}_{calc_metric}_formula").strip() != ""
            ):
                metrics_dict[f"m{widget_index}"].update(
                    {
                        f"{calc_metric}": [
                            {
                                "label": dbutils.widgets.get(name=f"{widget_index}_{calc_metric}_label"),
                                "formula": dbutils.widgets.get(name=f"{widget_index}_{calc_metric}_formula"),
                            }
                        ]
                    }
                )
                print(f"{calc_metric} configuration status: OK")
            else:
                print(f"{calc_metric} - WRONG CONFIGURATION:")
                print("\t- The derived_metric label and formula are mandatory.")