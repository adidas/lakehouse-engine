"""Expectation to check if aggregated column satisfy the condition."""
from typing import Any, Dict, Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


class ExpectQueriedColumnAggValueToBe(QueryExpectation):
    """Expect agg of column to satisfy the condition specified.

    Args:
        template_dict: dict with the following keys:
            column (column to check sum).
            group_column_list (group by column names to be listed).
            condition (how to validate the aggregated value eg: between,
            greater, lesser).
            max_value (maximum allowed value).
            min_value (minimum allowed value).
            agg_type (sum/count/max/min).
    """

    metric_dependencies = ("query.template_values",)
    query = """
            SELECT {group_column_list}, {agg_type}({column})
            FROM {active_batch}
            GROUP BY {group_column_list}
            """
    success_keys = ("template_dict", "query")
    domain_keys = (
        "query",
        "template_dict",
        "batch_id",
        "row_condition",
        "condition_parser",
    )
    default_kwarg_values = {
        "include_config": True,
        "mostly": 1.0,
        "result_format": "BASIC",
        "catch_exceptions": False,
        "meta": None,
        "query": query,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validates that a configuration has been set.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]):
            An optional Expectation Configuration entry.

        Returns:
            None. Raises InvalidExpectationConfigurationError
        """
        super().validate_configuration(configuration)

    @staticmethod
    def _validate_between(
        x: str, y: int, expected_max_value: int, expected_min_value: int
    ) -> dict:
        """Method to check whether value satisfy the between condition.

        Args:
            x: contains key of dict(query_result).
            y: contains value of dict(query_result).
            expected_max_value: max value passed.
            expected_min_value: min value passed.

        Returns:
            dict with the results after being validated.
        """
        if expected_min_value <= y <= expected_max_value:
            return {
                "info": f"Value is within range\
                    {expected_min_value} and {expected_max_value}",
                "success": True,
            }
        else:
            return {
                "success": False,
                "result": {
                    "info": f"Value not in range\
                        {expected_min_value} and {expected_max_value}",
                    "observed_value": (x, y),
                },
            }

    @staticmethod
    def _validate_lesser(x: str, y: int, expected_max_value: int) -> dict:
        """Method to check whether value satisfy the less condition.

        Args:
            x: contains key of dict(query_result).
            y: contains value of dict(query_result).
            expected_max_value: max value passed.

        Returns:
            dict with the results after being validated.
        """
        if y < expected_max_value:
            return {
                "info": f"Value is lesser than {expected_max_value}",
                "success": True,
            }
        else:
            return {
                "success": False,
                "result": {
                    "info": f"Value is greater than {expected_max_value}",
                    "observed_value": (x, y),
                },
            }

    @staticmethod
    def _validate_greater(x: str, y: int, expected_min_value: int) -> dict:
        """Method to check whether value satisfy the greater condition.

        Args:
            x: contains key of dict(query_result).
            y: contains value of dict(query_result).
            expected_min_value: min value passed.

        Returns:
            dict with the results after being validated.
        """
        if y > expected_min_value:
            return {
                "info": f"Value is greater than {expected_min_value}",
                "success": True,
            }
        else:
            return {
                "success": False,
                "result": {
                    "info": f"Value is less than {expected_min_value}",
                    "observed_value": (x, y),
                },
            }

    def _validate_condition(self, query_result: dict, template_dict: dict) -> dict:
        """Method to check whether value satisfy the expected result.

        Args:
            query_result: contains dict of key and value.
            template_dict: contains dict of input provided.

        Returns:
            dict with the results after being validated.
        """
        result: Dict[Any, Any] = {}
        for x, y in query_result.items():
            condition_check = template_dict["condition"]
            if condition_check == "between":
                _max = template_dict["max_value"]
                _min = template_dict["min_value"]
                result = self._validate_between(x, y, _max, _min)
            elif condition_check == "lesser":
                _max = template_dict["max_value"]
                result = self._validate_lesser(x, y, _max)
            else:
                _min = template_dict["min_value"]
                result = self._validate_greater(x, y, _min)

        return result

    @staticmethod
    def _generate_dict(query_result: list) -> dict:
        """Generate a dict from a list of dicts and merge the group by columns values.

        Args:
            query_result: contains list of dict values obtained from query.

        Returns:
            Dict

        Example:
            input: [dict_values(['Male', 25, 3500]), dict_values(['Female', 25, 6200]),
                dict_values(['Female', 20, 3500]), dict_values(['Male', 20, 6900])].
            output: {'Male|25': 3500, 'Female|25': 6200,
                'Female|20': 3500, 'Male|20': 6900}.
        """
        intermediate_list = []
        final_list = []
        for i in range(len(query_result)):
            intermediate_list.append(list(query_result[i]))
            for element in intermediate_list:
                if type(element) is list:
                    output = "|".join(map(str, element))
                    key = "|".join(map(str, element[0:-1]))
                    value = output.replace(key + "|", "")
                    final_list.append(key)
                    final_list.append(value)

        new_result = {
            final_list[i]: int(final_list[i + 1]) for i in range(0, len(final_list), 2)
        }

        return new_result

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ) -> Union[ExpectationValidationResult, dict]:
        """Implementation of the GE _validate method.

        This method is used on the tests to validate the result
        of the query output.

        Args:
            configuration: Configuration used in the test.
            metrics: Test result metrics.
            runtime_configuration: Configuration used when running the expectation.
            execution_engine: Execution Engine where the expectation was run.

        Returns:
            Dictionary with the result of the validation.
        """
        query_result = metrics.get("query.template_values")
        query_result = [element.values() for element in query_result]
        query_result = self._generate_dict(query_result)
        template_dict = self._validate_template_dict(configuration)
        output = self._validate_condition(query_result, template_dict)

        return output

    @staticmethod
    def _validate_template_dict(configuration: ExpectationConfiguration) -> dict:
        """Validate the template dict.

        Args:
            configuration (ExpectationConfiguration)

        Returns:
            Dict. Raises TypeError and KeyError
        """
        template_dict = configuration.kwargs.get("template_dict")

        if not isinstance(template_dict, dict):
            raise TypeError("template_dict must be supplied as a dict")

        if not all(
            [
                "column" in template_dict,
                "group_column_list" in template_dict,
                "agg_type" in template_dict,
                "condition" in template_dict,
            ]
        ):
            raise KeyError(
                "The following keys have to be in the \
                    template dict: column, group_column_list, condition, agg_type"
            )

        return template_dict

    examples = [
        {
            "dataset_name": "Test Dataset",
            "data": [
                {
                    "data": {
                        "ID": [1, 2, 3, 4, 5, 6],
                        "Names": [
                            "Ramesh",
                            "Nasser",
                            "Jessica",
                            "Komal",
                            "Jude",
                            "Muffy",
                        ],
                        "Age": [25, 25, 25, 20, 20, 25],
                        "Gender": [
                            "Male",
                            "Male",
                            "Female",
                            "Female",
                            "Male",
                            "Female",
                        ],
                        "Salary": [1000, 2500, 5000, 3500, 6900, 1200],
                    },
                    "schemas": {
                        "spark": {
                            "ID": "IntegerType",
                            "Names": "StringType",
                            "Age": "IntegerType",
                            "Gender": "StringType",
                            "Salary": "IntegerType",
                        }
                    },
                }
            ],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "column": "Salary",
                            "group_column_list": "Gender",
                            "agg_type": "sum",
                            "condition": "greater",
                            "min_value": 2000,
                        },
                        "result_format": {
                            "result_format": "COMPLETE",
                        },
                    },
                    "out": {"success": True},
                    "only_for": ["spark"],
                },
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "column": "Salary",
                            "group_column_list": "Gender,Age",
                            "agg_type": "sum",
                            "condition": "between",
                            "max_value": 7000,
                            "min_value": 2000,
                        },
                        "result_format": {
                            "result_format": "COMPLETE",
                        },
                    },
                    "out": {"success": True},
                    "only_for": ["spark"],
                },
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "column": "Salary",
                            "group_column_list": "Age",
                            "agg_type": "max",
                            "condition": "lesser",
                            "max_value": 10000,
                        },
                        "result_format": {
                            "result_format": "COMPLETE",
                        },
                    },
                    "out": {"success": True},
                    "only_for": ["spark"],
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "column": "Salary",
                            "group_column_list": "Gender",
                            "agg_type": "count",
                            "condition": "greater",
                            "min_value": 4,
                        },
                        "result_format": {
                            "result_format": "COMPLETE",
                        },
                    },
                    "out": {"success": False},
                    "only_for": ["sqlite", "spark"],
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "column": "Salary",
                            "group_column_list": "Gender,Age",
                            "agg_type": "sum",
                            "condition": "between",
                            "max_value": 2000,
                            "min_value": 1000,
                        },
                        "result_format": {
                            "result_format": "COMPLETE",
                        },
                    },
                    "out": {"success": False},
                    "only_for": ["spark"],
                },
            ],
        },
    ]

    library_metadata = {
        "tags": ["query-based"],
    }


if __name__ == "__main__":
    ExpectQueriedColumnAggValueToBe().print_diagnostic_checklist()
