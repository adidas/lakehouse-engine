# Custom Expectations

## Defining Custom Expectations

Custom expectations are defined in python and need to follow a structure to correctly integrate with Great Expectations.

Follow the [documentation of GX on Creating Custom Expectations](https://docs.greatexpectations.io/docs/oss/guides/expectations/custom_expectations_lp/) 
and find information about [the existing types of expectations](https://docs.greatexpectations.io/docs/conceptual_guides/expectation_classes). 

Here is an example of custom expectation.
As for other cases, the acon configuration should be executed with `load_data` using:
```python
from lakehouse_engine.engine import load_data
acon = {...}
load_data(acon=acon)
```

Example of ACON configuration:

```python 
{!../../../../lakehouse_engine/dq_processors/custom_expectations/expect_column_pair_a_to_be_smaller_or_equal_than_b.py!}
```

### Naming Conventions
Your expectation's name **should** start with expect.

The name of the file **must** be the name of the expectation written in snake case. Ex: `expect_column_length_match_input_length`

The name of the class **must** be the name of the expectation written in camel case. Ex: `ExpectColumnLengthMatchInputLength`

### File Structure
The file contains two main sections:

- the definition of the metric that we are tracking (where we define the logic of the expectation);
- the definition of the expectation

### Metric Definition
In this section we define the logic of the expectation. This needs to follow a certain structure:

#### Code Structure
1) The class you define needs to extend one of the Metric Providers defined by Great Expectations that corresponds 
to your expectation's type. More info on the [metric providers](https://docs.greatexpectations.io/docs/conceptual_guides/metricproviders). 

2) You need to define the name of your metric. This name **must** be unique and **must** follow the following structure: 
type of expectation.name of metric. Ex.: `column_pair_values.a_smaller_or_equal_than_b`
**Types of expectations:**  `column_values`, `multicolumn_values`, `column_pair_values`, `table_rows`, `table_columns`.

3) Any [GX default parameters](#parameters) that are necessary to calculate your metric **must** be defined as "condition_domain_keys".

4) Any [additional parameters](#parameters) that are necessary to calculate your metric **must** be defined as "condition_value_keys".

5) The logic of your expectation **must** be defined for the SparkDFExecutionEngine in order to be run on the Lakehouse.

```python
1) class ColumnMapMetric(ColumnMapMetricProvider):
    """Asserts that a column matches a pattern."""
 
    2) condition_metric_name = "column_pair_values.a_smaller_or_equal_than_b"
    3) condition_domain_keys = (
        "batch_id",
        "table",
        "column_A",
        "column_B",
        "ignore_row_if",
    )
    4) condition_value_keys = ("margin",)
     
    5) @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(
        self: ColumnPairMapMetricProvider,
        column_A: Any,
        column_B: Any,
        margin: Any,
        **kwargs: dict,
    ) -> Any:
        """Implementation of the expectation's logic.
 
        Args:
            column_A: Value of the row of column_A.
            column_B: Value of the row of column_B.
            margin: margin value to be added to column_b.
            kwargs: dict with additional parameters.
 
        Returns:
            If the condition is met.
        """
        if margin is None:
            approx = 0
        elif not isinstance(margin, (int, float, complex)):
            raise TypeError(
                f"margin must be one of int, float, complex."
                f" Found: {margin} as {type(margin)}"
            )
        else:
            approx = margin  # type: ignore
 
        return column_A <= column_B + approx  # type: ignore
```

### Expectation Definition
In this section we define the expectation. This needs to follow a certain structure:

#### Code Structure
1) The class you define needs to extend one of the Expectations defined by Great Expectations that corresponds to your expectation's type. 

2) You must define an "examples" object where you define at least one success and one failure of your expectation to 
demonstrate its logic. The result format must be set to complete, and you must set the [unexpected_index_name](#result-format) variable.

!!! note
    For any examples where you will have unexpected results you must define  unexpected_index_list in your "out" element.
    This will be validated during the testing phase.

3) The metric **must** be the same you defined in the metric definition.

4) You **must** define all [additional parameters](#parameters) that the user has to/should provide to the expectation. 

5) You **should** define any default values for your expectations parameters. 

6) You must **define** the `_validate` method like shown in the example. You **must** call the `validate_result` function 
inside your validate method, this process adds a validation to the unexpected index list in the examples.

!!! note
    If your custom expectation requires any extra validations, or you require additional fields to be returned on 
    the final dataframe, you can add them in this function. 
    The validate_result method has two optional parameters (`partial_success` and `partial_result) that can be used to 
    pass the result of additional validations and add more information to the result key of the returned dict respectively.

```python
1) class ExpectColumnPairAToBeSmallerOrEqualThanB(ColumnPairMapExpectation):
    """Expect values in column A to be lower or equal than column B.
 
    Args:
        column_A: The first column name.
        column_B: The second column name.
        margin: additional approximation to column B value.
 
    Keyword Args:
        allow_cross_type_comparisons: If True, allow
            comparisons between types (e.g. integer and string).
            Otherwise, attempting such comparisons will raise an exception.
        ignore_row_if: "both_values_are_missing",
            "either_value_is_missing", "neither" (default).
        result_format: Which output mode to use:
            `BOOLEAN_ONLY`, `BASIC` (default), `COMPLETE`, or `SUMMARY`.
        include_config: If True (default), then include the expectation config
            as part of the result object.
        catch_exceptions: If True, then catch exceptions and
            include them as part of the result object. Default: False.
        meta: A JSON-serializable dictionary (nesting allowed)
            that will be included in the output without modification.
 
    Returns:
        An ExpectationSuiteValidationResult.
    """
    2) examples = [
        {
            "dataset_name": "Test Dataset",
            "data": {
                "a": [11, 22, 50],
                "b": [10, 21, 100],
                "c": [9, 21, 30],
            },
            "schemas": {
                "spark": {"a": "IntegerType", "b": "IntegerType", "c": "IntegerType"}
            },
            "tests": [
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "a",
                        "column_B": "c",
                        "result_format": {
                            "result_format": "COMPLETE",
                            "unexpected_index_column_names": ["c"],
                            "include_unexpected_rows": True,
                        },
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [
                            {"c": 9, "a": 11},
                            {"c": 21, "a": 22},
                            {"c": 30, "a": 50},
                        ],
                    },
                },
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "a",
                        "column_B": "b",
                        "margin": 1,
                        "result_format": {
                            "result_format": "COMPLETE",
                            "unexpected_index_column_names": ["a"],
                        },
                    },
                    "out": {"success": True},
                },
            ],
        },
    ]
      
    3) map_metric = "column_values.pattern_match"
    4) success_keys = (
        "validation_regex",
        "mostly",
    )
    5) default_kwarg_values = {
        "ignore_row_if": "never",
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "mostly": 1,
    }
 
    6) def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ) -> dict:
        """Custom implementation of the GX _validate method.
 
        This method is used on the tests to validate both the result
        of the tests themselves and if the unexpected index list
        is correctly generated.
        The GX test logic does not do this validation, and thus
        we need to make it manually.
 
        Args:
            configuration: Configuration used in the test.
            metrics: Test result metrics.
            runtime_configuration: Configuration used when running the expectation.
            execution_engine: Execution Engine where the expectation was run.
 
        Returns:
            Dictionary with the result of the validation.
        """
        return validate_result(self, configuration, metrics)
```

### Printing the Expectation Diagnostics
Your expectations **must** include the ability to call the Great Expectations diagnostic function in order to be validated.

In order to do this code **must** be present.

```python
"""Mandatory block of code. If it is removed the expectation will not be available."""
if __name__ == "__main__":
    # test the custom expectation with the function `print_diagnostic_checklist()`
    ExpectColumnPairAToBeSmallerOrEqualThanB().print_diagnostic_checklist()
```

## Creation Process

1) Create a branch from lakehouse engine.

2) Create a custom expectation with your specific logic:

   1. All new expectations must be placed inside folder `/lakehouse_engine/dq_processors/custom_expectations`.
   2. The name of the expectation must be added to the file `/lakehouse_engine/core/definitions.py`, to the variable: `CUSTOM_EXPECTATION_LIST`.
   3. All new expectations must be tested on `/tests/feature/custom_expectations/test_custom_expectations.py`.
   In order to create a new test for your custom expectation it is necessary to:
   
   - Copy one of the expectation folders in `tests/resources/feature/custom_expectations` renaming it to your custom expectation.
   - Make any necessary changes on the data/schema file present.
   - On `/tests/feature/custom_expectations/test_custom_expectations.py` add a scenario to test your expectation, all expectations 
   must be tested on batch and streaming. The test is implemented to generate an acon based on each scenario data. 
   - Test your developments to check that everything is working as intended.

3) When the development is completed, create a pull request with your changes.

4) Your expectation will be available with the next release of the lakehouse engine that happens after you pull request is approved. 
This means that you need to upgrade your version of the lakehouse engine in order to use it.

## Usage
Custom Expectations are available to use like any other expectations provided by Great Expectations.

## Parameters
Depending on the type of expectation you are defining some parameters are expected by default. 
Ex: A ColumnMapExpectation has a default "column" parameter.

### Mostly
[Mostly](https://docs.greatexpectations.io/docs/reference/learn/expectations/standard_arguments/#mostly) is a standard 
parameter for a subset of expectations that is used to define a threshold for the failure of an expectation. 
Ex: A mostly value of 0.7 makes it so that the expectation only fails if more than 70% of records have 
a negative result.

## Result Format
Great Expectations has several different types of [result formats](https://docs.greatexpectations.io/docs/reference/learn/expectations/result_format/) 
for the expectations results. The lakehouse engine requires the result format to be set to "COMPLETE" in order to tag 
the lines where the expectations failed.

### `unexpected_index_column_names`
Inside this key you must define what columns are used as an index inside your data. If this is set and the result 
format is set to "COMPLETE" a list with the indexes of the lines that failed the validation will be returned by 
Great Expectations.
This information is used by the Lakehouse Engine to tag the lines in error after the fact. The additional tests 
inside the `_validate` method verify that the custom expectation is tagging these lines correctly.
