"""Custom transformers module."""

from typing import Callable

from pyspark.sql import DataFrame


class CustomTransformers(object):
    """Class representing a CustomTransformers."""

    @staticmethod
    def custom_transformation(custom_transformer: Callable) -> Callable:
        """Execute a custom transformation provided by the user.

        This transformer can be very useful whenever the user cannot use our provided
        transformers, or they want to write complex logic in the transform step of the
        algorithm.

        .. warning:: Attention!
            Please bear in mind that the custom_transformer function provided
            as argument needs to receive a DataFrame and return a DataFrame,
            because it is how Spark's .transform method is able to chain the
            transformations.

        Example:
        ```python
        def my_custom_logic(df: DataFrame) -> DataFrame:
        ```

        Args:
            custom_transformer: custom transformer function. A python function with all
                required pyspark logic provided by the user.

        Returns:
            Callable: the same function provided as parameter, in order to e called
                later in the TransformerFactory.

        {{get_example(method_name='custom_transformation')}}
        """
        return custom_transformer

    @staticmethod
    def sql_transformation(sql: str) -> Callable:
        """Execute a SQL transformation provided by the user.

        This transformer can be very useful whenever the user wants to perform
        SQL-based transformations that are not natively supported by the
        lakehouse engine transformers.

        Args:
            sql: the SQL query to be executed. This can read from any table or
                view from the catalog, or any dataframe registered as a temp
                view.

        Returns:
            Callable: A function to be called in .transform() spark function.

        {{get_example(method_name='sql_transformation')}}
        """

        def inner(df: DataFrame) -> DataFrame:
            return df.sparkSession.sql(sql)

        return inner
