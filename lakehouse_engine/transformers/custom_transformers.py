"""Custom transformers module."""
from typing import Callable


class CustomTransformers(object):
    """Class representing a CustomTransformers."""

    @staticmethod
    def custom_transformation(custom_transformer: Callable) -> Callable:
        """Execute a custom transformation provided by the user.

        This transformer can be very useful whenever the user cannot use our provided
        transformers, or they want to write complex logic in the transform step of the
        algorithm.

        Attention!!! Please bare in mind that the custom_transformer function provided
        as argument needs to receive a DataFrame and return a DataFrame, because it is
        how Spark's .transform method is able to chain the transformations.
        Example:
            def my_custom_logic(df: DataFrame) -> DataFrame:

        Args:
            custom_transformer: custom transformer function. A python function with all
                required pyspark logic provided by the user.

        Returns:
            Callable: the same function provided as parameter, in order to e called
                later in the TransformerFactory.

        """
        return custom_transformer
