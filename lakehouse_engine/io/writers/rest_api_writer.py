"""Module to define behaviour to write to REST APIs."""

import json
from typing import Any, Callable, OrderedDict

from pyspark.sql import DataFrame, Row

from lakehouse_engine.core.definitions import OutputSpec
from lakehouse_engine.io.writer import Writer
from lakehouse_engine.utils.logging_handler import LoggingHandler
from lakehouse_engine.utils.rest_api import (
    RESTApiException,
    RestMethods,
    RestStatusCodes,
    execute_api_request,
)


class RestApiWriter(Writer):
    """Class to write data to a REST API."""

    _logger = LoggingHandler(__name__).get_logger()

    def __init__(self, output_spec: OutputSpec, df: DataFrame, data: OrderedDict):
        """Construct RestApiWriter instances.

        Args:
            output_spec: output specification.
            df: dataframe to be written.
            data: list of all dfs generated on previous steps before writer.
        """
        super().__init__(output_spec, df, data)

    def write(self) -> None:
        """Write data to REST API."""
        if not self._df.isStreaming:
            self._write_to_rest_api_in_batch_mode(self._df, self._output_spec)
        else:
            self._write_to_rest_api_in_streaming_mode(
                self._df, self._output_spec, self._data
            )

    @staticmethod
    def _get_func_to_send_payload_to_rest_api(output_spec: OutputSpec) -> Callable:
        """Define and return a function to send the payload to the REST api.

        Args:
            output_spec: Output Specification containing configurations to
                communicate with the REST api. Within the output_spec, the user
                can specify several options:
                    - rest_api_header: http headers.
                    - rest_api_basic_auth: basic http authentication details
                        (e.g., {"username": "x", "password": "y"}).
                    - rest_api_url: url of the api.
                    - rest_api_method: REST method (e.g., POST or PUT).
                    - rest_api_sleep_seconds: sleep seconds to avoid throttling.
                    - rest_api_is_file_payload: if the payload to be sent to the
                        api is in the format of a file using multipart encoding
                        upload. if this is true, then the payload will always be
                        sent using the "files" parameter in Python's requests
                        library.
                    - rest_api_file_payload_name: when rest_api_is_file_payload
                        is true, this option can be used to define the file
                        identifier in Python's requests library.
                    - extra_json_payload: when rest_api_file_payload_name is False,
                        can be used to provide additional JSON variables to add to
                        the original payload. This is useful to complement
                        the original payload with some extra input to better
                        configure the final payload to send to the REST api. An
                        example can be to add a constant configuration value to
                        add to the payload data.

        Returns:
            Function to be called inside Spark dataframe.foreach.
        """
        headers = output_spec.options.get("rest_api_header", None)
        basic_auth_dict = output_spec.options.get("rest_api_basic_auth", None)
        url = output_spec.options["rest_api_url"]
        method = output_spec.options.get("rest_api_method", RestMethods.POST.value)
        sleep_seconds = output_spec.options.get("rest_api_sleep_seconds", 0)
        is_file_payload = output_spec.options.get("rest_api_is_file_payload", False)
        file_payload_name = output_spec.options.get(
            "rest_api_file_payload_name", "file"
        )
        extra_json_payload = output_spec.options.get(
            "rest_api_extra_json_payload", None
        )
        success_status_codes = output_spec.options.get(
            "rest_api_success_status_codes", RestStatusCodes.OK_STATUS_CODES.value
        )

        def send_payload_to_rest_api(row: Row) -> Any:
            """Send payload to the REST API.

            The payload needs to be prepared as a JSON string column in a dataframe.
            E.g., {"a": "a value", "b": "b value"}.

            Args:
                row: a row in a dataframe.
            """
            if "payload" not in row:
                raise ValueError("Input DataFrame must contain 'payload' column.")

            str_payload = row.payload

            payload = None
            if not is_file_payload:
                payload = json.loads(str_payload)
            else:
                payload = {file_payload_name: str_payload}

            if extra_json_payload:
                payload.update(extra_json_payload)

            RestApiWriter._logger.debug(f"Original payload: {str_payload}")
            RestApiWriter._logger.debug(f"Final payload: {payload}")

            response = execute_api_request(
                method=method,
                url=url,
                headers=headers,
                basic_auth_dict=basic_auth_dict,
                json=payload if not is_file_payload else None,
                files=payload if is_file_payload else None,
                sleep_seconds=sleep_seconds,
            )

            RestApiWriter._logger.debug(
                f"Response: {response.status_code} - {response.text}"
            )

            if response.status_code not in success_status_codes:
                raise RESTApiException(
                    f"API response status code {response.status_code} is not in"
                    f" {success_status_codes}. Got {response.text}"
                )

        return send_payload_to_rest_api

    @staticmethod
    def _write_to_rest_api_in_batch_mode(
        df: DataFrame, output_spec: OutputSpec
    ) -> None:
        """Write to REST API in Spark batch mode.

        This function uses the dataframe.foreach function to generate a payload
        for each row of the dataframe and send it to the REST API endpoint.

        Warning! Make sure your execution environment supports RDD api operations,
        as there are environments where RDD operation may not be supported. As,
        df.foreach() is a shorthand for df.rdd.foreach(), this can bring issues
        in such environments.

        Args:
            df: dataframe to write.
            output_spec: output specification.
        """
        df.foreach(RestApiWriter._get_func_to_send_payload_to_rest_api(output_spec))

    @staticmethod
    def _write_to_rest_api_in_streaming_mode(
        df: DataFrame, output_spec: OutputSpec, data: OrderedDict
    ) -> None:
        """Write to REST API in streaming mode.

        Args:
            df: dataframe to write.
            output_spec: output specification.
            data: list of all dfs generated on previous steps before writer.
        """
        df_writer = df.writeStream.trigger(**Writer.get_streaming_trigger(output_spec))

        stream_df = (
            df_writer.options(**output_spec.options if output_spec.options else {})
            .foreachBatch(
                RestApiWriter._write_transformed_micro_batch(output_spec, data)
            )
            .start()
        )

        if output_spec.streaming_await_termination:
            stream_df.awaitTermination(output_spec.streaming_await_termination_timeout)

    @staticmethod
    def _write_transformed_micro_batch(  # type: ignore
        output_spec: OutputSpec, data: OrderedDict
    ) -> Callable:
        """Define how to write a streaming micro batch after transforming it.

        Args:
            output_spec: output specification.
            data: list of all dfs generated on previous steps before writer.

        Returns:
            A function to be executed in the foreachBatch spark write method.
        """

        def inner(batch_df: DataFrame, batch_id: int) -> None:
            transformed_df = Writer.get_transformed_micro_batch(
                output_spec, batch_df, batch_id, data
            )

            if output_spec.streaming_micro_batch_dq_processors:
                transformed_df = Writer.run_micro_batch_dq_process(
                    transformed_df, output_spec.streaming_micro_batch_dq_processors
                )

            RestApiWriter._write_to_rest_api_in_batch_mode(transformed_df, output_spec)

        return inner
