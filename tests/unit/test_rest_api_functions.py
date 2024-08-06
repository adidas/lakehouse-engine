"""Test REST api related functions that cannot be tested inside Spark."""

import logging
from collections import namedtuple
from typing import Any
from unittest.mock import patch

from pyspark.sql import Row

from lakehouse_engine.core.definitions import OutputSpec
from lakehouse_engine.io.writers.rest_api_writer import RestApiWriter
from lakehouse_engine.utils.logging_handler import LoggingHandler

LOGGER = LoggingHandler(__name__).get_logger()
RestResponse = namedtuple("RestResponse", "status_code text")


@patch(
    "lakehouse_engine.io.writers.rest_api_writer.execute_api_request",
    return_value=RestResponse(status_code=200, text="ok"),
)
def test_send_payload_to_rest_api_simple_params(_: Any, caplog: Any) -> None:
    """Test if the REST API payload creation process is correct w/ simple params.

    Args:
        _: ignored patch.
        caplog: captures the log.
    """
    output_spec = OutputSpec(
        spec_id="test_output",
        input_id="test_input",
        write_type="overwrite",
        data_format="rest_api",
        options={
            "rest_api_url": "https://www.dummy-url.local/dummy-endpoint",
            "rest_api_method": "post",
            "rest_api_header": {"Authorization": "Bearer dummytoken"},
        },
    )
    row = Row(payload='{"dummy_payload":"dummy value"}')
    func = RestApiWriter._get_func_to_send_payload_to_rest_api(output_spec)
    func(row)

    str_to_assert = "Final payload: {'dummy_payload': 'dummy value'}"

    with caplog.at_level(logging.DEBUG):
        assert str_to_assert in caplog.text


@patch(
    "lakehouse_engine.io.writers.rest_api_writer.execute_api_request",
    return_value=RestResponse(status_code=200, text="ok"),
)
def test_send_payload_to_rest_api_with_file_params(_: Any, caplog: Any) -> None:
    """Test if the REST API payload creation process is correct with file params.

    Args:
        _: ignored patch.
        caplog: captures the log.
    """
    output_spec = OutputSpec(
        spec_id="test_output",
        input_id="test_input",
        write_type="overwrite",
        data_format="rest_api",
        options={
            "rest_api_url": "https://www.dummy-url.local/dummy-endpoint",
            "rest_api_method": "post",
            "rest_api_header": {"Authorization": "Bearer dummytoken"},
            "rest_api_is_file_payload": True,
            "rest_api_file_payload_name": "anotherFileName",
            "rest_api_extra_json_payload": {"a": "b"},
        },
    )
    row = Row(payload='{"dummy_payload":"dummy value"}')
    func = RestApiWriter._get_func_to_send_payload_to_rest_api(output_spec)
    func(row)

    str_to_assert = (
        "Final payload: {'anotherFileName': "
        "'{\"dummy_payload\":\"dummy value\"}', 'a': 'b'}"
    )

    with caplog.at_level(logging.DEBUG):
        assert str_to_assert in caplog.text
