"""Unit tests focusing on the logging filter FilterSensitiveData."""
import logging
from typing import Any

from lakehouse_engine.utils.logging_handler import LoggingHandler

STR_MSGS_TO_LOG = [
    {  # Sample acon being logged, password has comma and double quotes
        "original_log": "Read Algorithm Configuration: {'input_specs': [{'spec_id': "
        "'source', 'read_type': 'batch', 'data_format': 'sap_bw', 'options': "
        "{'driver': 'org.sqlite.JDBC', 'user': 'user', 'password': 'p,w\"d', "
        "'url': 'jdbc:url', 'dbtable': 'table', 'numPartitions': 2, 'extraction_type': "
        "'delta', 'partitionColumn': 'item', 'lowerBound': 1, 'upperBound': 3}}], "
        "'output_specs': [{'spec_id': 'bronze', 'input_id': 'source', 'write_type': "
        "'append', 'data_format': 'delta', 'partitions': ['actrequest_timestamp'], "
        "'location': 'file:////path'}]}",
        "masked_log": "Read Algorithm Configuration: {'input_specs': [{'spec_id': "
        "'source', 'read_type': 'batch', 'data_format': 'sap_bw', 'options': "
        "{'driver': 'org.sqlite.JDBC', 'user': 'user', 'masked_cred': '******', "
        "'url': 'jdbc:url', 'dbtable': 'table', 'numPartitions': 2, 'extraction_type': "
        "'delta', 'partitionColumn': 'item', 'lowerBound': 1, 'upperBound': 3}}], "
        "'output_specs': [{'spec_id': 'bronze', 'input_id': 'source', 'write_type': "
        "'append', 'data_format': 'delta', 'partitions': ['actrequest_timestamp'], "
        "'location': 'file:////path'}]}",
    },
    {  # no single neither double quotes
        "original_log": "prop1: prop2, password: pwd, secret: secret",
        "masked_log": "prop1: prop2, masked_cred: ******, " "masked_cred: ******, ",
    },
    {  # double quotes, password has single quotes and comma, ends with secret and space
        # and additional log
        "original_log": '"prop1": "prop2", "password": "p,w\'d", '
        '"secret": "secret" other logs',
        "masked_log": '"prop1": "prop2", "masked_cred": "******", '
        '"masked_cred": "******", other logs',
    },
    {
        "original_log": "Read Algorithm Configuration: {'input_specs': [{'spec_id': "
        "'source', 'read_type': 'streaming', 'data_format': 'kafka', 'options': "
        "{'kafka.ssl.truststore.password': 'p,w\"d', 'kafka.ssl.keystore.password': "
        "'p,w\"d'}}], 'output_specs': [{'spec_id': 'bronze', 'input_id': 'source', "
        "'write_type': 'append', 'data_format': 'delta', 'partitions': "
        "['actrequest_timestamp'], 'location': 'file:////path'}]}",
        "masked_log": "Read Algorithm Configuration: {'input_specs': [{'spec_id': "
        "'source', 'read_type': 'streaming', 'data_format': 'kafka', 'options': "
        "{'masked_cred': '******', 'masked_cred': '******', }], "
        "'output_specs': [{'spec_id': 'bronze', 'input_id': 'source', 'write_type': "
        "'append', 'data_format': 'delta', 'partitions': ['actrequest_timestamp'], "
        "'location': 'file:////path'}]}",
    },
]
DICT_MSGS_TO_LOG = [
    # fmt: off
    {  # test with dict, because we rely on space after comma for the replace
        # and python might change the dict structure in the future
        "original_log": {"secret":"dummy_pwd","prop":"prop_val"},  # noqa: E231
        "masked_log": "{'masked_cred': '******', 'prop': 'prop_val'}",
    },
    # fmt: on
]
LOGGER = LoggingHandler(__name__).get_logger()


def test_log_filter_sensitive_data(caplog: Any) -> None:
    """Test the logging filter FilterSensitiveData.

    Given a set of messages, each message is logged (original_log) and tested
    against the expected output (masked_log).

    :param caplog: captures the log.
    """
    with caplog.at_level(logging.INFO):
        for str_msg in STR_MSGS_TO_LOG:
            LOGGER.info(str_msg["original_log"])
            assert str_msg["masked_log"] in caplog.text

        for dict_msg in DICT_MSGS_TO_LOG:
            LOGGER.info(dict_msg["original_log"])
            assert dict_msg["masked_log"] in caplog.text
