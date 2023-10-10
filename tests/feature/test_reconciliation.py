"""Test reconciliation."""
from typing import Any, List, Union

import pytest

from lakehouse_engine.algorithms.exceptions import ReconciliationFailedException
from lakehouse_engine.algorithms.reconciliator import ReconciliationType
from lakehouse_engine.engine import execute_reconciliation
from tests.conftest import (
    FEATURE_RESOURCES,
    LAKEHOUSE_FEATURE_CONTROL,
    LAKEHOUSE_FEATURE_IN,
    LAKEHOUSE_FEATURE_OUT,
)
from tests.utils.local_storage import LocalStorage

TEST_PATH = "reconciliation"
TEST_RESOURCES = f"{FEATURE_RESOURCES}/{TEST_PATH}"
TEST_LAKEHOUSE_IN = f"{LAKEHOUSE_FEATURE_IN}/{TEST_PATH}"
TEST_LAKEHOUSE_CONTROL = f"{LAKEHOUSE_FEATURE_CONTROL}/{TEST_PATH}"
TEST_LAKEHOUSE_OUT = f"{LAKEHOUSE_FEATURE_OUT}/{TEST_PATH}"

ACON_WITH_QUERIES = {
    "metrics": [
        {
            "metric": "net_sales",
            "type": "absolute",
            "aggregation": "sum",
            "yellow": 0.05,
            "red": 0.1,
        },
        {
            "metric": "net_sales",
            "type": "percentage",
            "aggregation": "avg",
            "yellow": 0.04,
            "red": 0.08,
        },
    ],
    "truth_input_spec": {
        "spec_id": "truth",
        "read_type": "batch",
        "data_format": "json",
        "options": {"multiline": "true"},
        "location": "file:///app/tests/lakehouse/in/feature/"
        "reconciliation/data/truth.json",
    },
    "truth_preprocess_query": """
        SELECT country, sum(net_sales) as net_sales
        FROM truth
        GROUP BY country
    """,
    "truth_preprocess_query_args": [
        {
            "function": "persist",
            "args": {"storage_level": "MEMORY_AND_DISK_DESER"},
        }
    ],
    "current_input_spec": {
        "spec_id": "current_results",
        "read_type": "batch",
        "data_format": "json",
        "options": {"multiline": "true"},
        "location": "file:///app/tests/lakehouse/in/feature/"
        "reconciliation/data/current.json",
    },
    "current_preprocess_query": """
        SELECT country, sum(net_sales) as net_sales
        FROM current
        GROUP BY country
    """,
    "current_preprocess_query_args": [
        {
            "function": "persist",
            "args": {"storage_level": "MEMORY_AND_DISK"},
        }
    ],
}

ACON_WITHOUT_QUERIES = {
    "metrics": [
        {
            "metric": "net_sales",
            "type": "absolute",
            "aggregation": "sum",
            "yellow": 0.01,
            "red": 0.05,
        },
        {
            "metric": "net_sales",
            "type": "absolute",
            "aggregation": "avg",
            "yellow": 0.04,
            "red": 0.08,
        },
    ],
    "truth_input_spec": {
        "spec_id": "truth",
        "read_type": "batch",
        "data_format": "json",
        "options": {"multiline": "true"},
        "location": "file:///app/tests/lakehouse/in/feature/"
        "reconciliation/data/truth.json",
    },
    "truth_preprocess_query_args": [{"function": "cache"}],
    "current_input_spec": {
        "spec_id": "current_results",
        "read_type": "batch",
        "data_format": "json",
        "options": {"multiline": "true"},
        "location": "file:///app/tests/lakehouse/in/feature/"
        "reconciliation/data/current.json",
    },
    "current_preprocess_query_args": [],  # turn cache off as it is a default
}

ACON_WITH_QUERIES_EMPTY_DF_TRUE_CHECK = {
    "metrics": [
        {
            "metric": "net_sales",
            "type": "absolute",
            "aggregation": "sum",
            "yellow": 0.05,
            "red": 0.1,
        },
        {
            "metric": "net_sales",
            "type": "percentage",
            "aggregation": "avg",
            "yellow": 0.04,
            "red": 0.08,
        },
    ],
    "truth_input_spec": {
        "spec_id": "truth",
        "read_type": "batch",
        "data_format": "json",
        "options": {"multiline": "true"},
        "location": "file:///app/tests/lakehouse/in/feature/"
        "reconciliation/data/truth.json",
    },
    "truth_preprocess_query": """
        SELECT country, sum(net_sales) as net_sales
        FROM truth where 1 = 0
        group by country
    """,
    "truth_preprocess_query_args": [
        {
            "function": "persist",
            "args": {"storage_level": "MEMORY_AND_DISK_DESER"},
        }
    ],
    "current_input_spec": {
        "spec_id": "current_results",
        "read_type": "batch",
        "data_format": "json",
        "options": {"multiline": "true"},
        "location": "file:///app/tests/lakehouse"
        "/in/feature/reconciliation/data/current.json",
    },
    "current_preprocess_query": """
        SELECT country, sum(net_sales) as net_sales
        FROM current
        WHERE 1 = 0
        group by country
    """,
    "current_preprocess_query_args": [
        {
            "function": "persist",
            "args": {"storage_level": "MEMORY_AND_DISK"},
        }
    ],
    "ignore_empty_df": True,
}

ACON_WITH_QUERIES_EMPTY_DF_FALSE_CHECK = {
    "metrics": [
        {
            "metric": "net_sales",
            "type": "absolute",
            "aggregation": "sum",
            "yellow": 0.05,
            "red": 0.1,
        },
        {
            "metric": "net_sales",
            "type": "percentage",
            "aggregation": "avg",
            "yellow": 0.04,
            "red": 0.08,
        },
    ],
    "truth_input_spec": {
        "spec_id": "truth",
        "read_type": "batch",
        "data_format": "json",
        "options": {"multiline": "true"},
        "location": "file:///app/tests/lakehouse/in/feature/"
        "reconciliation/data/truth.json",
    },
    "truth_preprocess_query": """
        SELECT country, sum(net_sales) as net_sales
        FROM truth where 1 = 0
        group by country
    """,
    "truth_preprocess_query_args": [
        {
            "function": "persist",
            "args": {"storage_level": "MEMORY_AND_DISK_DESER"},
        }
    ],
    "current_input_spec": {
        "spec_id": "current_results",
        "read_type": "batch",
        "data_format": "json",
        "options": {"multiline": "true"},
        "location": "file:///app/tests/lakehouse/in/feature/"
        "reconciliation/data/current.json",
    },
    "current_preprocess_query": """
        SELECT country, sum(net_sales) as net_sales
        FROM current
        WHERE 1 = 0
        group by country
    """,
    "current_preprocess_query_args": [
        {
            "function": "persist",
            "args": {"storage_level": "MEMORY_AND_DISK"},
        }
    ],
    "ignore_empty_df": False,
}


ACONS = {
    "with_queries_pct": ACON_WITH_QUERIES,
    "with_files_abs": ACON_WITHOUT_QUERIES,
    "failed_reconciliation_pct": ACON_WITH_QUERIES,
    "empty_truth": ACON_WITHOUT_QUERIES,
    "different_rows": ACON_WITHOUT_QUERIES,
    "empty_df_true_check": ACON_WITH_QUERIES_EMPTY_DF_TRUE_CHECK,
    "empty_df_false_check": ACON_WITH_QUERIES_EMPTY_DF_FALSE_CHECK,
}


@pytest.mark.parametrize(
    "scenario",
    [
        [
            "with_queries_pct",
            "current.json",
            "truth.json",
            None,
            "The Reconciliation process has succeeded.",
        ],
        [
            "with_files_abs",
            "current.json",
            "truth.json",
            None,
            "The Reconciliation process has succeeded.",
        ],
        [
            "failed_reconciliation_pct",
            "current_fail.json",
            "truth.json",
            "Reconciliation result: {'net_sales_absolute_diff_sum': 100.0, "
            "'net_sales_percentage_diff_avg': 0.0625}",
            "The Reconciliation process has failed with status: red.",
        ],
        [
            "empty_truth",
            "current.json",
            "truth_empty.json",
            None,
            "The reconciliation has failed because either the truth dataset or the "
            "current results dataset was empty.",
        ],
        [
            "different_rows",
            "current_different_rows.json",
            "truth_different_rows.json",
            "Reconciliation result: {'net_sales_absolute_diff_sum': 500.0, "
            "'net_sales_absolute_diff_avg': 100.0}",
            "The Reconciliation process has failed with status: red.",
        ],
        [
            "empty_df_true_check",
            "current.json",
            "truth.json",
            None,
            "The Reconciliation process has succeeded.",
        ],
        [
            "empty_df_false_check",
            "current.json",
            "truth.json",
            None,
            "The reconciliation has failed because either the truth dataset or the "
            "current results dataset was empty.",
        ],
    ],
)
def test_reconciliation(scenario: str, caplog: Any) -> None:
    """Test reconciliation.

    Args:
        scenario: scenario to test.
             with_queries - uses queries to get the truth data and the current data.
                Reconciliation type is percentage.
             with_files - uses files for the truth data and query for the current data.
                Reconciliation type is absolute.
             failed_reconciliation - same as 'with_queries' but with a failed
                reconciliation. Reconciliation type is percentage.
             empty_truth - scenario in which the truth data is empty.
             different_rows - the truth dataset and current results dataset have
                different rows, therefore reconciliation should fail.
        caplog: captured log.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/*.json",
        f"{TEST_LAKEHOUSE_IN}/data/",
    )

    acon = ACONS[scenario[0]]
    acon["current_input_spec"][  # type: ignore
        "location"
    ] = f"file:///app/tests/lakehouse/in/feature/reconciliation/data/{scenario[1]}"
    acon["truth_input_spec"][  # type: ignore
        "location"
    ] = f"file:///app/tests/lakehouse/in/feature/reconciliation/data/{scenario[2]}"

    if scenario[0] in [
        "failed_reconciliation_pct",
        "empty_truth",
        "different_rows",
        "empty_df_false_check",
    ]:
        with pytest.raises(ReconciliationFailedException) as e:
            execute_reconciliation(acon=acon)  # type: ignore
        if scenario[3]:
            assert scenario[3] in caplog.text
        assert str(e.value) == scenario[4]
    else:
        execute_reconciliation(acon=acon)  # type: ignore
        assert scenario[4] in caplog.text


@pytest.mark.parametrize(
    "scenario",
    [
        [
            "pass",
            ReconciliationType.PCT.value,
            0.05,
            0.1,
            "current_nulls_and_zeros",
            "truth_nulls_and_zeros",
            "Reconciliation result: {'net_sales_percentage_diff_sum': 0.0, "
            "'net_sales_percentage_diff_avg': 0.0}",
            "The Reconciliation process has succeeded.",
        ],
        [
            "fail_if_threshold_zero",
            ReconciliationType.PCT.value,
            0,
            0,
            "current_nulls_and_zeros_fail",
            "truth_nulls_and_zeros_fail",
            "Reconciliation result: {'net_sales_percentage_diff_sum': 1.0, "
            "'net_sales_percentage_diff_avg': 0.3333333333333333}",
            "The Reconciliation process has failed with status: red.",
        ],
        [
            "fail_null_is_not_zero",
            ReconciliationType.PCT.value,
            0.05,
            0.1,
            "current_nulls_and_zeros_fail",
            "truth_nulls_and_zeros_fail",
            "Reconciliation result: {'net_sales_percentage_diff_sum': 1.0, "
            "'net_sales_percentage_diff_avg': 0.3333333333333333}",
            "The Reconciliation process has failed with status: red.",
        ],
    ],
)
def test_nulls_and_zero_values_and_threshold(
    scenario: List[Union[str, float]], caplog: Any
) -> None:
    """Test truth and current datasets with nulls and zeros.

    Args:
        scenario: scenario to test.
            pass - reconciliation should pass even if there are 0s and nulls in the
                truth and current datasets.
            fail_if_threshold_zero - reconciliation should fail if users pass 0 as
                threshold as of course 0 indicates there's no difference. If that's the
                threshold then it will indicate that the reconciliation has failed.
            fail_null_is_not_zero - reconciliation should fail if in the first record
                of the current data we have a 0, and in the corresponding row of the
                truth data we have a null, because that indicates a percentage
                difference of 1 according to the recon algorithm, and therefore,
                the reconciliation should present those differences properly, instead
                of assuming that 0 is equal to null.
        caplog: captured log.
    """
    LocalStorage.copy_file(
        f"{TEST_RESOURCES}/data/*.json",
        f"{TEST_LAKEHOUSE_IN}/data/",
    )

    acon = ACON_WITHOUT_QUERIES
    acon["current_input_spec"]["location"] = (  # type: ignore
        f"file:///app/tests/"
        f"lakehouse/in/feature/reconciliation/data/{scenario[4]}.json"
    )
    acon["truth_input_spec"]["location"] = (  # type: ignore
        f"file:///app/tests/"
        f"lakehouse/in/feature/reconciliation/data/{scenario[5]}.json"
    )
    acon["metrics"][0]["type"] = scenario[1]  # type: ignore
    acon["metrics"][0]["yellow"] = scenario[2]  # type: ignore
    acon["metrics"][0]["red"] = scenario[3]  # type: ignore
    acon["metrics"][1]["type"] = scenario[1]  # type: ignore
    acon["metrics"][1]["yellow"] = scenario[2]  # type: ignore
    acon["metrics"][1]["red"] = scenario[3]  # type: ignore

    if scenario[0] in ["fail_null_is_not_zero", "fail_if_threshold_zero"]:
        with pytest.raises(ReconciliationFailedException) as e:
            execute_reconciliation(acon=acon)
        assert scenario[6] in caplog.text
        assert str(e.value) == scenario[7]
    else:
        execute_reconciliation(acon=acon)
        assert scenario[6] in caplog.text
