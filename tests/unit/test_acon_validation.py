"""Unit tests for ACON validators."""

import pytest


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "name": "Validate delete objects function",
            "acon": {
                "operations": [
                    {
                        "manager": "file",
                        "function": "delete_objects",
                        "bucket": "example-bucket",
                        "object_paths": ["path/to/delete/"],
                        "dry_run": True,
                    }
                ],
            },
        },
        {
            "name": "Validate copy objects function with missing parameters",
            "acon": {
                "operations": [
                    {
                        "manager": "file",
                        "function": "copy_objects",
                        "bucket": "example-bucket",
                        "source_object": ["path/to/copy/"],
                    }
                ]
            },
            "exception": """Errors found during validation:
Missing mandatory parameters for file manager function copy_objects: ['destination_bucket', 'destination_object', 'dry_run']
Type validation errors for file manager function copy_objects: ["Parameter 'source_object' expected str, got list"]""",  # noqa: E501
        },
        {
            "name": "Validate list of operations",
            "acon": {
                "operations": [
                    {
                        "manager": "file",
                        "function": "delete_objects",
                        "bucket": "example-bucket",
                        "object_paths": ["path/to/delete/"],
                        "dry_run": True,
                    },
                    {
                        "manager": "table",
                        "function": "execute_sql",
                        "sql": "create example_table",
                    },
                    {
                        "manager": "table",
                        "function": "optimize",
                        "table_or_view": "example_table",
                    },
                ],
            },
        },
        {
            "name": "Validate list of operations with errors",
            "acon": {
                "operations": [
                    {
                        "manager": "file",
                        "function": "delete_objects",
                        "bucket": "example-bucket",
                        "object_paths": "path/to/delete/",
                        "dry_run": "test string",
                    },
                    {
                        "manager": "table",
                        "function": "execute_sql",
                        "sql": 10,
                    },
                    {
                        "manager": "table",
                        "function": "optimize_dataset",
                        "table_or_view": "example_table",
                    },
                ]
            },
            "exception": """Errors found during validation:
Type validation errors for file manager function delete_objects: ["Parameter 'object_paths' expected list, got str", "Parameter 'dry_run' expected bool, got str"]
Type validation errors for table manager function execute_sql: ["Parameter 'sql' expected str, got int"]
Function 'optimize_dataset' not supported for table manager""",  # noqa: E501
        },
    ],
)
def test_manager_validation(scenario: dict) -> None:
    """Test to validate manager acons."""
    from lakehouse_engine.engine import validate_manager_list

    acon = scenario["acon"]
    exception = scenario.get("exception", None)

    if exception:
        with pytest.raises(Exception) as e:
            validate_manager_list(acon)
        assert str(e.value) == exception
    else:
        validate_manager_list(acon)
