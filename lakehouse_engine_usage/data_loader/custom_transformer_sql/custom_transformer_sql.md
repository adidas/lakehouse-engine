# SQL Custom Transformer
The SQL Custom Transformer executes a SQL transformation provided by the user.This transformer can be very useful whenever the user wants to perform SQL-based transformations that are not natively supported by the lakehouse engine transformers.

The transformer receives the SQL query to be executed. This can read from any table or view from the catalog, or any dataframe registered as a temp view.

> To register a dataframe as a temp view you can use the "temp_view" config in the input_specs, as shown below.

```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "sales_source",
            "read_type": "batch",
            "data_format": "csv",
            "options": {"mode": "FAILFAST", "header": True, "delimiter": "|"},
            "schema_path": "file:///app/tests/lakehouse/in/feature/"
            "data_loader_custom_transformer/sql_transformation/"
            "source_schema.json",
            "location": "file:///app/tests/lakehouse/in/feature/"
            "data_loader_custom_transformer/sql_transformation/data",
            "temp_view": "sales_sql",
        }
    ],
    "transform_specs": [
        {
            "spec_id": "calculated_kpi",
            "input_id": "sales_source",
            "transformers": [
                {
                    "function": "sql_transformation",
                    "args": {"sql": SQL},
                }
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "sales_bronze",
            "input_id": "calculated_kpi",
            "write_type": "overwrite",
            "data_format": "delta",
            "location": "file:///app/tests/lakehouse/out/feature/"
            "data_loader_custom_transformer/sql_transformation/data",
        }
    ],
}

load_data(acon=acon)
```