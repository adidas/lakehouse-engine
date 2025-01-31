# Sensor from JDBC

This shows how to create a **Sensor to detect new data from a JDBC table**.

## Configuration required to have a Sensor

- **jdbc_args**: Arguments of the JDBC upstream.
- **generate_sensor_query**: Generates a Sensor query to consume data from the upstream, this function can be used on `preprocess_query` ACON option.
    - **sensor_id**: The unique identifier for the Sensor.
    - **filter_exp**: Expression to filter incoming new data.
      A placeholder `?upstream_key` and `?upstream_value` can be used, example: `?upstream_key > ?upstream_value` so that it can be replaced by the respective values from the sensor `control_db_table_name` for this specific sensor_id.
    - **control_db_table_name**: Sensor control table name.
    - **upstream_key**: the key of custom sensor information to control how to identify new data from the upstream (e.g., a time column in the upstream).
    - **upstream_value**: the **first** upstream value to identify new data from the upstream (e.g., the value of a time present in the upstream). ***Note:*** This parameter will have effect just in the first run to detect if the upstream have new data. If it's empty the default value applied is `-2147483647`.
    - **upstream_table_name**: Table name to consume the upstream value. If it's empty the default value applied is `sensor_new_data`.

If you want to know more please visit the definition of the class [here](../../../reference/packages/core/definitions.md#packages.core.definitions.SensorSpec).

## Scenarios 

This covers the following scenarios of using the Sensor:

1. [Generic JDBC template with `fail_on_empty_result=True` (the default and SUGGESTED behaviour).](#fail_on_empty_result-as-true-default-and-suggested)
2. [Generic JDBC template with `fail_on_empty_result=False`.](#fail_on_empty_result-as-false)

Data from JDBC, in batch mode, will be consumed. If there is new data based in the preprocess query from the source table, it will trigger the condition to proceed to the next task.

### `fail_on_empty_result` as True (default and SUGGESTED)

```python
from lakehouse_engine.engine import execute_sensor, generate_sensor_query

acon = {
    "sensor_id": "MY_SENSOR_ID",
    "assets": ["MY_SENSOR_ASSETS"],
    "control_db_table_name": "my_database.lakehouse_engine_sensors",
    "input_spec": {
        "spec_id": "sensor_upstream",
        "read_type": "batch",
        "data_format": "jdbc",
        "jdbc_args": {
            "url": "JDBC_URL",
            "table": "JDBC_DB_TABLE",
            "properties": {
                "user": "JDBC_USERNAME",
                "password": "JDBC_PWD",
                "driver": "JDBC_DRIVER",
            },
        },
        "options": {
            "compress": True,
        },
    },
    "preprocess_query": generate_sensor_query(
        sensor_id="MY_SENSOR_ID",
        filter_exp="?upstream_key > '?upstream_value'",
        control_db_table_name="my_database.lakehouse_engine_sensors",
        upstream_key="UPSTREAM_COLUMN_TO_IDENTIFY_NEW_DATA",
    ),
    "base_checkpoint_location": "s3://my_data_product_bucket/checkpoints",
    "fail_on_empty_result": True,
}

execute_sensor(acon=acon)
```

### `fail_on_empty_result` as False

Using `fail_on_empty_result=False`, in which the `execute_sensor` function returns a `boolean` representing if it
has acquired new data. This value can be used to execute or not the next steps.

```python
from lakehouse_engine.engine import execute_sensor

acon = {
    [...],
    "fail_on_empty_result": False
}

acquired_data = execute_sensor(acon=acon)
```