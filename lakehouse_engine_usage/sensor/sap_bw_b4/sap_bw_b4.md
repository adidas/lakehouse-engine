# Sensor from SAP

This shows how to create a **Sensor to detect new data from a SAP LOGCHAIN table**.

## Configuration required to have a Sensor

- **sensor_id**: A unique identifier of the sensor in a specific job.
- **assets**: List of assets considered for the sensor, which are considered as available once the
  sensor detects new data and status is `ACQUIRED_NEW_DATA`.
- **control_db_table_name**: Name of the sensor control table.
- **input_spec**: Input spec with the upstream source.
- **preprocess_query**: Query to filter data returned by the upstream.

.. note:: This parameter is only needed when the upstream data have to be filtered,
  in this case a custom query should be created with the source table as `sensor_new_data`.

- **base_checkpoint_location**: Spark streaming checkpoints to identify if the upstream has new data.
- **fail_on_empty_result**: Flag representing if it should raise `NoNewDataException` when
there is no new data detected from upstream.

Specific configuration required to have a Sensor consuming a SAP BW/B4 upstream.
The Lakehouse Engine provides two utility functions to make easier to consume SAP as upstream:
`generate_sensor_sap_logchain_query` and `generate_sensor_query`.

- **generate_sensor_sap_logchain_query**: This function aims
  to create a temporary table with timestamp from the SAP LOGCHAIN table, which is a process control table.
  
  **Note**: this temporary table only lives during runtime, and it is related with the
  sap process control table but has no relationship or effect on the sensor control table.
    - **chain_id**: SAP Chain ID process.
    - **dbtable**: SAP LOGCHAIN db table name, default: `my_database.RSPCLOGCHAIN`.
    - **status**: SAP Chain Status of your process, default: `G`.
    - **engine_table_name**: Name of the temporary table created from the upstream data,
      default: `sensor_new_data`.
      This temporary table will be used as source in the `query` option.

- **generate_sensor_query**: Generates a Sensor query to consume data from the temporary table created in the `prepareQuery`.
    - **sensor_id**: The unique identifier for the Sensor.
    - **filter_exp**: Expression to filter incoming new data.
      A placeholder `?upstream_key` and `?upstream_value` can be used, example: `?upstream_key > ?upstream_value`
      so that it can be replaced by the respective values from the sensor `control_db_table_name`
      for this specific sensor_id.
    - **control_db_table_name**: Sensor control table name.
    - **upstream_key**: the key of custom sensor information to control how to identify
      new data from the upstream (e.g., a time column in the upstream).
    - **upstream_value**: the **first** upstream value to identify new data from the
      upstream (e.g., the value of a time present in the upstream).
      .. note:: This parameter will have effect just in the first run to detect if the upstream have new data. If it's empty the default value applied is `-2147483647`.
    - **upstream_table_name**: Table name to consume the upstream value.
      If it's empty the default value applied is `sensor_new_data`.
      .. note:: In case of using the `generate_sensor_sap_logchain_query` the default value for the temp table is `sensor_new_data`, so if passing a different value in the `engine_table_name` this parameter should have the same value.

If you want to know more please visit the definition of the class [here](../../lakehouse_engine/core/definitions.html#SensorSpec).

## Scenarios

This covers the following scenarios of using the Sensor:

1. [The `fail_on_empty_result=True` (the default and SUGGESTED behaviour).](#fail_on_empty_result-as-true-default-and-suggested)
2. [The `fail_on_empty_result=False`.](#fail_on_empty_result-as-false)

Data from SAP, in streaming mode, will be consumed, so if there is any new data in the kafka topic it will give condition to proceed to the next task.

### `fail_on_empty_result` as True (default and SUGGESTED)

```python
from lakehouse_engine.engine import execute_sensor, generate_sensor_query, generate_sensor_sap_logchain_query

acon = {
    "sensor_id": "MY_SENSOR_ID",
    "assets": ["MY_SENSOR_ASSETS"],
    "control_db_table_name": "my_database.lakehouse_engine_sensors",
    "input_spec": {
        "spec_id": "sensor_upstream",
        "read_type": "batch",
        "data_format": "jdbc",
        "options": {
            "compress": True,
            "driver": "JDBC_DRIVER",
            "url": "JDBC_URL",
            "user": "JDBC_USERNAME",
            "password": "JDBC_PWD",
            "prepareQuery": generate_sensor_sap_logchain_query(chain_id="CHAIN_ID", dbtable="JDBC_DB_TABLE"),
            "query": generate_sensor_query(
                sensor_id="MY_SENSOR_ID",
                filter_exp="?upstream_key > '?upstream_value'",
                control_db_table_name="my_database.lakehouse_engine_sensors",
                upstream_key="UPSTREAM_COLUMN_TO_IDENTIFY_NEW_DATA",
            ),
        },
    },
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

