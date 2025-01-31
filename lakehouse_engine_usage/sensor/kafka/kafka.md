# Sensor from Kafka

This shows how to create a **Sensor to detect new data from Kafka**.

## Configuration required to have a Sensor

- **sensor_id**: A unique identifier of the sensor in a specific job.
- **assets**: List of assets considered for the sensor, which are considered as available once the
  sensor detects new data and status is `ACQUIRED_NEW_DATA`.
- **control_db_table_name**: Name of the sensor control table.
- **input_spec**: Input spec with the upstream source.
- **preprocess_query**: Query to filter data returned by the upstream.

!!! note
    This parameter is only needed when the upstream data have to be filtered, in this case a custom query should be created with the source table as `sensor_new_data`.

- **base_checkpoint_location**: Spark streaming checkpoints to identify if the upstream has new data.
- **fail_on_empty_result**: Flag representing if it should raise `NoNewDataException` when
there is no new data detected from upstream.

If you want to know more please visit the definition of the class [here](../../../reference/packages/core/definitions.md#packages.core.definitions.SensorSpec).

## Scenarios

This covers the following scenarios of using the Sensor:

1. [The `fail_on_empty_result=True` (the default and SUGGESTED behaviour).](#fail_on_empty_result-as-true-default-and-suggested)
2. [The `fail_on_empty_result=False`.](#fail_on_empty_result-as-false)

Data from Kafka, in streaming mode, will be consumed, so if there is any new data in the kafka topic it will give condition to proceed to the next task.

### `fail_on_empty_result` as True (default and SUGGESTED)

```python
from lakehouse_engine.engine import execute_sensor

acon = {
    "sensor_id": "MY_SENSOR_ID",
    "assets": ["MY_SENSOR_ASSETS"],
    "control_db_table_name": "my_database.lakehouse_engine_sensors",
    "input_spec": {
        "spec_id": "sensor_upstream",
        "read_type": "streaming",
        "data_format": "kafka",
        "options": {
            "kafka.bootstrap.servers": "KAFKA_SERVER",
            "subscribe": "KAFKA_TOPIC",
            "startingOffsets": "earliest",
            "kafka.security.protocol": "SSL",
            "kafka.ssl.truststore.location": "TRUSTSTORE_LOCATION",
            "kafka.ssl.truststore.password": "TRUSTSTORE_PWD",
            "kafka.ssl.keystore.location": "KEYSTORE_LOCATION",
            "kafka.ssl.keystore.password": "KEYSTORE_PWD",
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