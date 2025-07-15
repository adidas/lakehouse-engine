# Heartbeat Sensor for Kafka

This shows how to create a Heartbeat Sensor Orchestrator to detect new data from 
Kafka and trigger Databricks Workflows related to them.

## Configuration required to create an orchestration task for the kafka source

- **sensor_source**: Set to `kafka` in the Heartbeat Control Table to identify this as a Kafka source.
- **data_format**: Set to `kafka` to specify the data format for reading Kafka streams.
- **heartbeat_sensor_db_table**: Database table name for the Heartbeat sensor control table (e.g., `my_database.heartbeat_sensor`).
- **lakehouse_engine_sensor_db_table**: Database table name for the lakehouse engine sensors (e.g., `my_database.lakehouse_engine_sensors`).
- **options**: Configuration options for Kafka reading:
    - `readChangeFeed`: Set to `"true"` to enable change data feed reading.
- **kafka_configs**: Kafka connection and security configurations:
    - `kafka_bootstrap_servers_list`: Kafka server endpoints.
    - `kafka_ssl_truststore_location`: Path to SSL truststore.
    - `truststore_pwd_secret_key`: Secret key for truststore password.
    - `kafka_ssl_keystore_location`: Path to SSL keystore.
    - `keystore_pwd_secret_key`: Secret key for keystore password.
- **kafka_secret_scope**: Databricks secret scope for Kafka credentials.
- **base_checkpoint_location**: S3 path for storing checkpoint data (required if `sensor_read_type` is `streaming`).
- **domain**: Databricks workflows domain for job triggering.
- **token**: Databricks workflows token for authentication.

### Kafka Data Feed CSV Configuration Entry

To check how the entry for a Kafka source should look in the Heartbeat Control Table, [check it here](../heartbeat.md#heartbeat-sensor-control-table-reference-records).

**Additional Requirements for Kafka**:

The `sensor_id` follows a specific naming convention because you can have multiple data 
products using the same configuration file with different Kafka configuration values:

- The value for the `sensor_id` will be the Kafka Topic name starting with 
`<product_name:>` or any other prefix, example: `my_product: my.topic`.
- How it works? → Heartbeat receives a dictionary containing all kafka configurations by
product, which is passed as `kafka_configs` in the ACON.
Then it segregates the config based on `sensor_id` value present in the heartbeat
control table.
Heartbeat will split the `sensor_id` based on colon (:) and the first part of it will be
considered as product name (in our case, `my_product`) and the second part of
the split string will be the Kafka topic name (in our case, `my.topic`).
Finally, **it will make use of the product related kafka config from the `kafka_configs`**.

## Code sample of listener and trigger

```python
from lakehouse_engine.engine import (
    execute_sensor_heartbeat,
    trigger_heartbeat_sensor_jobs,
)

# Kafka configurations for the product, we strongly recommend to read these values from a external configuration file.
kafka_configs = {
  "my_product": {
    "kafka_bootstrap_servers_list": "KAFKA_SERVER",
    "kafka_ssl_truststore_location": "TRUSTSTORE_LOCATION",
    "truststore_pwd_secret_key": "TRUSTSTORE_PWD",
    "kafka_ssl_keystore_location": "KEYSTORE_LOCATION",
    "keystore_pwd_secret_key": "KEYSTORE_PWD"
  }
}

# Create an ACON dictionary for all kafka source entries.
# This ACON dictionary is useful for passing parameters to heartbeat sensors.

heartbeat_sensor_config_acon = {
    "sensor_source": "kafka",
    "data_format": "kafka",
    "heartbeat_sensor_db_table": "my_database.heartbeat_sensor",
    "lakehouse_engine_sensor_db_table": "my_database.lakehouse_engine_sensors",
    "options": {
        "readChangeFeed": "true",
    },
    "kafka_configs": kafka_configs,
    "kafka_secret_scope": "DB_SECRET_SCOPE",
    "base_checkpoint_location": "s3://my_data_product_bucket/checkpoints",
    "domain": "DATABRICKS_WORKFLOWS_DOMAIN",
    "token": "DATABRICKS_WORKFLOWS_TOKEN",
}

# Execute Heartbeat sensor and trigger jobs which have acquired new data. 
execute_sensor_heartbeat(acon=heartbeat_sensor_config_acon)
trigger_heartbeat_sensor_jobs(heartbeat_sensor_config_acon)
```
