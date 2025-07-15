# Heartbeat Sensor for Delta Table

This shows how to create a Heartbeat Sensor Orchestrator to detect new data from a 
Delta Table and trigger Databricks Workflows related to them.

## Configuration required to create an orchestration task for the delta table source

- **sensor_source**: Set to `delta_table` in the Heartbeat Control Table to identify this as a Delta table source.
- **data_format**: Set to `delta` to specify the data format for reading Delta tables.
- **heartbeat_sensor_db_table**: Database table name for the Heartbeat sensor control table (e.g., `my_database.heartbeat_sensor`).
- **lakehouse_engine_sensor_db_table**: Database table name for the lakehouse engine sensors (e.g., `my_database.lakehouse_engine_sensors`).
- **options**: Configuration options for Delta table reading:
    - `readChangeFeed`: Set to `"true"` to enable change data feed reading.
- **base_checkpoint_location**: `S3` path for storing checkpoint data (required if `sensor_read_type` is `streaming`).
- **domain**: Databricks workflows domain for job triggering.
- **token**: Databricks workflows token for authentication.

### Delta Table Data Feed CSV Configuration Entry

To check how the entry for a Delta table source should look in the Heartbeat Control Table, [check it here](../heartbeat.md#heartbeat-sensor-control-table-reference-records).

## Code sample of listener and trigger

```python
from lakehouse_engine.engine import (
    execute_sensor_heartbeat,
    trigger_heartbeat_sensor_jobs,
)

# Create an ACON dictionary for all delta table source entries.
# This ACON dictionary is useful for passing parameters to heartbeat sensors.

heartbeat_sensor_config_acon = {
    "sensor_source": "delta_table",
    "data_format": "delta",
    "heartbeat_sensor_db_table": "my_database.heartbeat_sensor",
    "lakehouse_engine_sensor_db_table": "my_database.lakehouse_engine_sensors",
    "options": {
        "readChangeFeed": "true",
    },
    "base_checkpoint_location": "s3://my_data_product_bucket/checkpoints",
    "domain": "DATABRICKS_WORKFLOWS_DOMAIN",
    "token": "DATABRICKS_WORKFLOWS_TOKEN",
}

# Execute Heartbeat sensor and trigger jobs which have acquired new data. 
execute_sensor_heartbeat(acon=heartbeat_sensor_config_acon)
trigger_heartbeat_sensor_jobs(heartbeat_sensor_config_acon)
```
