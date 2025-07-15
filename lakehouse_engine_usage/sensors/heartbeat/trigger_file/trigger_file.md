# Heartbeat Sensor for Trigger Files

This shows how to create a Heartbeat Sensor Orchestrator to detect new data from 
Trigger Files and trigger Databricks Workflows related to them.

## Generating the trigger file

It's needed to create a task in the upstream pipeline to generate a trigger file,
indicating that the upstream source has completed and the dependent job can be triggered.
The `sensor_id` used to generate the file must match the `sensor_id` specified in the
heartbeat control table. Check here the [code example](#creation-of-the-trigger-file-following-the-sensorid-standard-code-example) of how to generate the
trigger file.

#### Creation of the trigger file following the `sensor_id` standard code example:
```pyhon
import datetime

sensor_id = "my_trigger"
file_root_path = "s3://my_data_product_bucket/triggers"

file_name = f"{sensor_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
file_path = "/".join([file_root_path, sensor_id, file_name])

### Write Trigger File to S3 location using dbutils
output = dbutils.fs.put(file_path, "Success")
```

## Configuration required to create an orchestration task for the trigger file source

- **sensor_source**: Set to `trigger_file` in the Heartbeat Control Table to identify this as a trigger file source.
- **data_format**: Set to `cloudfiles` to enable Spark Auto Loader functionality for monitoring trigger files. This format allows the system to automatically detect when new trigger files are available at the specified location and trigger the [corresponding `trigger_job_id`](../heartbeat.md#control-table-schema).
- **heartbeat_sensor_db_table**: Database table name for the Heartbeat sensor control table (e.g., `my_database.heartbeat_sensor`).
- **lakehouse_engine_sensor_db_table**: Database table name for the lakehouse engine sensors (e.g., `my_database.lakehouse_engine_sensors`).
- **options**: Cloud files configuration:
    - `cloudFiles.format`: Set to `"csv"` to specify the file format.
- **schema_dict**: Schema definition for the trigger files:
    - Defines the structure with fields like `file_name` (string) and `file_modification_time` (timestamp).
- **base_checkpoint_location**: S3 path for storing checkpoint data (required if `sensor_read_type` is `streaming`).
- **base_trigger_file_location**: S3 path where trigger files are located.
- **domain**: Databricks workflows domain for job triggering.
- **token**: Databricks workflows token for authentication.

### Trigger File Data Feed CSV Configuration Entry

To check how the entry for a trigger file source should look in the Heartbeat Control Table, [check it here](../heartbeat.md#heartbeat-sensor-control-table-reference-records).

**Additional Requirements for Trigger File**:

- The `sensor_id` will match the name used to create the trigger file. For example, if
the trigger file is named `my_trigger_YYYYMMDDHHMMSS.txt`, then the sensor_id will be
`my_trigger`.

## Code sample of listener and trigger

```python
from lakehouse_engine.engine import (
    execute_sensor_heartbeat,
    trigger_heartbeat_sensor_jobs,
)

# Create an ACON dictionary for all trigger file source entries.
# This ACON dictionary is useful for passing parameters to heartbeat sensors.

heartbeat_sensor_config_acon = {
    "sensor_source": "trigger_file",
    "data_format": "cloudfiles",
    "heartbeat_sensor_db_table": "my_database.heartbeat_sensor",
    "lakehouse_engine_sensor_db_table": "my_database.lakehouse_engine_sensors",
    "options": {
        "cloudFiles.format": "csv",
    },
    "schema_dict": {
        "type": "struct",
        "fields": [
            {
                "name": "file_name",
                "type": "string",
            },
            {
                "name": "file_modification_time",
                "type": "timestamp",
            },
        ],
    },
    "base_checkpoint_location": "s3://my_data_product_bucket/checkpoints",
    "base_trigger_file_location": "s3://my_data_product_bucket/triggers",
    "domain": "DATABRICKS_WORKFLOWS_DOMAIN",
    "token": "DATABRICKS_WORKFLOWS_TOKEN",
}

# Execute Heartbeat sensor and trigger jobs which have acquired new data. 
execute_sensor_heartbeat(acon=heartbeat_sensor_config_acon)
trigger_heartbeat_sensor_jobs(heartbeat_sensor_config_acon)
```
