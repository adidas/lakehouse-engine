# Heartbeat Sensor for Manual Table

This shows how to create a Heartbeat Sensor Orchestrator to detect new data from a
Manual Table and trigger Databricks Workflows related to them.

**Manual Tables (Lakehouse Manual Upload)** are different from regular Delta tables because:

- **Data Upload Pattern**: Instead of continuous streaming or scheduled batch loads, data is manually uploaded by users at irregular intervals.
- **Detection Challenge**: Unlike regular Delta tables with change data feeds or append operations, manual tables are typically overwritten completely, making it harder to detect new data using standard mechanisms.
- **Custom Detection Logic**: Requires a special `upstream_key` (usually a timestamp column) to track when the table was last updated, since the table structure and most content may remain the same between uploads.
- **Sensor Source Type**: Uses `lmu_delta_table` instead of `delta_table` to indicate this special handling requirement.

## Configuration required to create an orchestration task for the manual table source

- **sensor_source**: Set to `lmu_delta_table` in the Heartbeat Control Table to identify this as a Lakehouse Manual Upload Delta table source.
- **data_format**: Set to `delta` to specify the data format for reading Delta tables.
- **heartbeat_sensor_db_table**: Database table name for the Heartbeat sensor control table (e.g., `my_database.heartbeat_sensor`).
- **lakehouse_engine_sensor_db_table**: Database table name for the lakehouse engine sensors (e.g., `my_database.lakehouse_engine_sensors`).
- **domain**: Databricks workflows domain for job triggering.
- **token**: Databricks workflows token for authentication.

### Manual Tables Data Feed CSV Configuration Entry

To check how the entry for a manual table source should look in the Heartbeat Control Table, [check it here](../heartbeat.md#heartbeat-sensor-control-table-reference-records).

**Additional Requirements for Manual Tables**:

- **sensor_id**: Needs to be filled with the Lakehouse Manual Upload Delta table name along with database, e.g., `my_database.my_manual_table`.
- **upstream_key**: Must specify the table date/timestamp column (typically named `date`) which indicates when the Lakehouse Manual Upload table was last overwritten. This is crucial for detecting new manual uploads.

**Setup Requirements**:

- A column named **`date`** must be added to your Lakehouse Manual Upload source Delta table.
- This column should contain a timestamp value in **YYYYMMDDHHMMSS** format.
- The value should be updated to `current_timestamp()` whenever new data is uploaded.
- This timestamp serves as the "fingerprint" that the sensor uses to detect new uploads.

!!! note
    **`date` (or any other name, but with the same purpose, need to be defined on 
    `upstream_key` CSV configuration entry) column requirement**: Since manual tables 
    are typically overwritten entirely during each upload, standard Delta table change
    detection mechanisms won't work. The Heartbeat sensor needs a reliable way to
    determine if new data has been uploaded since the last check.

## Code sample of listener and trigger

```python
from lakehouse_engine.engine import (
    execute_sensor_heartbeat,
    trigger_heartbeat_sensor_jobs,
)

# Create an ACON dictionary for all manual table source entries.
# This ACON dictionary is useful for passing parameters to heartbeat sensors.

heartbeat_sensor_config_acon = {
    "sensor_source": "lmu_delta_table",
    "data_format": "delta",
    "heartbeat_sensor_db_table": "my_database.heartbeat_sensor",
    "lakehouse_engine_sensor_db_table": "my_database.lakehouse_engine_sensors",
    "domain": "DATABRICKS_WORKFLOWS_DOMAIN",
    "token": "DATABRICKS_WORKFLOWS_TOKEN",
}

# Execute Heartbeat sensor and trigger jobs which have acquired new data. 
execute_sensor_heartbeat(acon=heartbeat_sensor_config_acon)
trigger_heartbeat_sensor_jobs(heartbeat_sensor_config_acon)
```
