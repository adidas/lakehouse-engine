# Heartbeat Sensor for SAP BW/B4

This shows how to create a Heartbeat Sensor Orchestrator to detect new data from 
SAP BW/B4 and trigger Databricks Workflows related to them.

## Configuration required to create an orchestration task for the SAP BW/B4 source

- **sensor_source**: Set to `sap_b4` or `sap_bw` in the Heartbeat Control Table to identify this as a SAP source.
- **data_format**: Set to `jdbc` to specify the data format for reading from SAP via JDBC connection.
- **heartbeat_sensor_db_table**: Database table name for the Heartbeat sensor control table (e.g., `my_database.heartbeat_sensor`).
- **lakehouse_engine_sensor_db_table**: Database table name for the lakehouse engine sensors (e.g., `my_database.lakehouse_engine_sensors`).
- **options**: JDBC connection configuration:
    - `compress`: Set to `true` to enable compression.
    - `driver`: JDBC driver class name.
    - `url`: JDBC connection URL.
    - `user`: JDBC username for authentication.
    - `password`: JDBC password for authentication.
- **jdbc_db_table**: SAP logchain table name to query for process chain status.
- **domain**: Databricks workflows domain for job triggering.
- **token**: Databricks workflows token for authentication.

### SAP BW/B4 Data Feed CSV Configuration Entry

To check how the entry for a SAP BW/B4 source should look in the Heartbeat Control Table, [check it here](../heartbeat.md#heartbeat-sensor-control-table-reference-records).

**Additional Requirements for SAP BW/4HANA**:

- The `sensor_id` needs to be filled with the Process Chain Name of the SAP object.
- `sensor_read_type` needs to be `batch` for SAP.

## Code sample of listener and trigger

```python
from lakehouse_engine.engine import (
    execute_sensor_heartbeat,
    trigger_heartbeat_sensor_jobs,
)

# Create an ACON dictionary for all SAP BW/B4 source entries.
# This ACON dictionary is useful for passing parameters to heartbeat sensors.

heartbeat_sensor_config_acon = {
    "sensor_source": "sap_b4|sap_bw",  # use sadp_b4 or sap_bw, depending on the source you are reading from
    "data_format": "jdbc",
    "heartbeat_sensor_db_table": "my_database.heartbeat_sensor",
    "lakehouse_engine_sensor_db_table": "my_database.lakehouse_engine_sensors",
    "options": {
        "compress": True,
        "driver": "JDBC_DRIVER",
        "url": "JDBC_URL",
        "user": "JDBC_USERNAME",
        "password": "JDBC_PSWD",
    },
    "jdbc_db_table": "SAP_LOGCHAIN_TABLE",
    "domain": "DATABRICKS_WORKFLOWS_DOMAIN",
    "token": "DATABRICKS_WORKFLOWS_TOKEN",
}

# Execute Heartbeat sensor and trigger jobs which have acquired new data. 
execute_sensor_heartbeat(acon=heartbeat_sensor_config_acon)
trigger_heartbeat_sensor_jobs(heartbeat_sensor_config_acon)
```
