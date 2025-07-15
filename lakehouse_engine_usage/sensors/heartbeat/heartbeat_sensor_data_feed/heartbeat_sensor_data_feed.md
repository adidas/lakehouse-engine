# Heartbeat Sensor Control Table Data Feeder

## What is it?

It's a foundational component of the Heartbeat Sensor architecture. The primary purpose
is to populate and maintain the Control Table, which drives the entire heartbeat 
monitoring process. The Data Feeder Job is responsible for creating and updating entries
in the Control Table. Each entry in the control table represents a sensor_source (e.g., 
SAP, Kafka, Delta) for a unique combination of `sensor_id` and `trigger_job_id`.

## Configuration required to execute heartbeat sensor data feed

- **heartbeat_sensor_data_feed_path**: S3 path to the CSV file containing the heartbeat sensor control table data (e.g., `"s3://my_data_product_bucket/local_data/heartbeat_sensor/heartbeat_sensor_control_table_data.csv"`).
- **heartbeat_sensor_control_table**: Database table name for the [Heartbeat sensor control table](../heartbeat.md#control-table-schema) (e.g., `"my_database.heartbeat_sensor"`).

## How it works

1. A Heartbeat Sensor data feed job in each data product needs to be created to facilitate any
addition, update and deletion of entries.
2. Entries need to be added in CSV file format [as shown in Heartbeat Sensor Control table
Metadata description section for more](../heartbeat.md#the-structure-and-relevance-of-the-data-products-heartbeat-sensor-control-table).
Other fields in the control table will be filled automatically at different stages of 
the sensor process.
3. After adding/updating/deleting any entries in CSV, the Data feeder job needs to run again
to reflect the changes in the table.

## Code sample

```python
from lakehouse_engine.engine import execute_heartbeat_sensor_data_feed

execute_heartbeat_sensor_data_feed(
    heartbeat_sensor_data_feed_path="s3://my_data_product_bucket/local_data/heartbeat_sensor/heartbeat_sensor_control_table_data.csv" ,
    heartbeat_sensor_control_table="my_database.heartbeat_sensor"
)
```
