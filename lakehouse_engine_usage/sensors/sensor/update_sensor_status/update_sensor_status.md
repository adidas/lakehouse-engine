# Update Sensor control delta table after processing the data

This shows how to **update the status of your Sensor after processing the new data**.

Here is an example on how to update the status of your sensor in the Sensors Control Table:
```python
from lakehouse_engine.engine import update_sensor_status

update_sensor_status(
    sensor_id="MY_SENSOR_ID",
    control_db_table_name="my_database.lakehouse_engine_sensors",
    status="PROCESSED_NEW_DATA",
    assets=["MY_SENSOR_ASSETS"]
)
```

If you want to know more please visit the definition of the class [here](../../../../reference/packages/core/definitions.md#packages.core.definitions.SensorSpec).