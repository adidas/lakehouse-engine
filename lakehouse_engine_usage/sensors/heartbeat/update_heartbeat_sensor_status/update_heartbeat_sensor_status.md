# Update Heartbeat Sensor control delta table after processing the data

This shows how to update the status of your Heartbeat Sensor after executing the pipeline.

The `update_heartbeat_sensor_status` function is **critical for the Heartbeat Sensor lifecycle** because:

- **Completes the monitoring cycle**: When a Heartbeat sensor triggers a job, it sets the status to `IN_PROGRESS`. Without this update, the sensor would never know the job completed successfully.
- **Enables continuous monitoring**: Only after a job is marked as `COMPLETED` will the Heartbeat sensor resume monitoring that source for new events.
- **Prevents stuck jobs**: Without proper status updates, failed jobs remain in `IN_PROGRESS` status indefinitely, blocking future job triggers.
- **Supports recovery process**: This is essential for the [Job Failure Recovery Process](../heartbeat.md#heartbeat-sensor-workflow-explanation) described in the main Heartbeat documentation, where at least one successful run must be completed before the sensor resumes monitoring.

!!! note
    **When to use**: This function must be called as the **final task** in every Databricks job that is orchestrated by the Heartbeat Sensor to properly update the `status` to `COMPLETED` and record the job completion timestamp.

!!! note "Events acquired while the job is running"
    The sensors of the job are marked as `PROCESSED_NEW_DATA`, except the ones that
    acquired new data while the job was running (after its `job_start_timestamp`).
    These keep the `ACQUIRED_NEW_DATA` status, so that the new data triggers the job
    again and no upstream event is lost. This only applies to records `IN_PROGRESS`
    (jobs triggered by the Heartbeat): for manual runs, all the sensors are marked as
    `PROCESSED_NEW_DATA`.
    This behaviour does not depend on `max_event_gap_hours`.
    The sensor statuses are updated in a single merge, so if the sensor table is
    changed by another process at the same time (e.g. the Heartbeat acquiring a new
    event), the task fails with a Delta concurrent modification error instead of
    overwriting that change. In that case, the job records stay `IN_PROGRESS`: repair
    the update status task to complete the job.

## Configuration required to update heartbeat sensor status

- **job_id**: The unique identifier of the Databricks job that was triggered by the Heartbeat sensor (e.g., `"MY_JOB_ID"`).
- **heartbeat_sensor_control_table**: Database table name for the Heartbeat sensor control table (e.g., `"my_database.heartbeat_sensor"`).
- **sensor_table**: Database table name for the lakehouse engine sensors table (e.g., `"my_database.lakehouse_engine_sensors"`).

## Code sample

Code sample on how to update the status of your sensor in the Heartbeat Sensors Control Table:
```python
from lakehouse_engine.engine import update_heartbeat_sensor_status

update_heartbeat_sensor_status(
    job_id="MY_JOB_ID",
    heartbeat_sensor_control_table="my_database.heartbeat_sensor",
    sensor_table="my_database.lakehouse_engine_sensors",
)
```

If you want to know more please visit the definition of the class [here](../../../../reference/packages/core/definitions.md#packages.core.definitions.HeartbeatConfigSpec).