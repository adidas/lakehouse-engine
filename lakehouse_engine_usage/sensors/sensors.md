# Sensors

## What is it?

The lakehouse engine provides two complementary sensor solutions for monitoring upstream systems and detecting new data:

### 1. Sensor

Traditional lakehouse engine sensors are abstractions that simplify complex Spark code, allowing you to check if an upstream system or data product contains new data since the last job execution. These sensors run in very small single-node clusters to ensure cost efficiency. If the upstream contains new data, the sensor triggers the rest of the job; otherwise, the job exits without spinning up larger clusters for intensive ETL operations.

**Key Characteristics:**

- Individual sensor configuration for each data source within jobs.
- Manual job execution after sensor detection or the need of adding a Sensor task in the beginning of the pipeline.
- Single-source monitoring capability per task, coupling it directly with the source and not with a source type.
- Built-in cost optimization through minimal cluster usage.

### 2. Heartbeat Sensor

The Heartbeat Sensor is a robust, centralized orchestration system that enhances the Sensor infrastructure. It provides automated event detection, efficient multiple sources parallelism detection, and seamless integration with downstream workflows. Unlike Sensors that require individual configuration for each data source, the Heartbeat Sensor manages multiple sources through a single control table and automatically triggers Databricks jobs when new data is detected.

**Key Characteristics:**

- Centralized control table for managing all Sensor sources
- Automatic Databricks job triggering via Job Run API
- Multi-source support with dependency management
- Built-in hard/soft dependency validation
- Comprehensive status tracking and lifecycle management

## When to Use Each Solution

| Aspect                    | Sensor                                                   | Heartbeat Sensor                                           |
|---------------------------|----------------------------------------------------------|------------------------------------------------------------|
| **Use Case**              | Simple, single-source monitoring within individual jobs. | Complex, multi-source orchestration with job dependencies. |
| **Configuration**         | Individual sensor setup per job.                         | Centralized control table configuration.                   |
| **Job Triggering**        | Manual job execution after sensor detection.             | Automatic Databricks job triggering via Job API.           |
| **Dependency Management** | Not supported.                                           | Built-in hard/soft dependency validation.                  |
| **Scalability**           | Limited to individual sensors.                           | Highly scalable with parallel source type processing.      |
| **Management Overhead**   | Higher (individual configurations).                      | Lower (centralized management).                            |
| **Best For**              | Single data product monitoring.                          | Enterprise-level orchestration.                            |

### Decision Guide

**Choose Sensors when:**

- You need simple monitoring for a single data source.
- Your workflow involves manual job execution or you are up to update your pipeline to have a Sensor task at the beginning.
- You have straightforward ETL pipelines without complex dependencies.
- You prefer embedded sensor logic within individual jobs.
- Your data pipeline is relatively straightforward.

**Choose Heartbeat Sensor when:**

- You need to orchestrate multiple data sources and dependencies.
- You want automated job triggering without manual intervention.
- You require centralized monitoring and management.
- You need to handle complex multi-source workflows at enterprise scale.
- You require enterprise-level orchestration capabilities.
- You need centralized monitoring and status management.

Both solutions can coexist in the same environment, allowing you to choose the appropriate sensor type based on specific use case requirements.

## How do Sensor-based Jobs Work?

With sensors, data products in the lakehouse can detect if another data product or upstream system contains new data since the last successful job execution. The workflow is as follows:

1. **Data Detection**: A data product checks if Kafka, JDBC, or any other supported Sensor source contains new data using the respective sensors.
2. **Cost-Efficient Execution**: The Sensor task runs in a very small single-node cluster to ensure cost efficiency.
3. **Conditional Processing**: If the Sensor detects new data in the upstream, you can start a different ETL job cluster to process all ETL tasks (data processing tasks).
4. **Cross-Product Sensing**: Different data products can Sense if upstream data products have new data using:
    - **(Preferred)** Sensing the upstream data product's Sensor control delta table.
    - Sensing the upstream data product's data files in S3 (files sensor) or delta tables (delta table sensor).

For detailed information about Heartbeat Sensor implementation, configuration, and usage, see the [Sensor documentation](sensor/sensor.md).

## How do Heartbeat Sensor Jobs Work?

The Heartbeat Sensor approach uses a centralized sensor cluster running on a single node that continuously checks for new events from different sensor sources mentioned in the Heartbeat sensor control table. When a new event is available from a sensor source, it automatically triggers the corresponding job via the Databricks Job Run API using a pull-based approach.

**Workflow Process:**

1. **Continuous Monitoring**: The heartbeat cluster continuously polls various sensor sources.
2. **Event Detection**: Checks for `NEW_EVENT_AVAILABLE` status from configured sources.
3. **Dependency Validation**: Validates hard/soft dependencies before job triggering.
4. **Automatic Triggering**: Automatically triggers dependent Databricks jobs.
5. **Status Management**: Updates job status throughout the lifecycle.

**Key Advantages:**

- **Centralized Control**: Single control table manages all sensor sources and dependencies.
- **Automated Orchestration**: No manual intervention required for job triggering.
- **Multi-Source Support**: Handles diverse source types (SAP, Kafka, Delta Tables, Manual Uploads, Trigger Files) in one unified system.
- **Dependency Management**: Built-in validation prevents premature job execution.
- **Status Tracking**: Comprehensive lifecycle tracking from detection to job completion.

For detailed information about Heartbeat Sensor implementation, configuration, and usage, see the [Heartbeat Sensor documentation](heartbeat/heartbeat.md).
