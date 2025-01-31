# Streaming Append Load with DROPMALFORMED

This scenario illustrates an append load done via streaming instead of batch, providing an efficient way of picking up new files from an S3 folder, instead of relying on the incremental filtering from the source needed from a batch based process (see append loads in batch from a JDBC source to understand the differences between streaming and batch append loads). However, not all sources (e.g., JDBC) allow streaming.
As for other cases, the acon configuration should be executed with `load_data` using:
```python
from lakehouse_engine.engine import load_data
acon = {...}
load_data(acon=acon)
```
Example of ACON configuration:
```json
{!../../../../tests/resources/feature/append_load/streaming_dropmalformed/streaming.json!}
```

##### Relevant notes:

* In this scenario, we use DROPMALFORMED read mode, which drops rows that do not comply with the provided schema;
* In this scenario, the schema is provided through the `input_spec` "schema" variable. This removes the need of a separate JSON Spark schema file, which may be more convenient in certain cases.
* As can be seen, we use the `output_spec` Spark option `checkpointLocation` to specify where to save the checkpoints indicating what we have already consumed from the input data. This allows fault-tolerance if the streaming job fails, but more importantly, it allows us to run a streaming job using [AvailableNow](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers) and the next job automatically picks up the stream state since the last checkpoint, allowing us to do efficient append loads without having to manually specify incremental filters as we do for batch append loads.