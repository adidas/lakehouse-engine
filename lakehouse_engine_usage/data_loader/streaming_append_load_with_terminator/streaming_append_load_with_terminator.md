# Streaming Append Load with Optimize Dataset Terminator

This scenario includes a terminator which optimizes a dataset (table), being able of vacuuming the table, optimising it with z-order or not, computing table statistics and more. You can find more details on the Terminator [here](../../../reference/packages/terminators/dataset_optimizer.md).

As for other cases, the acon configuration should be executed with `load_data` using:
```python
from lakehouse_engine.engine import load_data
acon = {...}
load_data(acon=acon)
```
Example of ACON configuration:
```json
{!../../../../tests/resources/feature/append_load/streaming_with_terminators/streaming.json!}
```