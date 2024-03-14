# Filtered Full Load with Selective Replace

This scenario is very similar to the [Filtered Full Load](filtered_full_load.html), but we only replace a subset of the partitions, leaving the other ones untouched, so we don't replace the entire table. This capability is very useful for backfilling scenarios.
As for other cases, the acon configuration should be executed with `load_data` using:
```python
from lakehouse_engine.engine import load_data
acon = {...}
load_data(acon=acon)
```
Example of ACON configuration:
```json
.. include:: ../../../tests/resources/feature/full_load/with_filter_partition_overwrite/batch.json
```

##### Relevant notes:

* The key option for this scenario in the ACON is the `replaceWhere`, which we use to only overwrite a specific period of time, that realistically can match a subset of all the partitions of the table. Therefore, this capability is very useful for backfilling scenarios.