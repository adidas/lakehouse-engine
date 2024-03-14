# Streaming Delta Load with Group and Rank Condensation

This scenario is useful for when we want to do delta loads based on changelogs that need to be first condensed based on a group by and then a rank only, instead of the record mode logic in the record mode based change data capture.
As for other cases, the acon configuration should be executed with `load_data` using:
```python
from lakehouse_engine.engine import load_data
acon = {...}
load_data(acon=acon)
```
Example of ACON configuration:
```json
.. include:: ../../../tests/resources/feature/delta_load/group_and_rank/with_duplicates_in_same_file/streaming_delta.json
```

##### Relevant notes:
* This type of delta load with this type of condensation is useful when the source changelog can be condensed based on dates, instead of technical fields like `datapakid`, `record`, `record_mode`, etc., as we see in SAP BW DSOs.An example of such system is Omnihub Tibco orders and deliveries files.