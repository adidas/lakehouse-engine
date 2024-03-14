# Filtered Full Load

This scenario is very similar to the [full load](../../lakehouse_engine_usage/data_loader/full_load.html), but it filters the data coming from the source, instead of doing a complete full load.
As for other cases, the acon configuration should be executed with `load_data` using:
```python
from lakehouse_engine.engine import load_data
acon = {...}
load_data(acon=acon)
```
Example of ACON configuration:
```json
.. include:: ../../../tests/resources/feature/full_load/with_filter/batch.json
```

##### Relevant notes:

* As seen in the ACON, the filtering capabilities are provided by a transformer called `expression_filter`, where you can provide a custom Spark SQL filter.