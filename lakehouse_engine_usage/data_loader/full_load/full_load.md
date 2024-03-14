# Full Load

This scenario reads CSV data from a path and writes in full to another path with delta lake files.

##### Relevant notes

- This ACON infers the schema automatically through the option `inferSchema` (we use it for local tests only). This is usually not a best practice using CSV files, and you should provide a schema through the InputSpec variables `schema_path`, `read_schema_from_table` or `schema`.
- The `transform_specs` in this case are purely optional, and we basically use the repartition transformer to create one partition per combination of date and customer. This does not mean you have to use this in your algorithm.
- A full load is also adequate for an init load (initial load).

As for other cases, the acon configuration should be executed with `load_data` using:
```python
from lakehouse_engine.engine import load_data
acon = {...}
load_data(acon=acon)
```
Example of ACON configuration:
```json
.. include:: ../../../tests/resources/feature/full_load/full_overwrite/batch.json
```