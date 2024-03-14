# Flatten Schema and Explode Columns

Related with schema, we can make two kind of operations:

* **Flatten Schema**: transformer named "flatten_schema" used to flatten the schema of dataframe.
    * Parameters to be defined:
        * max_level: 2 => this sets the level until you want to flatten the schema.
        * shorten_names: True => this flag is when you want to shorten the name of the prefixes of the fields.
        * alias: True => this flag is used when you want to define a prefix for the column to be flattened.
        * num_chars: 7 => this sets the number of characters to consider when shortening the names of the fields.
        * ignore_cols: True => this list value should be set to specify the columns you don't want to flatten.


* **Explode Columns**: transformer named "explode_columns" used to explode columns with types ArrayType and MapType. 
    * Parameters to be defined:
        * explode_arrays: True => this flag should be set to true to explode all array columns present in the dataframe.
        * array_cols_to_explode: ["sample_col"] => this list value should be set when to specify the array columns desired to explode.
        * explode_maps: True => this flag should be set to true to explode all map columns present in the dataframe.
        * map_cols_to_explode: ["map_col"] => this list value should be set when to specify the map columns desired to explode.
    * Recommendation: use array_cols_to_explode and map_cols_to_explode to specify the columns desired to explode and do not do it for all of them.


The below scenario of **flatten_schema** is transforming one or more columns and dividing the content nested in more columns, as desired. We defined the number of levels we want to flatten in the schema, regarding the nested values. In this case, we are just setting `max_level` of `2`.
As for other cases, the acon configuration should be executed with `load_data` using:
```python
from lakehouse_engine.engine import load_data
acon = {...}
load_data(acon=acon)
```
Example of ACON configuration:
```json
.. include:: ../../../tests/resources/feature/transformations/column_reshapers/flatten_schema/batch.json
```

The scenario of **explode_arrays** is transforming the arrays columns in one or more rows, depending on the number of elements, so, it replicates the row for each array value. In this case we are using explode to all array columns, using `explode_arrays` as `true`.
As for other cases, the acon configuration should be executed with `load_data` using:
```python
from lakehouse_engine.engine import load_data
acon = {...}
load_data(acon=acon)
```
Example of ACON configuration:
```json
.. include:: ../../../tests/resources/feature/transformations/column_reshapers/explode_arrays/batch.json
```

The scenario of **flatten_and_explode_arrays_and_maps** is using `flatten_schema` and `explode_columns` to have the desired output. In this case, the desired output is to flatten all schema and explode maps and arrays, even having an array inside a struct. Steps:

    1. In this case, we have an array column inside a struct column, so first we need to use the `flatten_schema` transformer to extract the columns inside that struct;
    2. Then, we are able to explode all the array columns desired and map columns, using `explode_columns` transformer.
    3. To be able to have the map column in 2 columns, we use again the `flatten_schema` transformer.

As for other cases, the acon configuration should be executed with `load_data` using:
```python
from lakehouse_engine.engine import load_data
acon = {...}
load_data(acon=acon)
```
Example of ACON configuration:
```json
.. include:: ../../../tests/resources/feature/transformations/column_reshapers/flatten_and_explode_arrays_and_maps/batch.json
```
