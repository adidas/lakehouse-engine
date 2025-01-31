# Read from Dataframe

!!! danger
    Don't use this feature if the Lakehouse Engine already has a supported data format for your use case, as in that case it is preferred to use the dedicated data formats which are more extensively tested and predictable. Check the supported data formats [here](../../../reference/packages/core/definitions.md#packages.core.definitions.InputFormat).

Reading from a Spark DataFrame is very simple using our framework. You just need to define the input_specs as follows: 

```python
{
    "input_spec": {
        "spec_id": "my_df",
        "read_type": "batch",
        "data_format": "dataframe",
        "df_name": df,
    }
}
```

!!! note "**Why is it relevant?**"
    With this capability of reading a dataframe you can deal with sources that do not yet officially have a reader (e.g., REST api, XML files, etc.).