# Batch Delta Load Init, Delta and Backfill with Merge

This scenario illustrates the process of implementing a delta load algorithm by first using an ACON to perform an initial load, then another one to perform the regular deltas that will be triggered on a recurrent basis, and finally an ACON for backfilling specific parcels if ever needed.

## Init Load

```python
from lakehouse_engine.engine import load_data

acon = {
  "input_specs": [
    {
      "spec_id": "sales_source",
      "read_type": "batch",
      "data_format": "csv",
      "options": {
        "header": True,
        "delimiter": "|",
        "inferSchema": True
      },
      "location": "file:///app/tests/lakehouse/in/feature/delta_load/record_mode_cdc/backfill/data"
    }
  ],
  "transform_specs": [
    {
      "spec_id": "condensed_sales",
      "input_id": "sales_source",
      "transformers": [
        {
          "function": "condense_record_mode_cdc",
          "args": {
            "business_key": [
              "salesorder",
              "item"
            ],
            "ranking_key_desc": [
              "extraction_timestamp",
              "actrequest_timestamp",
              "datapakid",
              "partno",
              "record"
            ],
            "record_mode_col": "recordmode",
            "valid_record_modes": [
              "",
              "N",
              "R",
              "D",
              "X"
            ]
          }
        }
      ]
    }
  ],
  "output_specs": [
    {
      "spec_id": "sales_bronze",
      "input_id": "condensed_sales",
      "write_type": "merge",
      "data_format": "delta",
      "location": "file:///app/tests/lakehouse/out/feature/delta_load/record_mode_cdc/backfill/data",
      "merge_opts": {
        "merge_predicate": "current.salesorder = new.salesorder and current.item = new.item and current.date <=> new.date"
      }
    }
  ]
}

load_data(acon=acon)
```

##### Relevant Notes

- We can see that even though this is an init load we still have chosen to condense the records through our ["condense_record_mode_cdc"](../../lakehouse_engine/transformers/condensers.html#Condensers.condense_record_mode_cdc) transformer. This is a condensation step capable of handling SAP BW style changelogs based on actrequest_timestamps, datapakid, record_mode, etc...
- In the init load we actually did a merge in this case because we wanted to test locally if a merge with an empty target table works, but you don't have to do it, as an init load usually can be just a full load. If a merge of init data with an empty table has any performance implications when compared to a regular insert remains to be tested, but we don't have any reason to recommend a merge over an insert for an init load, and as said, this was done solely for local testing purposes, you can just use `write_type: "overwrite"`

## Delta Load

```python
from lakehouse_engine.engine import load_data

acon = {
  "input_specs": [
    {
      "spec_id": "sales_source",
      "read_type": "batch",
      "data_format": "csv",
      "options": {
        "header": True,
        "delimiter": "|",
        "inferSchema": True
      },
      "location": "file:///app/tests/lakehouse/in/feature/delta_load/record_mode_cdc/backfill/data"
    },
    {
      "spec_id": "sales_bronze",
      "read_type": "batch",
      "data_format": "delta",
      "location": "file:///app/tests/lakehouse/out/feature/delta_load/record_mode_cdc/backfill/data"
    }
  ],
  "transform_specs": [
    {
      "spec_id": "max_sales_bronze_timestamp",
      "input_id": "sales_bronze",
      "transformers": [
        {
          "function": "get_max_value",
          "args": {
            "input_col": "actrequest_timestamp"
          }
        }
      ]
    },
    {
      "spec_id": "condensed_sales",
      "input_id": "sales_source",
      "transformers": [
        {
          "function": "incremental_filter",
          "args": {
            "input_col": "actrequest_timestamp",
            "increment_df": "max_sales_bronze_timestamp"
          }
        },
        {
          "function": "condense_record_mode_cdc",
          "args": {
            "business_key": [
              "salesorder",
              "item"
            ],
            "ranking_key_desc": [
              "extraction_timestamp",
              "actrequest_timestamp",
              "datapakid",
              "partno",
              "record"
            ],
            "record_mode_col": "recordmode",
            "valid_record_modes": [
              "",
              "N",
              "R",
              "D",
              "X"
            ]
          }
        }
      ]
    }
  ],
  "output_specs": [
    {
      "spec_id": "sales_bronze",
      "input_id": "condensed_sales",
      "write_type": "merge",
      "data_format": "delta",
      "location": "file:///app/tests/lakehouse/out/feature/delta_load/record_mode_cdc/backfill/data",
      "merge_opts": {
        "merge_predicate": "current.salesorder = new.salesorder and current.item = new.item and current.date <=> new.date",
        "delete_predicate": "new.recordmode in ('R','D','X')",
        "insert_predicate": "new.recordmode is null or new.recordmode not in ('R','D','X')"
      }
    }
  ]
}

load_data(acon=acon)
```

##### Relevant Notes

- The merge predicate and the insert, delete or update predicates should reflect the reality of your data, and it's up to each data product to figure out which predicates better match their reality:
    - The merge predicate usually involves making sure that the "primary key" for your data matches.
    .. note::**Performance Tip!!!** Ideally, in order to get a performance boost in your merges, you should also place a filter in your merge predicate (e.g., certain technical or business date in the target table >= x days ago), based on the assumption that the rows in that specified interval will never change in the future. This can drastically decrease the merge times of big tables.
    - The insert, delete and update predicates will always depend on the structure of your changelog, and also how you expect your updates to arrive (e.g., in certain data products you know that you will never get out of order data or late arriving data, while in other you can never ensure that). These predicates should reflect that in order to prevent you from doing unwanted changes to the target delta lake table.
        - For example, in this scenario, we delete rows that have the R, D or X record_mode values, because we know that if after condensing the rows that is the latest status of that row from the changelog, they should be deleted, and we never insert rows with those status (**note**: we use this guardrail in the insert to prevent out of order changes, which is likely not the case in SAP BW).
        - Because the `insert_predicate` is fully optional, in your scenario you may not require that.
    - In this scenario, we don't pass an `update_predicate` in the ACON, because both `insert_predicate` and update_predicate are fully optional, i.e., if you don't pass them the algorithm will update any data that matches the `merge_predicate` and insert any data that does not match it. The predicates in these cases just make sure the algorithm does not insert or update any data that you don't want, as in the late arriving changes scenario where a deleted row may arrive first from the changelog then the update row, and to prevent your target table to have inconsistent data for a certain period of time (it will eventually get consistent when you receive the latest correct status from the changelog though) you can have this guardrail in the insert or update predicates. Again, for most sources this will not happen but sources like Kafka for example cannot 100% ensure order, for example.
    - In order to understand how we can cover different scenarios (e.g., late arriving changes, out of order changes, etc.), please go [here](../../lakehouse_engine_usage/data_loader/streaming_delta_with_late_arriving_and_out_of_order_events.html).
- The order of the predicates in the ACON does not matter, is the logic in the lakehouse engine [DeltaMergeWriter's "_merge" function](../../lakehouse_engine/io/writers/delta_merge_writer.html#DeltaMergeWriter.__init__) that matters.
- Notice the "<=>" operator? In Spark SQL that's the null safe equal.

## Backfilling

```python
from lakehouse_engine.engine import load_data

acon = {
  "input_specs": [
    {
      "spec_id": "sales_source",
      "read_type": "batch",
      "data_format": "csv",
      "options": {
        "header": True,
        "delimiter": "|",
        "inferSchema": True
      },
      "location": "file:///app/tests/lakehouse/in/feature/delta_load/record_mode_cdc/backfill/data"
    },
    {
      "spec_id": "sales_bronze",
      "read_type": "batch",
      "data_format": "delta",
      "location": "file:///app/tests/lakehouse/out/feature/delta_load/record_mode_cdc/backfill/data"
    }
  ],
  "transform_specs": [
    {
      "spec_id": "max_sales_bronze_timestamp",
      "input_id": "sales_bronze",
      "transformers": [
        {
          "function": "get_max_value",
          "args": {
            "input_col": "actrequest_timestamp"
          }
        }
      ]
    },
    {
      "spec_id": "condensed_sales",
      "input_id": "sales_source",
      "transformers": [
        {
          "function": "incremental_filter",
          "args": {
            "input_col": "actrequest_timestamp",
            "increment_value": "20180110120052t",
            "greater_or_equal": True
          }
        },
        {
          "function": "condense_record_mode_cdc",
          "args": {
            "business_key": [
              "salesorder",
              "item"
            ],
            "ranking_key_desc": [
              "extraction_timestamp",
              "actrequest_timestamp",
              "datapakid",
              "partno",
              "record"
            ],
            "record_mode_col": "recordmode",
            "valid_record_modes": [
              "",
              "N",
              "R",
              "D",
              "X"
            ]
          }
        }
      ]
    }
  ],
  "output_specs": [
    {
      "spec_id": "sales_bronze",
      "input_id": "condensed_sales",
      "write_type": "merge",
      "data_format": "delta",
      "location": "file:///app/tests/lakehouse/out/feature/delta_load/record_mode_cdc/backfill/data",
      "merge_opts": {
        "merge_predicate": "current.salesorder = new.salesorder and current.item = new.item and current.date <=> new.date",
        "delete_predicate": "new.recordmode in ('R','D','X')",
        "insert_predicate": "new.recordmode is null or new.recordmode not in ('R','D','X')"
      }
    }
  ]
}

load_data(acon=acon)
```

##### Relevant Notes

- The backfilling process depicted here is fairly similar to the init load, but it is relevant to highlight  by using a static value (that can be modified accordingly to the backfilling needs) in the [incremental_filter](../../lakehouse_engine/transformers/filters.html#Filters.incremental_filter) function.
- Other relevant functions for backfilling may include the [expression_filter](../../lakehouse_engine/transformers/filters.html#Filters.expression_filter) function, where you can use a custom SQL filter to filter the input data.