# Write to REST API

REST API writer is an interesting feature to send data from Spark to a REST API within the data pipeline context. It uses the Python requests library to execute the REST calls.

It is possible to configure a few aspects of the writer, like if the payload should be sent via JSON body or via file, or configure additional JSON body parameters to add to the payload generated via Spark.

In the current implementation of the writer, each row will generate a request to the API, so it is important that you prepare your dataframe accordingly (check example below).

## Silver Dummy Sales Write to REST API Example

In this template we will cover the Dummy Sales write to a REST API. An ACON is used to read from bronze, apply silver transformations to prepare the REST api payload and write to the API through the following steps:

1. Definition of how to read data (input data location, read type and data format);
2. Transformation of the data so that we form a payload column per each row.
    **Important Note:** In the current implementation of the writer, each row will generate a request to the API, so `create_payload` is a lakehouse engine custom transformer function that creates a JSON string with the **payload** to be sent to the API. The column name should be exactly **"payload"**, so that the lakehouse engine further processes that column accordingly, in order to correctly write the data to the REST API.
3. Definition of how to write to a REST api (url, authentication, payload format configuration, ...);

For this, the ACON specs are :

- **input_specs** (MANDATORY): specify how to read data;
- **transform specs** (MANDATORY): specify how to transform data to prepare the payload;
- **output_specs** (MANDATORY): specify how to write data to the target.

```python
from lakehouse_engine.engine import load_data

def create_payload(df: DataFrame) -> DataFrame:
    payload_df = payload_df.withColumn(
        "payload",
        lit('{"just a dummy key": "just a dummy value"}')
    )

    return payload_df

acon = {
    "input_specs": [
        {
            "spec_id": "dummy_sales_bronze",
            "read_type": "streaming",
            "data_format": "delta",
            "location": "s3://my_data_product_bucket/bronze/dummy_sales",
        }
    ],
    "transform_specs": [
        {
            "spec_id": "dummy_sales_transform",
            "input_id": "dummy_sales_bronze",
            "transformers": [
                {
                    "function": "custom_transformation",
                    "args": {
                        "custom_transformer": create_payload,
                    },
                }
            ],
        },
    ],
    "output_specs": [
        { 
            "spec_id": "data_to_send_to_api",
            "input_id": "dummy_sales_transform",
            "data_format": "rest_api",
            "options": {
                "rest_api_url": "https://foo.bar.com",
                "rest_api_method": "post",
                "rest_api_basic_auth": {
                    "username": "...",
                    "password": "...",
                },
                "rest_api_is_file_payload": False, # True if payload is to be sent via JSON file instead of JSON body (application/json)
                "rest_api_file_payload_name": "custom_file", # this is the name of the file to be sent in cases where the payload uses file uploads rather than JSON body.
                "rest_api_extra_json_payload": {"x": "y"}
            }
        }
    ],
}

load_data(acon=acon)
```
