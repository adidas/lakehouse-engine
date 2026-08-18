# pip install lakehouse-engine

from lakehouse_engine.engine import load_data


acon = {
    "input_specs": [
        {
            "spec_id": "table_tennis_matches_source",
            "read_type": "batch",
            "data_format": "csv",
            "options": {
                "header": True,
                "delimiter": ",",
                "dateFormat": "yyyy-MM-dd HH:mm:ss",
            },
            "location": "s3://my-data-product-bucket/sports/table_tennis/inbound",
        }
    ],
    "output_specs": [
        {
            "spec_id": "table_tennis_matches_bronze",
            "input_id": "table_tennis_matches_source",
            "data_format": "delta",
            "write_type": "overwrite",
            "location": "s3://my-data-product-bucket/sports/table_tennis/bronze/"
        }
    ]
}

load_data(acon=acon)
