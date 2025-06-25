# Write to Sharepoint

There may be scenarios where data products must deliver curated datasets to external platforms like SharePoint, 
often to serve business users or reporting tools outside the lakehouse environment.

The SharePointWriter is a specialized writer module designed to export a single file from the lakehouse to a SharePoint document library.
It handles the complexities of the export by:

* Writing the dataset to a temporary local file. 
* Uploading that file to the configured SharePoint location using authenticated APIs. 
* Since it is scoped to handle only a single file per execution, any logic for splitting or generating multiple files must be implemented within your notebook prior to invoking the writer.

!!! note 
    📘 Tip: This writer integrates seamlessly into the lakehouse engine's output step and can be triggered as part of the ACON-based pipeline, just like any other writer module.

!!! warning
    **CSV files do not support complex data types such as array, map, or struct.**
    If these fields exist in the dataset, they must be converted to string (e.g., via to_json(), cast, or similar) before using the SharePoint Writer, as **these types will cause the export to fail.**

### Usage Scenarios

The examples below show how to write data to SharePoint, ranging from simple single-DataFrame writes to more complex multi-DataFrame workflows.

1. [Configuration parameters](#1-configuration-parameters)
2. [**Simple:** Write one Dataframe to Sharepoint](#2-simple-write-one-dataframe-to-sharepoint)
    1. [Minimal configuration](#i-minimal-configuration)
    2. [With optional configurations](#ii-with-optional-configurations)
3. [**Complex:** Write multiple Dataframes to Sharepoint](#3-complex-write-multiple-dataframes-to-sharepoint)
    1. [Example: Partitioning function](#i-example-partitioning-function)
    2. [Example: Detect Unsupported Column Types](#ii-detect-unsupported-columns-types)
    2. [Without parallelism (sequential processing)](#iii-without-parallelism-sequential-processing)
    3. [With parallelism (optimized for efficiency)](#iv-complex---with-parallelism-optimized-for-efficiency)

## 1. Configuration parameters

### The mandatory configuration parameters are:

   - **client_id** (str): azure client ID application, available at the
     Azure Portal -> Azure Active Directory.
   - **tenant_id** (str): tenant ID associated with the SharePoint site, available at the
     Azure Portal -> Azure Active Directory.
   - **site_name** (str): name of the SharePoint site where the document library resides.
     Sharepoint URL naming convention is: **https://your_company_name.sharepoint.com/sites/site_name**
   - **drive_name** (str): name of the document library where the file will be uploaded.
     Sharepoint URL naming convention is: **https://your_company_name.sharepoint.com/sites/site_name/drive_name**
   - **file_name** (str): name of the file to be uploaded to local path and to SharePoint.
   - **secret** (str): client secret for authentication, available at the
     Azure Portal -> Azure Active Directory.
   - **local_path** (str): Temporary local storage path for the file before uploading.
     - Ensure the **path ends with "/"**.
     - Note: The **specified sub-folder is deleted during the process**; it does not perform a recursive
     delete on parent directories.
     - **Avoid using a critical sub-folder.**
   - **api_version** (str): version of the Graph SharePoint API to be used for operations.
     This defaults to "v1.0".

### The optional parameters are:

   - **folder_relative_path** (Optional[str]): relative folder path within the document
       library to upload the file.
   - **chunk_size** (Optional[int]): Optional; size (in Bytes) of the file chunks for
       uploading to SharePoint. **Default is 100 Mb.**
   - **local_options** (Optional[dict]): Optional; additional options for customizing
       write to csv action to local path. You can check the available options
       below.
   - **conflict_behaviour** (Optional[str]): Optional; behavior to adopt in case
       of a conflict (e.g., 'replace', 'fail').

!!! note
    For more details about the SharePoint framework, refer to Microsoft's official documentation:
    
    > 📖[ Microsoft Graph API - SharePoint](https://learn.microsoft.com/en-us/graph/api/resources/sharepoint?view=graph-rest-1.0)
    
    > 🛠️ [Graph Explorer Tool](https://developer.microsoft.com/en-us/graph/graph-explorer) -  this tool helps you explore available SharePoint Graph API functionalities.

    > 📑 [Spark CSV options](https://spark.apache.org/docs/3.5.3/sql-data-sources-csv.html)

## 2. Simple: Write one Dataframe to Sharepoint

This section demonstrates both minimal configuration and extended configurations
when using the SharePoint Writer.

### i. Minimal Configuration

This approach uses only the mandatory parameters, making it the quickest way to write a DataFrame to SharePoint.

**Note:** With minimal configurations, not even the header is written on the table. Furthermore, the file is
written on the Sharepoint Drive root folder.

```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "dummy_input",
            "read_type": "batch",
            "data_format": "delta",
            "db_table": "dummy_sales",
        },
    ],
    "output_specs": [
        {
            "spec_id": "dummy_output",
            "input_id": "dummy_input",
            "data_format": "sharepoint",
            "sharepoint_opts": {
                "client_id": "dummy_client_id",
                "tenant_id": "dummy_tenant_id",
                "secret": "dummy_secret",
                "site_name": "dummy_site_name",
                "drive_name": "dummy_drive_name",
                "local_path": "s3://my_data_product_bucket/silver/dummy_sales/",  # this path must end with an "/"
                "file_name": "dummy_sales",
            },
        },
    ],
}

load_data(acon=acon)
```

### ii. With Optional Configurations

For more control over the upload process, additional parameters can be specified:

>**folder_relative_path (Optional):** Defines the subfolder inside the SharePoint drive
where the file should be stored.
> 
> ‼️ **Important:** The drive within the site acts as the root.
>
> **Example:**
> 
>   * Site Name: "dummy_sharepoint"
>   * Drive Name: "dummy_drive"
>   * Folder Path: "dummy/test/"
>   * File Name: "test.csv"
>   * Final Destination: "dummy_sharepoint/dummy_drive/dummy/test/test.csv"

> **chunk_size (Optional):** Defines the file chunk size (in bytes) for uploading.
>
> * Default: 100 MB (Recommended unless handling large files).
> * Larger chunk sizes can improve performance but may increase memory usage.

> **local_options (Optional):** Additional options for writing the DataFrame to a CSV file before upload.
>
> * For available options, refer to: [Apache Spark CSV Options](https://spark.apache.org/docs/3.5.4/sql-data-sources-csv.html).

> **conflict_behaviour (Optional):** Determines the action taken if a file with the same name already exists.
>
> * Possible values: "replace", "fail", "rename", etc.
> * Refer to Microsoft’s documentation: [Drive Item Conflict Behavior](https://learn.microsoft.com/en-us/dynamics365/business-central/application/system-application/enum/system.integration.graph.graph-conflictbehavior).

```python
from lakehouse_engine.engine import load_data

# Set the optional parameters
LOCAL_OPTIONS = {"mode": "overwrite", "header": "true"}

acon = {
    "input_specs": [
        {
            "spec_id": "dummy_input",
            "read_type": "batch",
            "data_format": "delta",
            "db_table": "dummy_sales",
        },
    ],
    "transform_specs": [
        {
            "spec_id": "dummy_transform",
            "input_id": "dummy_input",
            "transformers": [
                {
                    "function": "add_current_date",
                    "args": {"output_col": "extraction_timestamp"},
                },  # Add a new column with the current date if needed
                {
                    "function": "expression_filter",
                    "args": {"exp": "customer = 'customer 1'"},
                },  # Filter the data if needed
            ],
        },
    ],
    "output_specs": [
        {
            "spec_id": "dummy_output",
            "input_id": "dummy_transform",
            "data_format": "sharepoint",
            "sharepoint_opts": {
                "client_id": "dummy_client_id",
                "tenant_id": "dummy_tenant_id",
                "secret": "dummy_secret",
                "site_name": "dummy_site_name",
                "drive_name": "dummy_drive_name",
                "local_path": "s3://my_data_product_bucket/silver/dummy_sales/",  # this path must end with an "/"
                "file_name": "dummy_sales",
                "folder_relative_path": "dummy_simple",  # writes file in the folder ./dummy_simple
                "local_options": LOCAL_OPTIONS,
                "chunk_size": 300 * 1024 * 1024,  # 300 MB
            },
        },
    ],
}

load_data(acon=acon)
```

## 3. Complex: Write multiple Dataframes to Sharepoint

This scenario illustrates how to write multiple files to Sharepoint within a loop.
Some use cases may require uploading files categorized by season, customer type, product category, etc.,
depending on the business needs.

Partitioning the data ensures better organization and optimized file management in SharePoint.

!!!warning
    ‼️ **Caution: Excessive Parallelism!**

    * Too many simultaneous uploads can trigger Graph API throttling, leading to 503 (Service Unavailable) errors.
    * Use a controlled level of parallelism (limit concurrent uploads) **if necessary**.
        * [Coalesce](https://spark.apache.org/docs/3.5.3/sql-performance-tuning.html#coalesce-hints-for-sql-queries) allows you to control Spark's parallelism.
    * **As the size of the files increases so does this concern,** so it’s important to test and monitor upload
    processes to avoid service disruptions and ensure smooth performance.

**Neverthless, a stress test with over 50 partition files with > 4GB each** was performed and parallelism
issues were not detected.
The Lakehouse Engine Framework uses a **exponential backoff retry logic to avoid throttling** issues.

### i. Example: Partitioning function

This function is a mere example on how to fetch the distinct of a column from a given table.\
It is not part of the lakehouse_engine framework.

```python
def get_partitions(
partition: str, bucket: Optional[str] = None, table: Optional[str] = None, filter_expression: Optional[str] = None
) -> List[Dict[str, str]]:
"""Fetch distinct values from a given partition column in a table or bucket.

    Parameters
    ----------
    partition : str
        The name of the partition column.
    bucket : Optional[str], default=None
        The path to the S3 bucket (if applicable).
    table : Optional[str], default=None
        The name of the table (if applicable).
    filter_expression : Optional[str], default=None
        A filter condition to apply.

    Returns
    -------
    List[Dict[str, str]]
        A list of dictionaries with unique partition values.
    """
    if not bucket and not table:
        raise ValueError("Either 'bucket' or 'table' must be provided")

    df = spark.read.format("delta").load(bucket) if bucket else spark.table(table)

    partitions = df.select(partition).distinct()

    if filter_expression:
        partitions = partitions.filter(filter_expression)

    return [{partition: row[partition]} for row in partitions.collect()]
```

### ii. Detect unsupported columns types

This function exemplifies how to detect unsupported .csv column types.
It is not part of the lakehouse_engine framework.

```python
def detect_array_or_struct_fields(df: DataFrame) -> Dict[str, str]:
"""
Detect fields in a DataFrame that are arrays, structs, or maps.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        Dict[str, str]: A dictionary with field names as keys and their types ('array', 'struct', or 'map') as values.
    """
    field_types = {}
    type_mapping = {ArrayType: "StringType", StructType: "StringType", MapType: "StringType"}

    for field in df.schema.fields:
        for data_type, type_name in type_mapping.items():
            if isinstance(field.dataType, data_type):
                field_types[field.name] = type_name
                break
    return field_types
```

### iii. Without parallelism (sequential processing)

```python
from lakehouse_engine.engine import load_data

# Set the optional parameters
LOCAL_OPTIONS = {"mode": "overwrite", "header": "true"}

# Set the partition column
PARTITION = "customer"

# Fetch distinct values from the partition column
partitions = get_partitions(partition=PARTITION, table="dummy_sales")

# Sort the distinct values to ensure the correct order of the files
# Note:
#   - If an error occurs during the process, by sorting beforehand, you guarantee the correct order of the files.
#   - It may come in handy if you want to restart the process (starting on a given file).
partitions.sort(key=lambda x: x["customer"])

for partition in partitions:
    acon = {
        "input_specs": [
            {
                "spec_id": "dummy_input",
                "read_type": "batch",
                "data_format": "delta",
                "db_table": "dummy_sales",
            },
        ],
        "transform_specs": [
            {
                "spec_id": "dummy_transform",
                "input_id": "dummy_input",
                "transformers": [
                    {"function": "add_current_date", "args": {"output_col": "extraction_timestamp"}},
                    {"function": "expression_filter", "args": {"exp": f"customer = '{partition['customer']}'"}},
                    {
                        "function": "coalesce",
                        "args": {"num_partitions": 1},
                    },  # Enforce that only 1 file is written - eliminating the parallelism
                ],
            },
        ],
        "output_specs": [
            {
                "spec_id": "dummy_output",
                "input_id": "dummy_transform",
                "data_format": "sharepoint",
                "sharepoint_opts": {
                    "client_id": "dummy_client_id",
                    "tenant_id": "dummy_tenant_id",
                    "secret": "dummy_secret",
                    "site_name": "dummy_site_name",
                    "drive_name": "dummy_drive_name",
                    "local_path": "s3://my_data_product_bucket/silver/dummy_sales/",  # this path must end with an "/"
                    "folder_relative_path": "dummy_complex/wo_parallelism",
                    "file_name": f"dummy_sales_{partition['customer']}",
                    "local_options": LOCAL_OPTIONS,
                    "chunk_size": 200 * 1024 * 1024,  # 200 MB
                },
            },
        ],
    }

load_data(acon=acon)
```

### iv. Complex - With parallelism (optimized for efficiency)

```python
from lakehouse_engine.engine import load_data

# Set the optional parameters
LOCAL_OPTIONS = {"mode": "overwrite", "header": "true"}

# Set the partition column
PARTITION = "customer"

# Fetch distinct values from the partition column
partitions = get_partitions(partition=PARTITION, table="dummy_sales")

# Detect array, struct or map fields which cannot be written to .csv files
columns_to_cast = detect_array_or_struct_fields(spark.sql(f"SELECT * FROM {dummy_sales}"))

# Sort the distinct values to ensure the correct order of the files
# Note:
#   - If an error occurs during the process, by sorting beforehand, you guarantee the correct order of the files.
#   - It may come in handy if you want to restart the process (starting on a given file).
partitions.sort(key=lambda x: x["customer"])

for partition in partitions:
    acon = {
        "input_specs": [
            {
                "spec_id": "dummy_input",
                "read_type": "batch",
                "data_format": "delta",
                "db_table": "dummy_sales",
            },
        ],
        "transform_specs": [
            {
                "spec_id": "dummy_transform",
                "input_id": "dummy_input",
                "transformers": [
                    {"function": "add_current_date", "args": {"output_col": "extraction_timestamp"}},
                    {"function": "expression_filter", "args": {"exp": f"customer = '{partition['customer']}'"}},
                    # Coalesce removed guaranteeing maximum parallelism
                    {"function": "cast", "args": {"cols": columns_to_cast}}, # Cast unsupported column types
                ],
            },
        ],
        "output_specs": [
            {
                "spec_id": "dummy_output",
                "input_id": "dummy_transform",
                "data_format": "sharepoint",
                "sharepoint_opts": {
                    "client_id": "dummy_client_id",
                    "tenant_id": "dummy_tenant_id",
                    "secret": "dummy_secret",
                    "site_name": "dummy_site_name",
                    "drive_name": "dummy_drive_name",
                    "local_path": "s3://my_data_product_bucket/silver/dummy_sales/",  # this path must end with an "/"
                    "folder_relative_path": "dummy_complex/with_parallelism",
                    "file_name": f"dummy_sales_{partition['customer']}",
                    "local_options": LOCAL_OPTIONS,
                    "chunk_size": 200 * 1024 * 1024,  # 200 MB
                },
            },
        ],
    }

load_data(acon=acon)
```

### Relevant Notes

- Multi-file export is not supported. For such use cases, loop through files manually and invoke SharePointWriter per file. 
- Authentication details should be handled securely via lakehouse configuration or secret management tools.