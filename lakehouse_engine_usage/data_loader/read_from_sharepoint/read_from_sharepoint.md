# Read from Sharepoint

There may be scenarios where data products must ingest curated datasets that business teams maintain directly in Sharepoint, for example exports from external systems or manually maintained reference files.

The `SharepointReader` is a specialized reader module designed to load one or more files from a Sharepoint document library into the lakehouse. It abstracts away the complexity of accessing Sharepoint by:

* Resolving the configured Sharepoint site, drive, and document path.
* Downloading the target file or all files matching a configured pattern into a temporary local location.
* Reading the downloaded file(s) into a Spark DataFrame using the configured format and options.
* Optionally combining multiple files into a single DataFrame (for example, unioning all matching CSV files in a folder) and optionally archiving processed files back to Sharepoint (success and error folders).

!!! note
    📘 Tip: This reader integrates seamlessly into the lakehouse engine’s input step and can be triggered as part of the ACON-based pipeline, just like any other reader module.

!!! warning
    When reading from text-based formats such as CSV, complex data types (arrays, maps, structs) are not preserved in the source file. If your downstream tables expect these types, you must reconstruct them from string columns after ingestion (for example using `from_json` or explicit casts).


### Usage Scenarios

The examples below show how to read data from Sharepoint, ranging from simple single-file reads to more advanced multi-file and large-file scenarios.

1. [Configuration parameters](#1-configuration-parameters)
2. [**Simple:** Read one file from Sharepoint](#2-simple-read-one-file-from-sharepoint)
    1. [Minimal configuration](#i-minimal-configuration)
    2. [With optional configurations](#ii-with-optional-configurations)
3. [**Complex:** Read multiple files from Sharepoint](#3-complex-read-multiple-files-from-sharepoint)
    1. [Read multiple files (standard size)](#i-read-multiple-files-standard-size)
    2. [Read multiple large files with `chunk_size` and CSV options](#ii-read-multiple-large-files-with-chunk_size-and-csv-options)
4. [Delimiter handling](#4-delimiter-handling)
5. [Orchestrating multiple Sharepoint reads (loop pattern)](#5-orchestrating-multiple-sharepoint-reads-loop-pattern)


## 1. Configuration parameters

### The mandatory configuration parameters are:

   - **client_id** (str): azure client ID application, available at the
     Azure Portal -> Azure Active Directory.
   - **tenant_id** (str): tenant ID associated with the Sharepoint site, available at the
     Azure Portal -> Azure Active Directory.
   - **site_name** (str): name of the Sharepoint site where the document library resides.
     Sharepoint URL naming convention is: **https://your_company_name.Sharepoint.com/sites/site_name**
   - **drive_name** (str): name of the document library where the file will be uploaded.
     Sharepoint URL naming convention is: **https://your_company_name.Sharepoint.com/sites/site_name/drive_name**
   - **file_name** (str): name of the file to be read from Sharepoint when
     performing a **single-file** read.
     - In multi-file scenarios, `file_pattern` is typically used instead
       (see examples below).
   - **secret** (str): client secret for authentication, available at the
     Azure Portal -> Azure Active Directory.
   - **local_path** (str): temporary local storage path (Volume) where files are
     downloaded before being read.
     - Ensure the **path ends with "/"**.
     - The **specified sub-folder may be deleted during processing** (for example when
       cleaning up temporary files); it does not perform a recursive delete on parent
       directories.
     - **Avoid using a critical sub-folder.**
   - **api_version** (str): version of the Graph Sharepoint API to be used for operations.
     This defaults to "v1.0".

> 🔐 Authentication details (`client_id`, `secret`, etc.) should be handled
> securely via lakehouse configuration or secret management tools, rather than
> hard-coded in notebooks.

### The optional parameters are:

   - **folder_relative_path** (Optional[str]): relative folder path within the
     document library where the file(s) are located (for example,
     `"incoming/daily_exports"`).
   - **chunk_size** (Optional[int]): size (in bytes) of the file chunks used when
     downloading and archiving files.
     **Default is `5 * 1024 * 1024` (5 MB).**
     Useful when working with large files to avoid memory pressure.
   - **local_options** (Optional[dict]): additional options for customizing the
     **Spark read** from the temporary local file(s) (for example CSV options such as
     `header`, `delimiter`, `encoding`, etc.). See the Spark CSV options link below.
   - **conflict_behaviour** (Optional[str]): behavior to adopt when archiving files
     and a file with the same name already exists in the target location
     (for example, `"replace"`, `"fail"`).
   - **file_pattern** (Optional[str]): pattern to match **multiple files** in
     Sharepoint (for example, `"export_*.csv"`).
     Used by the multi-file reader flow to download and union all matching files.
   - **file_type** (Optional[str]): type of the files to be read from Sharepoint
     (for example, `"csv"`). The reader uses this to decide which Spark data source
     to use when reading from `local_path`.


!!! note
    For more details about the Sharepoint framework, refer to Microsoft's official documentation:

    > 📖[ Microsoft Graph API - Sharepoint](https://learn.microsoft.com/en-us/graph/api/resources/sharepoint?view=graph-rest-1.0)

    > 🛠️ [Graph Explorer Tool](https://developer.microsoft.com/en-us/graph/graph-explorer) -  this tool helps you explore available Sharepoint Graph API functionalities.

    > 📑 [Spark CSV options](https://spark.apache.org/docs/3.5.3/sql-data-sources-csv.html)

## 2. Simple: Read one file from Sharepoint

This section demonstrates both minimal configuration and extended configurations
when using the Sharepoint Reader.

### i. Minimal Configuration

This approach uses only the mandatory parameters needed to connect to Sharepoint
and read a single CSV file into the lakehouse.

**Note:** In this minimal configuration:

- The file is read from the configured `drive_name` (optionally under `folder_relative_path`).
- No explicit archiving or custom CSV options are configured; those are covered in later sections.


```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "csv_read",
            "data_format": "sharepoint",
            "read_type": "batch",
            "sharepoint_opts": {
                "client_id": "dummy_client_id",
                "tenant_id": "dummy_tenant_id",
                "secret": "dummy_secret",
                "site_name": "dummy_site_name",
                "drive_name": "dummy_drive_name",
                "local_path": "/Volumes/my_volume/sharepoint_tmp/",  # must end with "/"
                "folder_relative_path": "dummy_folder",              # optional
                "file_name": "dummy_sales.csv",
                "file_type": "csv",
            },
        },
    ],
    "output_specs": [
        {
            "spec_id": "dummy_output",
            "input_id": "csv_read",
            "data_format": "delta",
            "db_table": "dummy_sales",
            "write_type": "overwrite",
            "location": "s3://my_data_product_bucket/silver/dummy_sales/"
        },
    ],
}

load_data(acon=acon)
```

### ii. With optional configurations

For more control over the read process, additional parameters can be specified on
top of the minimal configuration:

> **archive_enabled (Optional):** Enables archiving of the processed file in
> Sharepoint.
>
> * If `True`, the reader moves the file out of the input folder after the read.
> * Successful reads go to the *success* subfolder; failures go to the *error*
>   subfolder.

> **archive_success_subfolder (Optional):** Name of the subfolder used to store
> successfully processed files (default is `"done"`).
> The folder is created under the same `folder_relative_path` and `drive_name`.

> **archive_error_subfolder (Optional):** Name of the subfolder used to store
> files that failed to be processed (default is `"error"`).

> **local_options (Optional):** Additional options passed to Spark when reading
> the downloaded CSV file(s) from `local_path` (for example `header`, `delimiter`,
> `encoding`, etc.).
> These options can be used in both **single-file** and **multi-file** read modes.

>
> * For available options, refer to:
>   [Apache Spark CSV Options](https://spark.apache.org/docs/3.5.4/sql-data-sources-csv.html).

> **chunk_size (Optional):** Size (in bytes) of the chunks used when
> downloading files.
>
> * Default: `5 * 1024 * 1024` (5 MB).
> * Smaller chunks are safer for very large files or memory-constrained clusters.

```python
from lakehouse_engine.engine import load_data

# Optional CSV options for the local read
LOCAL_OPTIONS = {
    "header": "true",
    "delimiter": ";",
}

acon = {
    "input_specs": [
        {
            "spec_id": "csv_read",
            "data_format": "sharepoint",
            "read_type": "batch",
            "sharepoint_opts": {
                "client_id": "dummy_client_id",
                "tenant_id": "dummy_tenant_id",
                "secret": "dummy_secret",
                "site_name": "dummy_site_name",
                "drive_name": "dummy_drive_name",
                "local_path": "/Volumes/my_volume/sharepoint_tmp/",
                "folder_relative_path": "dummy_simple",
                "file_name": "dummy_sales.csv",
                "file_type": "csv",
                "archive_enabled": True,
                "archive_success_subfolder": "successful",
                "archive_error_subfolder": "with_error",
                "local_options": LOCAL_OPTIONS,
                "chunk_size": 5 * 1024 * 1024,
            },
        },
    ],
    "output_specs": [
        {
            "spec_id": "dummy_output",
            "input_id": "csv_read",
            "data_format": "delta",
            "db_table": "dummy_sales",
            "write_type": "overwrite",
            "location": "s3://my_data_product_bucket/silver/dummy_sales/"
        },
    ],
}

load_data(acon=acon)
```

## 3. Complex: Read multiple files from Sharepoint

In many cases, data in Sharepoint is split across multiple files within a folder or
exported periodically.
The `SharepointReader` can automatically locate and read all matching files based
on a configured pattern, merging them into a single DataFrame.

### i. Read multiple files (standard size)

Use `file_pattern` to match and load multiple files within the same folder.
The reader downloads all matching files into the temporary local folder and
performs a union of their contents before returning the DataFrame.

⚠️ **Schema consistency check:**
All matched files must share the same schema.
If a file with a different schema is encountered, the reader stops the ingestion,
moves that file to the configured *error archive* folder, and logs the event.

> **file_pattern (Optional):** Glob-style pattern for matching files, such as `"export_*.csv"`.

```python
from lakehouse_engine.engine import load_data

acon = {
    "input_specs": [
        {
            "spec_id": "csv_read_multi",
            "data_format": "sharepoint",
            "read_type": "batch",
            "sharepoint_opts": {
                "client_id": "dummy_client_id",
                "tenant_id": "dummy_tenant_id",
                "secret": "dummy_secret",
                "site_name": "dummy_site_name",
                "drive_name": "dummy_drive_name",
                "local_path": "/Volumes/my_volume/sharepoint_tmp/",
                "folder_relative_path": "dummy_sales/daily_exports",
                "file_pattern": "export_*.csv",
                "file_type": "csv",
            },
        },
    ],
    "output_specs": [
        {
            "spec_id": "dummy_output",
            "input_id": "csv_read_multi",
            "data_format": "delta",
            "db_table": "dummy_sales_daily_exports",
            "write_type": "overwrite",
            "location": "s3://my_data_product_bucket/silver/dummy_sales/"
        },
    ],
}

load_data(acon=acon)
```

## ii. Read multiple large files with `chunk_size` and CSV options

When reading multiple large CSV files, the reader can:

- Download each file in chunks (to avoid memory pressure).
- Apply custom CSV read options (delimiter, header, encoding, etc.) before unioning the data.

> **chunk_size (Optional):**
> Size (in bytes) of the chunks used when downloading and archiving files.
> Default is `5 * 1024 * 1024` (5 MB). Increase this for very large files to reduce the number of download operations.

> **local_options (Optional):**
> Spark CSV options used when reading the downloaded files from `local_path`
> (for example `header`, `delimiter`, `encoding`, `quote`, etc.).

```python
from lakehouse_engine.engine import load_data

LOCAL_OPTIONS = {
    "header": "true",
    "delimiter": ";",
    "encoding": "utf-8",
}

acon = {
    "input_specs": [
        {
            "spec_id": "csv_read_multi_large",
            "data_format": "sharepoint",
            "read_type": "batch",
            "sharepoint_opts": {
                "client_id": "dummy_client_id",
                "tenant_id": "dummy_tenant_id",
                "secret": "dummy_secret",
                "site_name": "dummy_site_name",
                "drive_name": "dummy_drive_name",
                "local_path": "/Volumes/my_volume/sharepoint_tmp/",
                "folder_relative_path": "dummy_sales/big_daily_exports/",
                "file_pattern": "big_export_*.csv",
                "file_type": "csv",
                "chunk_size": 50 * 1024 * 1024,  # 50 MB per chunk
                "local_options": LOCAL_OPTIONS,
            },
        },
    ],
    "output_specs": [
        {
            "spec_id": "dummy_output",
            "input_id": "csv_read_multi_large",
            "data_format": "delta",
            "db_table": "dummy_sales_daily_exports",
            "write_type": "overwrite",
        },
    ],
}

load_data(acon=acon)
```

## 4. Delimiter handling

When reading CSV files (single-file or multi-file), the Sharepoint Reader:

- Uses `sep` or `delimiter` from `local_options` as-is if provided
  (no auto-detection in this case).
- If no delimiter is provided, it:
    - Tries to auto-detect one from `; , | \t` using `csv.Sniffer`.
    - Optionally compares the resulting column count with `expected_columns`
      (if set) and logs a warning if they do not match.
    - Falls back to comma (`,`) if detection fails.

Internally, the final delimiter is always passed to Spark as `sep`
(`delimiter` is mapped to `sep` and then removed).

> 💡 Tip: You can use `local_options` (including `sep` / `delimiter`) in both
> single-file and multi-file read modes. When in doubt, set `sep` explicitly.


## 5. Orchestrating multiple Sharepoint reads (loop pattern)

If you need to read from multiple independent Sharepoint locations
(different folders, drives, or file patterns), you can orchestrate a loop in your
notebook and call `load_data` once per configuration.

```python
from lakehouse_engine.engine import load_data

sharepoint_sources = [
    {"folder_relative_path": "dummy_sales/big_daily_exports", "file_pattern": "big_export_*.csv"},
    {"folder_relative_path": "dummy_sales/daily_exports", "file_pattern": "export_*.csv.csv"},
]

for src in sharepoint_sources:
    acon = {
        "input_specs": [
            {
                "spec_id": "csv_read",
                "data_format": "sharepoint",
                "read_type": "batch",
                "sharepoint_opts": {
                    "client_id": "...",
                    "tenant_id": "...",
                    "secret": "...",
                    "site_name": "...",
                    "drive_name": "...",
                    "local_path": "/Volumes/my_volume/sharepoint_tmp/",
                    "folder_relative_path": src["folder_relative_path"],
                    "file_pattern": src["file_pattern"],
                    "file_type": "csv",
                },
            },
        ],
        "output_specs": [
            {
                "spec_id": "output",
                "input_id": "csv_read",
                "data_format": "delta",
                "db_table": "dummy_sales_daily_exports",
                "write_type": "append",
            },
        ],
    }

    load_data(acon=acon)
```

‼️ Caution: excessive parallelism

    - Running too many Sharepoint reads in parallel can trigger MS Graph API
    throttling (for example 429 or 503 responses).
    - Prefer a controlled level of parallelism when orchestrating multiple
    pipelines or loops that read from Sharepoint.
    - Monitor logs and retries to ensure stable performance, especially when
    working with large files or many files at once.

The Lakehouse Engine framework uses retry logic with backoff to mitigate
throttling, but it cannot fully replace sensible limits on concurrency.
