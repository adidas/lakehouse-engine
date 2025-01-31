# Extract from SFTP

Secure File Transfer Protocol (SFTP) is a file protocol for transferring files over the web.

This feature is available in the Lakehouse Engine with the purpose of having a mechanism to read data directly from SFTP directories without moving those files manually/physically to a S3 bucket.

The engine uses Pandas to read the files and converts them into a Spark dataframe, which makes the available resources of an Acon usable, such as `dq_specs`, `output_specs`, `terminator_specs` and `transform_specs`.

Furthermore, this feature provides several filters on the directories that makes easier to control the extractions.


#### **Introductory Notes**:

There are important parameters that must be added to **input specs** in order to make the SFTP extraction work properly:


!!! note "**Read type**"
    The engine supports only **BATCH** mode for this feature.


**sftp_files_format** - File format that will be used to read data from SFTP. **The engine supports: CSV, FWF, JSON and XML**.

**location** - The SFTP directory to be extracted. If it is necessary to filter a specific file, it can be made using the `file_name_contains` option.

**options** - Arguments used to set the Paramiko SSH client connection (hostname, username, password, port...), set the filter to retrieve files and set the file parameters (separators, headers, cols...). For more information about the file parameters, please go to the Pandas link in the useful links section.

The options allowed are:

| Property type                 | Detail                   | Example                                                                | Comment                                                                                                                                                                                                                                                                                               |
|-------------------------------|--------------------------|------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Connection                    | add_auto_policy(str)     | true of false                                                          | Indicates to allow an SFTP connection using no host key. When a connection attempt is being made using no host key, then the engine will throw an exception if the auto_add_policy property is false. The purpose of this flag is to make the user conscientiously choose a lesser secure connection. |
| Connection                    | key_type (str)           | "Ed25519" or "RSA"                                                     | Indicates the key type to be used for the connection (SSH, Ed25519).                                                                                                                                                                                                                                  |
| Connection                    | key_filename (str)       | "/path/to/private_key/private_key.ppk"                                 | The filename, or list of filenames, of optional private(keys), and/or certs to try for authentication. It must be used with a pkey in order to add a policy. If a pkey is not provided, then use `add_auto_policy`.                                                                                   |
| Connection                    | pkey (str)               | "AAAAC3MidD1lVBI1NTE5AAAAIKssLqd6hjahPi9FBH4GPDqMqwxOMsfxTgowqDCQAeX+" | Value to use for the host key when connecting to the remote SFTP server.                                                                                                                                                                                                                              |
| Filter                        | date_time_gt (str)       | "1900-01-01" or "1900-01-01 08:59:59"                                  | Filter the files greater than the string datetime formatted as "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS"                                                                                                                                                                                                  |
| Filter                        | date_time_lt (str)       | "3999-12-31" or "3999-12-31 20:59:59"                                  | Filter the files lower than the string datetime formatted as "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS"                                                                                                                                                                                                    |
| Filter                        | earliest_file (bool)     | true or false                                                          | Filter the earliest dated file in the directory.                                                                                                                                                                                                                                                      |
| Filter                        | file_name_contains (str) | "part_of_filename"                                                     | Filter files when match the pattern.                                                                                                                                                                                                                                                                  |
| Filter                        | latest_file (bool)       | true or false                                                          | Filter the most recent dated file in the directory.                                                                                                                                                                                                                                                   |
| Read data from subdirectories | sub_dir (bool)           | true or false                                                          | The engine will search files into subdirectories of the **location**. It will consider one level below the root location given.<br>When `sub_dir` is used with **latest_file/earliest_file** argument, the engine will retrieve the latest/earliest file for each subdirectory.                       |
| Add metadata info             | file_metadata (bool)     | true or false                                                          | When this option is set as True, the dataframe retrieves the **filename with location** and the **modification_time** from the original files in sftp. It attaches these two columns adding the information to respective records.                                                                    |

**Useful Info & Links**:
1. [Paramiko SSH Client](https://docs.paramiko.org/en/latest/api/client.html)
2. [Pandas documentation](https://pandas.pydata.org/docs/reference/io.html)


## Scenario 1
The scenario below shows the extraction of a CSV file using most part of the available filter options. Also, as an example, the column "created_on" is created in the transform_specs in order to store the processing date for every record. As the result, it will have in the output table the original file date (provided by the option `file_metadata`) and the processing date from the engine.

For an incremental load approach, it is advised to use the "modification_time" column created by the option `file_metadata`. Since it has the original file date of modification, this date can be used in the logic to control what is new and has been changed recently.

!!! note
    Below scenario uses **"add_auto_policy": true**, which is **not recommended**.

```python
from lakehouse_engine.engine import load_data

acon = {
  "input_specs": [
      {
          "spec_id": "sftp_source",
          "read_type": "batch",
          "data_format": "sftp",
          "sftp_files_format": "csv",
          "location": "my_sftp_data_path",
          "options": {
              "hostname": "my_sftp_hostname",
              "username": "my_sftp_username",
              "password": "my_sftp_password",
              "port": "my_port",
              "add_auto_policy": True,
              "file_name_contains": "test_pattern",
              "args": {"sep": "|"},
              "latest_file": True,
              "file_metadata": True
          }
      },
  ],
  "transform_specs": [
      {
          "spec_id": "sftp_transformations",
          "input_id": "sftp_source",
          "transformers": [
              {
                  "function": "with_literals",
                  "args": {"literals": {"created_on": datetime.now()}},
              },
          ],
      },
  ],
  "output_specs": [
    {
      "spec_id": "sftp_bronze",
      "input_id": "sftp_transformations",
      "write_type": "append",
      "data_format": "delta",
      "location": "s3://my_path/dummy_table"
    }
  ]
}

load_data(acon=acon)
```

## Scenario 2
The following scenario shows the extraction of a JSON file using an RSA pkey authentication instead of auto_add_policy. The engine supports Ed25519Key and RSA for pkeys.

For the pkey file location, it is important to have the file in a location accessible by the cluster. This can be achieved either by mounting the location or with volumes.

!!! note
    This scenario uses a more secure authentication, thus it is the recommended option, instead of the previous scenario.

```python
from lakehouse_engine.engine import load_data

acon = {
  "input_specs": [
      {
          "spec_id": "sftp_source",
          "read_type": "batch",
          "data_format": "sftp",
          "sftp_files_format": "json",
          "location": "my_sftp_data_path",
          "options": {
              "hostname": "my_sftp_hostname",
              "username": "my_sftp_username",
              "password": "my_sftp_password",
              "port": "my_port",
              "key_type": "RSA",
              "key_filename": "dbfs_mount_location/my_file_key.ppk",
              "pkey": "my_key",
              "latest_file": True,
              "file_metadata": True,
              "args": {"lines": True, "orient": "columns"},
          },
      },
  ],
  "transform_specs": [
      {
          "spec_id": "sftp_transformations",
          "input_id": "sftp_source",
          "transformers": [
              {
                  "function": "with_literals",
                  "args": {"literals": {"lh_created_on": datetime.now()}},
              },
          ],
      },
  ],
  "output_specs": [
    {
      "spec_id": "sftp_bronze",
      "input_id": "sftp_transformations",
      "write_type": "overwrite",
      "data_format": "delta",
      "location": "s3://my_path/dummy_table"
    }
  ]
}

load_data(acon=acon)
```