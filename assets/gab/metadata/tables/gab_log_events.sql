DROP TABLE IF EXISTS `database`.`gab_log_events`;
CREATE EXTERNAL TABLE `database`.`gab_log_events`
(
`run_start_time` TIMESTAMP COMMENT 'Run start time for the use case',
`run_end_time` TIMESTAMP COMMENT 'Run end time for the use case',
`input_start_date` TIMESTAMP COMMENT 'The start time set for the use case process',
`input_end_date` TIMESTAMP COMMENT 'The end time set for the use case process',
`query_id` STRING COMMENT 'Query ID for the use case',
`query_label` STRING COMMENT 'Query label for the use case',
`cadence` STRING COMMENT 'This field stores the cadence of data granularity (Day/Week/Month/Quarter/Year)',
`stage_name` STRING COMMENT 'Intermediate stage',
`stage_query` STRING COMMENT 'Query run as part of stage',
`status` STRING COMMENT 'Status of the stage',
`error_code` STRING COMMENT 'Error code'
)
USING DELTA
PARTITIONED BY (query_id)
LOCATION 's3://my-data-product-bucket/gab_log_events'
COMMENT 'This table stores the log for all use cases in gab'
TBLPROPERTIES(
  'lakehouse.primary_key'='run_start_time,query_id,stage_name',
  'delta.enableChangeDataFeed'='false'
)