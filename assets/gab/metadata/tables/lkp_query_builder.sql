DROP TABLE IF EXISTS `database`.`lkp_query_builder`;
CREATE EXTERNAL TABLE `database`.`lkp_query_builder`
(
`query_id` INT COMMENT 'Query ID for the use case which is a sequence of numbers',
`query_label` STRING COMMENT 'Summarized description of the use case',
`query_type` STRING COMMENT 'Type of use case based on region',
`mappings` STRING COMMENT 'Dictionary of mappings for dimensions and metrics',
`intermediate_stages` STRING COMMENT 'All the stages and their configs such as storageLevel repartitioning date columns',
`recon_window` STRING COMMENT 'Configurations for Cadence and Reconciliation Windows',
`timezone_offset` INT COMMENT 'Timezone offsets can be configured by a positive or negative integer',
`start_of_the_week` STRING COMMENT 'Sunday or Monday can be configured as the start of the week',
`is_active` STRING COMMENT 'Active Flag - Can be set to Y or N',
`queue` STRING COMMENT 'Can be set to High/Medium/Low based on the cluster computation requirement',
`lh_created_on` TIMESTAMP COMMENT 'This field stores the created_on in lakehouse'
)
USING DELTA
LOCATION 's3://my-data-product-bucket/lkp_query_builder'
COMMENT 'This table stores the configuration for the gab framework'
TBLPROPERTIES(
  'lakehouse.primary_key'='query_id',
  'delta.enableChangeDataFeed'='false'
)