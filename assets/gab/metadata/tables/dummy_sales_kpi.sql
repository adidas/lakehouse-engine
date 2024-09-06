DROP TABLE IF EXISTS `database`.`dummy_sales_kpi`;
CREATE EXTERNAL TABLE `database`.`dummy_sales_kpi` (
  `order_date` DATE COMMENT 'date of the orders',
  `article_id` STRING COMMENT 'article id',
  `amount` INT COMMENT 'quantity/amount sold on this date'
)
USING DELTA
PARTITIONED BY (order_date)
LOCATION 's3://my-data-product-bucket/dummy_sales_kpi'
COMMENT 'Dummy sales KPI (articles sold per date).'
TBLPROPERTIES(
  'lakehouse.primary_key'='article_id, order_date',
  'delta.enableChangeDataFeed'='true'
)
