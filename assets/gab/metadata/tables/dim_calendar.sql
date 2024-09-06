DROP TABLE IF EXISTS  `database`.dim_calendar;
CREATE EXTERNAL TABLE `database`.dim_calendar (
  calendar_date DATE COMMENT 'Full calendar date in the format yyyyMMdd.',
  day_en STRING COMMENT 'Name of the day of the week.',
  weeknum_mon INT COMMENT 'Week number where the week starts on Monday.',
  weekstart_mon DATE COMMENT 'First day of the week where the week starts on Monday.',
  weekend_mon DATE COMMENT 'Last day of the week where the week starts on Monday.',
  weekstart_sun DATE COMMENT 'First day of the week where the week starts on Sunday.',
  weekend_sun DATE COMMENT 'Last day of the week where the week starts on Sunday.',
  month_start DATE COMMENT 'First day of the Month.',
  month_end DATE COMMENT 'Last day of the Month.',
  quarter_start DATE COMMENT 'First day of the Quarter.',
  quarter_end DATE COMMENT 'Last day of the Quarter.',
  year_start DATE COMMENT 'First day of the Year.',
  year_end DATE COMMENT 'Last day of the Year.'
)
USING DELTA
LOCATION 's3://my-data-product-bucket/dim_calendar'
COMMENT 'This table stores the calendar information.'
TBLPROPERTIES(
  'lakehouse.primary_key'='calendar_date',
  'delta.enableChangeDataFeed'='false'
)