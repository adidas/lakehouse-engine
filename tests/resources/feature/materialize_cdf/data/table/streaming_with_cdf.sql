CREATE TABLE test_db.streaming_with_cdf (salesorder INT, item INT, date INT, customer STRING, article STRING, amount INT)
USING DELTA
PARTITIONED BY (date)
LOCATION 'file:///app/tests/lakehouse/out/feature/materialize_cdf/streaming_with_cdf'
TBLPROPERTIES(
  'delta.enableChangeDataFeed'='true'
)