-- New table manager test table, to check if new parser works as expected and deals well with different delimiters (;).
-- The parser must be able to deal with the delimiters that are inside of "", '', --, /* */.
CREATE TABLE test_db.DummyTableBronzeComplexDefaultScenario1
    (
        id INT COMMENT 'id with special (< characters ;',
        col1 STRING COMMENT 'col1 with >) special character " and ;',
        col2 INT COMMENT 'col2 with () special character \" and ;',
        col3 BOOLEAN COMMENT 'col3 with special <> character \" and ;',
        col4 STRING COMMENT "col4 with special /* character ;",
        year INT COMMENT "year with */ special character ;",
        month INT COMMENT "month with special -- character ;",
        day INT COMMENT "day with special \" character ;"
    )
USING DELTA PARTITIONED BY (year, month, day)
LOCATION 'file:///app/tests/lakehouse/out/feature/table_manager/dummy_table_bronze/data_complex_default_scenario1'
TBLPROPERTIES('lakehouse.primary_key'=' id, `col1`');
-- New table manager test table, to check if new parser works as expected and deals well to different delimiters (;).
-- The parser must be able to deal with the delimiters that are inside of "", '', --, /* */.
/* New table manager test table, to check if new parser works as expected and deals well to different delimiters (;).
The parser must be able to deal with the delimiters that are inside of "", '', -- */
CREATE TABLE test_db.DummyTableBronzeComplexDefaultScenario2
    (
    id INT COMMENT 'id with special (< characters ;',
    col1 STRING COMMENT 'col1 with >) special character " and ;',
    col2 INT COMMENT 'col2 with () special character \" and ;',
    col3 BOOLEAN COMMENT 'col3 with special <> character \" and ;',
    col4 STRING COMMENT "col4 with special /* character ;",
    year INT COMMENT "year with */ special character ;",
    month INT COMMENT "month with special -- character ;",
    day INT COMMENT "day with special \" character ;"
    )
USING DELTA PARTITIONED BY (year, month, day)
LOCATION 'file:///app/tests/lakehouse/out/feature/table_manager/dummy_table_bronze/data_complex_default_scenario2'
TBLPROPERTIES('lakehouse.primary_key'=' id, `col1`')