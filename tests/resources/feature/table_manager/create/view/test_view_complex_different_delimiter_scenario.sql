-- New table manager test view, to check if new parser works as expected and deals well with different delimiters (===).
-- The parser must be able to deal with the delimiters that are inside of "", '', --, /* */.
CREATE VIEW test_db.DummyViewBronzeComplexDifferentDelimiterScenario1 (id,col1,col2,col3,col4) AS
    SELECT id,col1,CONCAT_WS(";",col2) AS col2,col3,col4
    FROM test_db.DummyTableBronzeComplexDifferentDelimiterScenario1===
-- New table manager test view, to check if new parser works as expected and deals well with different delimiters (===).
-- The parser must be able to deal with the delimiters that are inside of "", '', --, /* */.
CREATE VIEW test_db.DummyViewBronzeComplexDifferentDelimiterScenario2 (id,col1,col2,col3,col4) AS
    SELECT id,col1,col2,CONCAT_WS(";",col3) AS col3,col4
    FROM test_db.DummyTableBronzeComplexDifferentDelimiterScenario2