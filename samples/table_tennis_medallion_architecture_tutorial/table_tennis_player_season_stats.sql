CREATE EXTERNAL TABLE `your_gold_database`.`table_tennis_player_season_stats` (
    `player_name` STRING COMMENT 'Player name',
    `total_matches` INT COMMENT 'Number of matches played',
    `victories` INT COMMENT 'Number of victories',
    `home_matches` INT COMMENT 'Number of matches as home player',
    `home_victories` INT COMMENT 'Number of victories as home player',
    `clean_victories` INT COMMENT 'Number of clean victories (3-0)',
    `sets_played` INT COMMENT 'Number of sets played',
    `sets_victories` INT COMMENT 'Number of sets won',
    `season` INT COMMENT 'Year season'
)
USING DELTA
LOCATION 's3://my-data-product-bucket/sports/table_tennis/gold/table_tennis_player_season_stats'
COMMENT 'Table tennis player season stats.'
TBLPROPERTIES(
  'lakehouse.primary_key'='player_name, season',
  'delta.enableChangeDataFeed'='false'
);