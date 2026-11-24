CREATE EXTERNAL TABLE `your_database`.`table_tennis_matches` (
    `match_id`  INT COMMENT 'match id',
    `match_date`  TIMESTAMP COMMENT 'match date',
    `home_player`  STRING COMMENT 'Home player name',
    `visitor_player`  STRING COMMENT 'Visitor player name',
    `sets_home`  INT COMMENT 'Number of sets won by Home player',
    `sets_visitor`  INT COMMENT 'Number of sets won by visitor player',
    `sets_played`  INT COMMENT 'Number of sets played',
    `home_player_game_1`  INT COMMENT 'Home player points on game 1',
    `visitor_player_game_1`  INT COMMENT 'Visitor player points on game 1',
    `home_player_game_2`  INT COMMENT 'Home player points on game 2',
    `visitor_player_game_2`  INT COMMENT 'visitor player points on game 2',
    `home_player_game_3`  INT COMMENT 'Home player points on game 3',
    `visitor_player_game_3`  INT COMMENT 'visitor player points on game 3',
    `home_player_game_4`  INT COMMENT 'Home player points on game 4',
    `visitor_player_game_4`  INT COMMENT 'visitor player points on game 4',
    `home_player_game_5`  INT COMMENT 'Home player points on game 5',
    `visitor_player_game_5`  INT COMMENT 'visitor player points on game 5',
    `home_winner` BOOLEAN COMMENT 'Is home winner',
    `lh_created_on` TIMESTAMP COMMENT 'Creation date on lakehouse',
    `match_year` INT COMMENT 'Year of the match',
    `match_month` INT COMMENT 'Month of the match'
)
USING DELTA
PARTITIONED BY (`match_year`, `match_month`)
LOCATION 's3://my-data-product-bucket/sports/table_tennis/silver/table_tennis_matches'
COMMENT 'Table tennis matches.'
TBLPROPERTIES(
  'lakehouse.primary_key'='match_id',
  'delta.enableChangeDataFeed'='true'
);