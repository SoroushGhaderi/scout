-- Scenario tables for silver layer

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_demolition (
    match_id Int32,
    home_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_id Nullable(Int32),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    goal_diff Int32,
    winning_side LowCardinality(String),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, winning_side)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_win_with_low_xg_conceded (
    match_id Int32,
    home_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_id Nullable(Int32),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    expected_goals_home Nullable(Float32),
    expected_goals_away Nullable(Float32),
    winning_team Nullable(String),
    winning_side LowCardinality(String),
    xg_conceded Nullable(Float32),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, winning_side)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_underdog_heist (
    match_id Int32,
    home_team_id Nullable(Int32),
    away_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    goal_diff Int32,
    xg_home Nullable(Float32),
    xg_away Nullable(Float32),
    xg_diff Nullable(Float32),
    winning_team Nullable(String),
    winning_side LowCardinality(String),
    winner_xg Nullable(Float32),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, winning_side)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_dead_ball_dominance (
    match_id Int32,
    home_team_id Nullable(Int32),
    away_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    home_set_piece_goals UInt32,
    away_set_piece_goals UInt32,
    winning_team Nullable(String),
    winning_side LowCardinality(String),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, winning_side)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_low_block_heist (
    match_id Int32,
    home_team_id Nullable(Int32),
    away_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    goal_diff Int32,
    possession_home Nullable(Float32),
    possession_away Nullable(Float32),
    winning_team Nullable(String),
    winning_side LowCardinality(String),
    winner_possession Nullable(Float32),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, winning_side)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));
