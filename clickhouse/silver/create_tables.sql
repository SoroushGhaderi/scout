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

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_defensive_shutdown_win (
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

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_tactical_stalemate (
    match_id Int32,
    home_team_id Nullable(Int32),
    away_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    xg_home Nullable(Float32),
    xg_away Nullable(Float32),
    combined_xg Nullable(Float32),
    winning_team Nullable(String),
    match_result LowCardinality(String),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, match_result)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_great_escape (
    match_id Int32,
    home_team_id Nullable(Int32),
    away_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    goal_diff Int32,
    home_score_at_60 UInt32,
    away_score_at_60 UInt32,
    winning_team Nullable(String),
    winning_side LowCardinality(String),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, winning_side)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_one_man_army (
    match_id Int32,
    player_id Nullable(Int32),
    player_name Nullable(String),
    team_id Nullable(Int32),
    team_name Nullable(String),
    goals Nullable(Int32),
    assists Nullable(Int32),
    goal_contributions Nullable(Int32),
    xg Nullable(Float32),
    xa Nullable(Float32),
    xg_xa Nullable(Float32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    team_side LowCardinality(String),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, ifNull(player_id, -1))
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_last_gasp (
    match_id Int32,
    home_team_id Nullable(Int32),
    away_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    goal_diff Int32,
    winning_goal_minute Nullable(Int32),
    winning_goal_added_time Nullable(Int32),
    winning_goal_scorer Nullable(String),
    home_score_before Nullable(Int32),
    away_score_before Nullable(Int32),
    winning_team Nullable(String),
    winning_side LowCardinality(String),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, winning_side)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_shot_stopper (
    match_id Int32,
    home_team_id Nullable(Int32),
    away_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    keeper_id Nullable(Int32),
    goalkeeper_name Nullable(String),
    goalkeeper_team_id Nullable(Int32),
    goalkeeper_team Nullable(String),
    saves UInt32,
    xg_saved Nullable(Float32),
    keeper_side LowCardinality(String),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, ifNull(keeper_id, -1))
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_war_zone (
    match_id Int32,
    home_team_id Nullable(Int32),
    away_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    fouls_home Nullable(Int32),
    fouls_away Nullable(Int32),
    combined_fouls Nullable(Int32),
    yellow_cards_home Nullable(Int32),
    yellow_cards_away Nullable(Int32),
    combined_yellow_cards Nullable(Int32),
    red_cards_home Nullable(Int32),
    red_cards_away Nullable(Int32),
    combined_red_cards Nullable(Int32),
    match_result LowCardinality(String),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, match_result)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_clinical_finisher (
    match_id Int32,
    home_team_id Nullable(Int32),
    away_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    player_id Nullable(Int32),
    player_name Nullable(String),
    team_id Nullable(Int32),
    goals UInt32,
    total_shots UInt32,
    combined_xg Nullable(Float32),
    team_side LowCardinality(String),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, ifNull(player_id, -1))
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_russian_roulette (
    match_id Int32,
    home_team_id Nullable(Int32),
    away_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    match_duration UInt32,
    total_penalties UInt32,
    home_penalties UInt32,
    away_penalties UInt32,
    penalties_scored UInt32,
    penalties_missed UInt32,
    home_penalties_scored UInt32,
    away_penalties_scored UInt32,
    home_penalties_missed UInt32,
    away_penalties_missed UInt32,
    home_penalty_xgot Nullable(Float32),
    away_penalty_xgot Nullable(Float32),
    combined_penalty_xgot Nullable(Float32),
    winning_team Nullable(String),
    match_result LowCardinality(String),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, match_result)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_efficiency_machine (
    match_id Int32,
    home_team_id Nullable(Int32),
    away_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    goal_diff Int32,
    winning_team Nullable(String),
    winning_side LowCardinality(String),
    home_total_shots UInt32,
    away_total_shots UInt32,
    home_avg_xg_per_shot Nullable(Float32),
    away_avg_xg_per_shot Nullable(Float32),
    home_total_xg Nullable(Float32),
    away_total_xg Nullable(Float32),
    winner_total_shots UInt32,
    winner_avg_xg_per_shot Nullable(Float32),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, winning_side)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_away_day_masterclass (
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
    xg_home Nullable(Float32),
    xg_away Nullable(Float32),
    xg_diff Nullable(Float32),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, ifNull(away_team_id, -1))
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_key_pass_king (
    match_id Int32,
    player_id Nullable(Int32),
    player_name Nullable(String),
    team_id Nullable(Int32),
    team_name Nullable(String),
    chances_created Nullable(Int32),
    xa Nullable(Float32),
    assists Nullable(Int32),
    goals Nullable(Int32),
    minutes_played Nullable(Int32),
    fotmob_rating Nullable(Float32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    team_side LowCardinality(String),
    match_result LowCardinality(String),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, ifNull(player_id, -1))
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS fotmob.silver_scenario_wildcard (
    match_id Int32,
    home_team_id Nullable(Int32),
    away_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    player_id Nullable(Int32),
    player_name Nullable(String),
    team_side LowCardinality(String),
    substitution_time Nullable(Int32),
    substitution_reason Nullable(String),
    goals Nullable(Int32),
    assists Nullable(Int32),
    goal_contributions Nullable(Int32),
    xg Nullable(Float32),
    xa Nullable(Float32),
    minutes_played Nullable(Int32),
    fotmob_rating Nullable(Float32),
    match_result LowCardinality(String),
    match_time_utc_date Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, ifNull(player_id, -1))
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));
