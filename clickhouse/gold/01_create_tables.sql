-- Gold aggregated tables in single fotmob database (schema-style via gold_ prefix)

CREATE TABLE IF NOT EXISTS fotmob.gold_player_match_stats (
    match_id Int32,
    player_id Int32,
    player_name String,
    team_id Nullable(Int32),
    team_name Nullable(String),
    goals Int32,
    assists Int32,
    rating Float32,
    minutes_played Int32,
    shot_events Int32,
    xg Float32,
    xgot Float32,
    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, player_id);

CREATE TABLE IF NOT EXISTS fotmob.gold_match_summary (
    match_id Int32,
    league_id Nullable(Int32),
    league_name Nullable(String),
    home_team_id Nullable(Int32),
    away_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_name Nullable(String),
    full_score Nullable(String),
    match_time_utc Nullable(String),
    attendance Nullable(Int32),
    referee_name Nullable(String),
    expected_goals_home Float32,
    expected_goals_away Float32,
    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id);

CREATE TABLE IF NOT EXISTS fotmob.gold_team_season_stats (
    league_id Int32,
    team_id Int32,
    team_name String,
    matches UInt32,
    total_goals Int32,
    total_assists Int32,
    avg_rating Float32,
    total_minutes Int64,
    season_first_seen Date,
    season_last_seen Date,
    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (league_id, team_id);
