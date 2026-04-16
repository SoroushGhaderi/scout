-- Signal tables for gold layer

CREATE TABLE IF NOT EXISTS gold.signal_team_possession_passing_total_dominance (
    match_id Int32,
    match_time_utc_date Nullable(String),
    home_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_id Nullable(Int32),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    triggered_side LowCardinality(String),
    triggered_team_id Nullable(Int32),
    triggered_team_name Nullable(String),
    accurate_passes_home Nullable(Int32),
    accurate_passes_away Nullable(Int32),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, triggered_side)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS gold.signal_team_possession_passing_possession_without_purpose (
    match_id Int32,
    match_time_utc_date Nullable(String),
    home_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_id Nullable(Int32),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    triggered_side LowCardinality(String),
    triggered_team_id Nullable(Int32),
    triggered_team_name Nullable(String),
    possession_pct Nullable(Float64),
    shots_on_target Nullable(Int32),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, triggered_side)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

CREATE TABLE IF NOT EXISTS gold.signal_team_possession_passing_efficient_directness (
    match_id Int32,
    match_time_utc_date Nullable(String),
    home_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_id Nullable(Int32),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    triggered_side LowCardinality(String),
    triggered_team_id Nullable(Int32),
    triggered_team_name Nullable(String),
    possession_pct Nullable(Float64),
    total_shots Nullable(Int32),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, triggered_side)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));
