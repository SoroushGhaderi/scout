-- Signal tables for gold layer

CREATE TABLE IF NOT EXISTS gold.sig_team_possession_passing_high_press_victim (
    match_id Int32,
    match_date Date,
    home_team_id Nullable(Int32),
    home_team_name Nullable(String),
    away_team_id Nullable(Int32),
    away_team_name Nullable(String),
    home_score Nullable(Int32),
    away_score Nullable(Int32),
    triggered_side LowCardinality(String),
    triggered_team_id Nullable(Int32),
    triggered_team_name Nullable(String),
    triggered_pass_accuracy_pct Nullable(Float32),
    pass_accuracy_home_pct Nullable(Float32),
    pass_accuracy_away_pct Nullable(Float32),
    pass_accuracy_delta_pct Float32,
    pass_attempts_home Int32,
    pass_attempts_away Int32,
    own_half_pass_share_home_pct Nullable(Float32),
    own_half_pass_share_away_pct Nullable(Float32),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, triggered_side)
PARTITION BY toYYYYMM(match_date);
