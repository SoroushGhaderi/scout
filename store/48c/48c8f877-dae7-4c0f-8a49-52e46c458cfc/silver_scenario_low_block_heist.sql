ATTACH TABLE _ UUID '18f56c46-6586-445f-82e4-94167d369135'
(
    `match_id` Int32,
    `home_team_id` Nullable(Int32),
    `away_team_id` Nullable(Int32),
    `home_team_name` Nullable(String),
    `away_team_name` Nullable(String),
    `home_score` Nullable(Int32),
    `away_score` Nullable(Int32),
    `goal_diff` Int32,
    `possession_home` Nullable(Float32),
    `possession_away` Nullable(Float32),
    `winning_team` Nullable(String),
    `winning_side` LowCardinality(String),
    `winner_possession` Nullable(Float32),
    `match_time_utc_date` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)))
ORDER BY (match_id, winning_side)
SETTINGS index_granularity = 8192
