ATTACH TABLE _ UUID '3db08e5d-4475-430d-8a66-f85972e0dc65'
(
    `match_id` Int32,
    `home_team_id` Nullable(Int32),
    `away_team_id` Nullable(Int32),
    `home_team_name` Nullable(String),
    `away_team_name` Nullable(String),
    `home_score` Nullable(Int32),
    `away_score` Nullable(Int32),
    `goal_diff` Int32,
    `xg_home` Nullable(Float32),
    `xg_away` Nullable(Float32),
    `xg_diff` Nullable(Float32),
    `winning_team` Nullable(String),
    `winning_side` LowCardinality(String),
    `winner_xg` Nullable(Float32),
    `match_time_utc_date` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)))
ORDER BY (match_id, winning_side)
SETTINGS index_granularity = 8192
