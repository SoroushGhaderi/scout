ATTACH TABLE _ UUID '7a32a8f4-8404-45ef-a5bf-5c643f7a13e3'
(
    `match_id` Int32,
    `player_id` Int32,
    `player_name` Nullable(String),
    `team_id` Nullable(Int32),
    `team_name` Nullable(String),
    `goals` Nullable(Int32),
    `assists` Nullable(Int32),
    `goal_contributions` Nullable(Int32),
    `xg` Nullable(Float32),
    `xa` Nullable(Float32),
    `xg_xa` Nullable(Float32),
    `home_team_name` Nullable(String),
    `away_team_name` Nullable(String),
    `home_score` Nullable(Int32),
    `away_score` Nullable(Int32),
    `team_side` LowCardinality(String),
    `match_time_utc_date` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)))
ORDER BY (match_id, player_id)
SETTINGS index_granularity = 8192
