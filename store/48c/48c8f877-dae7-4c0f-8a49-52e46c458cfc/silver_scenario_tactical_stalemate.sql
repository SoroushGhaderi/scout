ATTACH TABLE _ UUID '142b2a32-43d3-4f40-bedb-f5229ff060ae'
(
    `match_id` Int32,
    `home_team_id` Nullable(Int32),
    `away_team_id` Nullable(Int32),
    `home_team_name` Nullable(String),
    `away_team_name` Nullable(String),
    `home_score` Nullable(Int32),
    `away_score` Nullable(Int32),
    `xg_home` Nullable(Float32),
    `xg_away` Nullable(Float32),
    `combined_xg` Nullable(Float32),
    `winning_team` Nullable(String),
    `match_result` LowCardinality(String),
    `match_time_utc_date` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)))
ORDER BY (match_id, match_result)
SETTINGS index_granularity = 8192
