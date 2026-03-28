ATTACH TABLE _ UUID 'eea5df73-e93a-4973-99b8-3befd40f4467'
(
    `match_id` Int32,
    `home_team_id` Nullable(Int32),
    `away_team_id` Nullable(Int32),
    `home_team_name` Nullable(String),
    `away_team_name` Nullable(String),
    `home_score` Nullable(Int32),
    `away_score` Nullable(Int32),
    `keeper_id` Nullable(Int32),
    `goalkeeper_name` Nullable(String),
    `goalkeeper_team_id` Nullable(Int32),
    `goalkeeper_team` Nullable(String),
    `saves` UInt32,
    `xg_saved` Nullable(Float32),
    `keeper_side` LowCardinality(String),
    `match_time_utc_date` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)))
ORDER BY (match_id, ifNull(keeper_id, -1))
SETTINGS index_granularity = 8192
