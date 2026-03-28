ATTACH TABLE _ UUID '4cd1aca8-eb20-488a-b603-66f1b82b1abe'
(
    `match_id` Int32,
    `home_team_id` Nullable(Int32),
    `home_team_name` Nullable(String),
    `away_team_id` Nullable(Int32),
    `away_team_name` Nullable(String),
    `home_score` Nullable(Int32),
    `away_score` Nullable(Int32),
    `goal_diff` Int32,
    `winning_side` LowCardinality(String),
    `match_time_utc_date` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)))
ORDER BY (match_id, winning_side)
SETTINGS index_granularity = 8192
