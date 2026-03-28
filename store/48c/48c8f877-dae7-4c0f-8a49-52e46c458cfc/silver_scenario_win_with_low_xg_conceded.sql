ATTACH TABLE _ UUID '6fd718b8-4edb-43b6-9099-f4e9768c10e0'
(
    `match_id` Int32,
    `home_team_id` Nullable(Int32),
    `home_team_name` Nullable(String),
    `away_team_id` Nullable(Int32),
    `away_team_name` Nullable(String),
    `home_score` Nullable(Int32),
    `away_score` Nullable(Int32),
    `expected_goals_home` Nullable(Float32),
    `expected_goals_away` Nullable(Float32),
    `winning_team` Nullable(String),
    `winning_side` LowCardinality(String),
    `xg_conceded` Nullable(Float32),
    `match_time_utc_date` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)))
ORDER BY (match_id, winning_side)
SETTINGS index_granularity = 8192
