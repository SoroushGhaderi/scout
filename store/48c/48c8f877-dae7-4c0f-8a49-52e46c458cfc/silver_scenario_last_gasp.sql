ATTACH TABLE _ UUID '57332b6c-3517-44e5-80ac-b9a4c458064f'
(
    `match_id` Int32,
    `home_team_id` Nullable(Int32),
    `away_team_id` Nullable(Int32),
    `home_team_name` Nullable(String),
    `away_team_name` Nullable(String),
    `home_score` Nullable(Int32),
    `away_score` Nullable(Int32),
    `goal_diff` Int32,
    `winning_goal_minute` Nullable(Int32),
    `winning_goal_added_time` Nullable(Int32),
    `winning_goal_scorer` Nullable(String),
    `home_score_before` Nullable(Int32),
    `away_score_before` Nullable(Int32),
    `winning_team` Nullable(String),
    `winning_side` LowCardinality(String),
    `match_time_utc_date` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)))
ORDER BY (match_id, winning_side)
SETTINGS index_granularity = 8192
