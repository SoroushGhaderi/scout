ATTACH TABLE _ UUID '8d1e2ae9-2e9d-4e48-9ff3-14a4c2d2d383'
(
    `match_id` Int32,
    `event_id` Int64,
    `red_card_time` Int32,
    `red_card_overload_time` Nullable(Int32),
    `player_id` Nullable(Int32),
    `player_name` Nullable(String),
    `home_score` Int32,
    `away_score` Int32,
    `is_home` UInt8,
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, event_id)
SETTINGS index_granularity = 8192
