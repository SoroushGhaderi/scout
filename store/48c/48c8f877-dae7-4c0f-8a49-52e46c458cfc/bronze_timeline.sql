ATTACH TABLE _ UUID '903dea66-4fa0-41af-a85c-55c4b82ad96b'
(
    `match_id` Int32,
    `match_time_utc` Nullable(DateTime),
    `first_half_started` Nullable(String),
    `first_half_ended` Nullable(String),
    `second_half_started` Nullable(String),
    `second_half_ended` Nullable(String),
    `first_extra_half_started` Nullable(String),
    `second_extra_half_started` Nullable(String),
    `game_ended` Nullable(String),
    `game_finished` UInt8,
    `game_started` UInt8,
    `game_cancelled` UInt8,
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(toDate(assumeNotNull(match_time_utc)))
ORDER BY match_id
SETTINGS index_granularity = 8192
