ATTACH TABLE _ UUID '2a78e2a7-3d2f-4128-b1bf-487d2733455d'
(
    `match_id` Int32,
    `minute` Nullable(Float32),
    `value` Nullable(Int32),
    `momentum_team` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, assumeNotNull(minute))
SETTINGS index_granularity = 8192
