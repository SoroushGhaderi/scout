CREATE TABLE IF NOT EXISTS gold_signals.signal_activations (
    signal_instance_id String,
    signal_id LowCardinality(String),
    signal_id_version LowCardinality(String) DEFAULT 'v1',
    match_id Int32,
    match_date Date,
    triggered_side LowCardinality(Nullable(String)),
    triggered_team_id Nullable(Int32),
    triggered_player_id Nullable(Int32),
    source_table LowCardinality(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (signal_id, match_id, signal_instance_id)
PARTITION BY toYYYYMM(match_date);
